#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
newenrich.py - Find official website URLs for every record in News_Without_Web.xlsx
using Bing Maps (primary) + Bing Search top3 scoring + Google Maps (fallback) via browser automation.

ONE browser, multiple tabs, asyncio (Playwright async API).
Updated 2026-08-31 for News_Without_Web.xlsx:
  - Input: News_Without_Web.xlsx (ID, keyid, COMPANY NAME, Web Address, ENTITY_TYPE, DUNS_NUMBER, dup)
  - Query: ONLY company name (COMPANY NAME column) – no address/country appended
  - Output: CSV with columns ID, keyid, COMPANY NAME, Web Address, ENTITY_TYPE, DUNS_NUMBER, dup, URL

Features:
  - URLs saved immediately to Supabase database (public.companies) after each search
  - Resume support: skips already processed records
  - Real-time console output: shows company_name -> URL for each result

Usage:
    python newenrich.py --limit 100                 # first 100 records
    python newenrich.py --workers 6                 # 6 parallel tabs, one browser
    python newenrich.py --start 10000 --limit 5000  # a specific chunk
    python newenrich.py                             # everything (resumes)
    python newenrich.py --input News_Without_Web.xlsx --out enriched_news.csv

Results are:
  - Saved immediately to Supabase database (public.companies table) if configured
  - Appended row-by-row to enriched_news.csv (UTF-8 with BOM, CSV format)
  - Stop anytime with Ctrl+C and re-run to continue (progress saved)

Supabase Configuration (environment variables, or .env file):
  SUPABASE_URL      e.g. https://xxxxxxxx.supabase.co
  SUPABASE_KEY      service role key (recommended) or anon key
  SUPABASE_TABLE    table name, default: companies

  Create the table first by running setup.sql in the Supabase SQL editor.
"""

import argparse
import asyncio
import csv
import difflib
import html
import json
import os
import random
import re
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

try:
    import httpx
    from playwright.async_api import async_playwright
except ModuleNotFoundError:
    req = Path(__file__).resolve().with_name("requirements.txt")
    if req.is_file():
        print("Missing dependencies, installing from requirements.txt ...", flush=True)
        subprocess.run([sys.executable, "-m", "pip", "install", "--user", "-U",
                        "-r", str(req)], check=False)
        os.execv(sys.executable, [sys.executable, *sys.argv])
    raise

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().with_name(".env"))
except Exception:
    pass

# Fix Windows console encoding for Japanese / accented PARTY_NAME
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

SRC = str(Path(__file__).resolve().parent / "News_Without_Web.xlsx")
OUT = Path(__file__).resolve().parent / "enriched_news.csv"
PROFILE = Path(__file__).resolve().parent / ".browser_profile"

# Supabase connection settings (from environment variables)
SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE") or "companies"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# Old 10-column format (508K file) vs New 4-column format (CompanyCountry298k_08212026.xlsx)
# vs News 7-column format (News_Without_Web.xlsx)
OLD_FIELDS = ["id", "keyid", "company_name", "account_alias", "address1",
              "address2", "state", "city", "postal_code", "country"]
NEW_FIELDS = ["keyid", "PARTY_NAME", "ACCOUNT_ALIAS", "country_format"]
NEWS_FIELDS = ["id", "keyid", "COMPANY NAME", "Web Address", "ENTITY_TYPE", "DUNS_NUMBER", "dup"]
# keep FIELDS for backwards compat where needed
FIELDS = OLD_FIELDS

BAD_DOMAINS = {
    "bing.com", "microsoft.com", "google.com", "googleusercontent.com",
    "facebook.com", "linkedin.com", "twitter.com", "x.com", "instagram.com",
    "yelp.com", "tripadvisor.com", "yellowpages.com", "cybozu.com",
    "wikipedia.org", "googlemaps.com", "g.co", "goo.gle",
    "dnb.com", "bloomberg.com", "emis.com", "companyhouse.de", "sgpgrid.com",
    "info-clipper.com", "abncheck.com", "opencorporates.com", "zoominfo.com",
    "craft.co", "crunchbase.com", "glassdoor.com", "indeed.com", "owler.com",
    "rocketreach.co", "apollo.io", "youtube.com", "vimeo.com",
    "amazon.com", "ebay.com",
    "indiatimes.com", "economictimes.com", "indiamart.com", "zaubacorp.com",
    "justdial.com", "tradeindia.com", "kompass.com",
}

OVERLAY = "https://www.bing.com/maps/overlaybfpr?q={q}"

# Extended domain scoring helpers (added 2026-08-25 for Bing Search fallback)
LEGAL_SUFFIXES = {
    "gmbh","ltd","limited","inc","incorporated","llc","plc","corp","corporation",
    "co","company","berhad","sdn","bhd","pvt","private","sa","sarl","sas",
    "pty","bv","ag","kg","ev","ug","ohg","ltda","lda","nv","ab","as","aps","spa","srl",
    "sro","doo","kft","oy","pte","holdings","holding","group","partners",
}
GEO_TOKENS = {
    "india","indian","germany","german","brazil","brazilian","france","french","japan","japanese",
    "malaysia","malaysian","denmark","danish","australia","australian","italy","italian","hong","kong",
    "china","chinese","switzerland","swiss","canada","canadian","usa","america","american","mexico","mexican",
    "spain","spanish","uk","england","english","britain","british","russia","russian","korea","korean",
}

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def supabase_base():
    return f"{SUPABASE_URL}/rest/v1"


def init_supabase():
    """Check Supabase connectivity and that the target table exists."""
    if not (SUPABASE_URL and SUPABASE_KEY):
        env_path = Path(__file__).resolve().with_name(".env")
        print(f"Warning: SUPABASE_URL / SUPABASE_KEY not set. "
              f"Checked .env at: {env_path} (exists: {env_path.exists()}). "
              f"Will use CSV output only.", flush=True)
        return False
    try:
        with httpx.Client(timeout=15) as c:
            resp = c.get(f"{supabase_base()}/{SUPABASE_TABLE}?select=id&limit=1",
                         headers=SB_HEADERS)
            if resp.status_code == 404:
                print(f"Warning: table '{SUPABASE_TABLE}' not found in Supabase. "
                      f"Run setup.sql in the Supabase SQL editor, then re-run. "
                      f"Will use CSV output only.", flush=True)
                return False
            resp.raise_for_status()
        print(f"Supabase connected: {SUPABASE_URL} (table {SUPABASE_TABLE})", flush=True)
        return True
    except Exception as e:
        print(f"Warning: Could not connect to Supabase ({e}). "
              f"Will use CSV output only.", flush=True)
        return False


def sb_write_batch(result_rows, recs, client):
    """Upsert a batch of rows into Supabase in one request (merge on keyid)."""
    if not result_rows:
        return
    payload = []
    for result_row, rec in zip(result_rows, recs):
        # Support both old 10-col and new 4-col (PARTY_NAME + country_format) formats
        company_name = rec.get("PARTY_NAME") or rec.get("company_name") or ""
        account_alias = rec.get("ACCOUNT_ALIAS") or rec.get("account_alias") or ""
        country = rec.get("country_format") or rec.get("country") or ""
        payload.append({
            "id": rec.get("id", ""),
            "keyid": rec.get("keyid", ""),
            "company_name": company_name,
            "account_alias": account_alias,
            "address1": rec.get("address1", ""),
            "address2": rec.get("address2", ""),
            "state": rec.get("state", ""),
            "city": rec.get("city", ""),
            "postal_code": rec.get("postal_code", ""),
            "country": country,
            "website": result_row[4] or result_row[6] or "",
            "found_name": result_row[3] or result_row[5] or "",
            "source": result_row[7],
            "status": result_row[7],
            "error": result_row[8] or "",
        })
    try:
        resp = client.post(
            f"{supabase_base()}/{SUPABASE_TABLE}",
            headers={**SB_HEADERS,
                     "Prefer": "resolution=merge-duplicates"},
            params={"on_conflict": "keyid"},
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"  [db] supabase upsert failed for {len(payload)} rows: {e}",
              flush=True)


def fetch_done_keyids(client):
    """Fetch all existing keyids from Supabase (paginated, PostgREST caps at 1000)."""
    done = set()
    if not client:
        return done
    offset = 0
    limit = 1000
    while True:
        try:
            resp = client.get(
                f"{supabase_base()}/{SUPABASE_TABLE}",
                headers=SB_HEADERS,
                params={"select": "keyid", "offset": offset, "limit": limit},
                timeout=30,
            )
            resp.raise_for_status()
            rows = resp.json()
        except Exception:
            break
        if not rows:
            break
        for r in rows:
            if r.get("keyid"):
                done.add(r["keyid"])
        if len(rows) < limit:
            break
        offset += limit
    return done


class _Resp:
    def __init__(self, ok, text, status=200):
        self.ok = ok
        self.status = status
        self._t = text

    async def text(self):
        return self._t


class HttpxRequest:
    """Drop-in replacement for Playwright's APIRequestContext (Bing API only).

    Honors PROXY_URL (or standard HTTP(S)_PROXY) env vars so the container can
    route Bing traffic through a rotating/residential proxy."""

    def __init__(self):
        proxy = (os.environ.get("PROXY_URL")
                 or os.environ.get("HTTPS_PROXY")
                 or os.environ.get("HTTP_PROXY")
                 or None)
        # FIX 2026-08-25: Added proper headers (UA, Accept-Language, Referer) - overlay was returning
        # empty/error without headers after Bing tightened bot detection 2 days ago.
        headers = {
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.bing.com/maps",
            "Accept-Encoding": "gzip, deflate",
        }
        self._client = httpx.AsyncClient(timeout=20, follow_redirects=True,
                                         proxy=proxy, headers=headers)

    async def get(self, url, timeout=20000):
        try:
            r = await self._client.get(url)
            return _Resp(r.status_code < 400, r.text, r.status_code)
        except Exception:
            return _Resp(False, "", 0)

    async def close(self):
        await self._client.aclose()


class Sink:
    """Collects results and flushes them to CSV + Supabase in batches, so that
    slow disk/DB I/O never blocks the browser workers."""

    def __init__(self, wtr, out_f, sb_client, stats, todo_count, quiet,
                 flush_every=100, flush_interval=0.3):
        self.wtr = wtr
        self.out_f = out_f
        self.sb_client = sb_client
        self.stats = stats
        self.todo_count = todo_count
        self.quiet = quiet
        self.flush_every = flush_every
        self.flush_interval = flush_interval
        self.lock = asyncio.Lock()
        self.pending = []
        self.last_flush = time.time()
        self._stop = asyncio.Event()

    async def push(self, row, rec):
        async with self.lock:
            self.pending.append((row, rec))

    async def _flush_locked(self):
        if not self.pending:
            return
        pending = self.pending
        self.pending = []
        rows = [r for r, _ in pending]
        recs = [rc for _, rc in pending]
        # Detect output format from record: news 7-col vs new 4-col vs old 10-col
        def _csv_row(r, rc):
            fmt = rc.get("__format", "")
            # News format: ID, keyid, COMPANY NAME, Web Address, ENTITY_TYPE, DUNS_NUMBER, dup, URL
            if fmt == "news" or ("COMPANY NAME" in rc and "ENTITY_TYPE" in rc):
                url = r[4] or r[6] or ""
                return [rc.get("id", ""), rc.get("keyid", ""),
                        rc.get("COMPANY NAME", "") or rc.get("company_name", "") or rc.get("PARTY_NAME", ""),
                        rc.get("Web Address", "") or "",
                        rc.get("ENTITY_TYPE", "") or "", rc.get("DUNS_NUMBER", "") or "",
                        rc.get("dup", "") or "", url]
            elif fmt == "new" or ("PARTY_NAME" in rc and "country_format" in rc):
                # New file: keyid, PARTY_NAME, ACCOUNT_ALIAS, country_format, URL, high/low
                return [rc.get("keyid", ""), rc.get("PARTY_NAME", "") or rc.get("company_name", ""),
                        rc.get("ACCOUNT_ALIAS", "") or rc.get("account_alias", ""),
                        rc.get("country_format", "") or rc.get("country", ""),
                        r[4] or r[6] or "", r[3] or r[5] or ""]
            else:
                return [rc.get("id", ""), rc.get("keyid", ""), rc.get("company_name", ""),
                        rc.get("account_alias", ""), rc.get("address1", ""),
                        rc.get("address2", ""), rc.get("state", ""), rc.get("city", ""),
                        rc.get("postal_code", ""), rc.get("country", ""),
                        r[4] or r[6] or "", r[3] or r[5] or ""]
        self.wtr.writerows([_csv_row(r, rc) for r, rc in pending])
        self.out_f.flush()
        if self.sb_client:
            await asyncio.to_thread(sb_write_batch, rows, recs, self.sb_client)
        for row, _ in pending:
            self.stats["processed"] += 1
            if row[7] == "bing":
                self.stats["bing"] += 1
            elif row[7] == "google":
                self.stats["google"] += 1
            elif row[7] == "none":
                self.stats["none"] += 1
            else:
                self.stats["errors"] += 1
        if not self.quiet:
            rate = self.stats["processed"] / max(time.time() - self.stats["start"], 1)
            eta = (self.todo_count - self.stats["processed"]) / rate / 3600 if rate else 0
            found_url = rows[-1][4] or rows[-1][6] or '-'
            _rec = pending[-1][1]
            company_display = (_rec.get("COMPANY NAME") or _rec.get("PARTY_NAME") or _rec.get("company_name")
                               or _rec.get("ACCOUNT_ALIAS") or _rec.get("account_alias") or "")[:40]
            try:
                print(f"  {self.stats['processed']:>6}/{self.todo_count} "
                      f"[{self.stats['bing']}b/{self.stats['google']}g/"
                      f"{self.stats['none']}n] {rate:.1f}/s ETA {eta:.1f}h "
                      f"{company_display} -> {found_url}", flush=True)
            except UnicodeEncodeError:
                safe = company_display.encode("ascii", errors="replace").decode("ascii")
                print(f"  {self.stats['processed']:>6}/{self.todo_count} "
                      f"[{self.stats['bing']}b/{self.stats['google']}g/"
                      f"{self.stats['none']}n] {rate:.1f}/s ETA {eta:.1f}h "
                      f"{safe} -> {found_url}", flush=True)
        self.last_flush = time.time()

    async def flusher(self):
        while True:
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.flush_interval)
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                break
            async with self.lock:
                if len(self.pending) >= self.flush_every or \
                   time.time() - self.last_flush >= self.flush_interval:
                    await self._flush_locked()
        async with self.lock:
            await self._flush_locked()

    async def stop(self):
        self._stop.set()
        await self.flusher()


def _norm_hdr(h):
    """Normalize header: strip BOM, quotes, whitespace, lower case."""
    if h is None:
        return ""
    s = str(h).strip()
    s = s.lstrip("\ufeff").strip()
    # strip surrounding single or double quotes e.g. "'keyid'" or '"keyid"'
    if len(s) >= 2 and ((s[0] == "'" and s[-1] == "'") or (s[0] == '"' and s[-1] == '"')):
        s = s[1:-1].strip().lstrip("\ufeff").strip()
    s = s.strip().lstrip("\ufeff")
    return s.lower().strip()

def read_records():
    src = SRC
    if src.lower().endswith(".xlsx"):
        try:
            import openpyxl
            wb = openpyxl.load_workbook(src, read_only=True, data_only=True)
        except Exception as e:
            err = str(e).lower()
            if "zip" in err or "not a zip" in err or "no such file" in err:
                pass  # fall through to CSV logic below
            else:
                raise
        else:
            try:
                ws = wb[wb.sheetnames[0]]
                it = ws.iter_rows(values_only=True)
                header = next(it, None)
                if header is None:
                    return
                hdr = [(str(h).strip() if h is not None else "") for h in header]
                hdr_norm = [_norm_hdr(h) for h in header]
                idx_norm = {h: i for i, h in enumerate(hdr_norm) if h}
                is_news = "company name" in hdr_norm
                is_new = "party_name" in hdr_norm and "country_format" in hdr_norm

                if is_news:
                    # News_Without_Web.xlsx: ID, keyid, COMPANY NAME, Web Address, ENTITY_TYPE, DUNS_NUMBER, dup
                    for row in it:
                        if not row or all(v is None or str(v).strip() == "" for v in row):
                            continue
                        row_vals = ["" if v is None else str(v).strip() for v in row]
                        if len(row_vals) < len(hdr):
                            row_vals += [""] * (len(hdr) - len(row_vals))
                        rec = {}
                        rec["id"] = row_vals[idx_norm.get("id", 0)] if "id" in idx_norm else ""
                        rec["keyid"] = row_vals[idx_norm.get("keyid", 1)] if "keyid" in idx_norm else ""
                        rec["COMPANY NAME"] = row_vals[idx_norm.get("company name", 2)] if "company name" in idx_norm else ""
                        rec["company_name"] = rec["COMPANY NAME"]
                        rec["PARTY_NAME"] = rec["COMPANY NAME"]
                        rec["Web Address"] = row_vals[idx_norm.get("web address", 3)] if "web address" in idx_norm else ""
                        rec["ENTITY_TYPE"] = row_vals[idx_norm.get("entity_type", 4)] if "entity_type" in idx_norm else ""
                        rec["DUNS_NUMBER"] = row_vals[idx_norm.get("duns_number", 5)] if "duns_number" in idx_norm else ""
                        rec["dup"] = row_vals[idx_norm.get("dup", 6)] if "dup" in idx_norm else ""
                        # compat helpers
                        rec["account_alias"] = ""
                        rec["ACCOUNT_ALIAS"] = ""
                        rec["country_format"] = ""
                        rec["country"] = ""
                        rec["address1"] = rec["address2"] = rec["state"] = rec["city"] = rec["postal_code"] = ""
                        rec["__format"] = "news"
                        yield rec
                elif is_new:
                    for row in it:
                        if not row or all(v is None or str(v).strip() == "" for v in row):
                            continue
                        row_vals = ["" if v is None else str(v).strip() for v in row]
                        if len(row_vals) < len(hdr):
                            row_vals += [""] * (len(hdr) - len(row_vals))
                        rec = {}
                        rec["keyid"] = row_vals[idx_norm.get("keyid", 0)] if "keyid" in idx_norm else ""
                        rec["PARTY_NAME"] = row_vals[idx_norm.get("party_name", 1)] if "party_name" in idx_norm else ""
                        rec["ACCOUNT_ALIAS"] = row_vals[idx_norm.get("account_alias", 2)] if "account_alias" in idx_norm else ""
                        rec["country_format"] = row_vals[idx_norm.get("country_format", 3)] if "country_format" in idx_norm else ""
                        rec["company_name"] = rec["PARTY_NAME"]
                        rec["account_alias"] = rec["ACCOUNT_ALIAS"]
                        rec["country"] = rec["country_format"]
                        rec["id"] = rec["keyid"]
                        rec["address1"] = rec["address2"] = rec["state"] = rec["city"] = rec["postal_code"] = ""
                        rec["__format"] = "new"
                        yield rec
                else:
                    is_old_header = "company_name" in hdr_norm
                    for row in it:
                        if not row:
                            continue
                        row_vals = tuple("" if v is None else str(v).strip() for v in row)
                        if is_old_header:
                            rec = {}
                            for col_name in OLD_FIELDS:
                                key_lower = col_name.lower()
                                pos = idx_norm.get(key_lower)
                                if pos is not None and pos < len(row_vals):
                                    rec[col_name] = row_vals[pos]
                                else:
                                    rec[col_name] = ""
                            rec["PARTY_NAME"] = rec.get("company_name", "")
                            rec["ACCOUNT_ALIAS"] = rec.get("account_alias", "")
                            rec["country_format"] = rec.get("country", "")
                            rec["__format"] = "old"
                            yield rec
                        else:
                            row_padded = (row_vals + ("",) * 10)[:10]
                            rec = dict(zip(OLD_FIELDS, row_padded))
                            rec["PARTY_NAME"] = rec.get("company_name", "")
                            rec["ACCOUNT_ALIAS"] = rec.get("account_alias", "")
                            rec["country_format"] = rec.get("country", "")
                            rec["__format"] = "old"
                            yield rec
            finally:
                wb.close()
            return
    if not Path(src).is_file():
        sys.exit(f"Error: input file not found: {src}\n"
                 f"Upload News_Without_Web.xlsx next to newenrich.py, or set INPUT_CSV "
                 f"to the path of your input CSV/XLSX.")
    # CSV path - also support news/new/old formats via header detection
    with open(src, newline="", encoding="utf-8-sig") as f:
        rd = csv.reader(f)
        header = next(rd, None)
        if header is None:
            return
        hdr_norm = [_norm_hdr(h) for h in header]
        idx_norm = {h: i for i, h in enumerate(hdr_norm) if h}
        is_news = "company name" in hdr_norm
        is_new = "party_name" in hdr_norm and "country_format" in hdr_norm
        for row in rd:
            if not row or all(v.strip() == "" for v in row):
                continue
            if is_news:
                row = (row + [""] * len(header))[:len(header)]
                rec = {}
                rec["id"] = row[idx_norm.get("id", 0)] if "id" in idx_norm else ""
                rec["keyid"] = row[idx_norm.get("keyid", 1)] if "keyid" in idx_norm else ""
                rec["COMPANY NAME"] = row[idx_norm.get("company name", 2)] if "company name" in idx_norm else ""
                rec["company_name"] = rec["COMPANY NAME"]
                rec["PARTY_NAME"] = rec["COMPANY NAME"]
                rec["Web Address"] = row[idx_norm.get("web address", 3)] if "web address" in idx_norm else ""
                rec["ENTITY_TYPE"] = row[idx_norm.get("entity_type", 4)] if "entity_type" in idx_norm else ""
                rec["DUNS_NUMBER"] = row[idx_norm.get("duns_number", 5)] if "duns_number" in idx_norm else ""
                rec["dup"] = row[idx_norm.get("dup", 6)] if "dup" in idx_norm else ""
                rec["account_alias"] = ""
                rec["ACCOUNT_ALIAS"] = ""
                rec["country_format"] = ""
                rec["country"] = ""
                rec["address1"] = rec["address2"] = rec["state"] = rec["city"] = rec["postal_code"] = ""
                rec["__format"] = "news"
                yield rec
            elif is_new:
                row = (row + [""] * 10)[:len(header)]
                rec = {}
                rec["keyid"] = row[idx_norm.get("keyid", 0)] if "keyid" in idx_norm else ""
                rec["PARTY_NAME"] = row[idx_norm.get("party_name", 1)] if "party_name" in idx_norm else ""
                rec["ACCOUNT_ALIAS"] = row[idx_norm.get("account_alias", 2)] if "account_alias" in idx_norm else ""
                rec["country_format"] = row[idx_norm.get("country_format", 3)] if "country_format" in idx_norm else ""
                rec["company_name"] = rec["PARTY_NAME"]
                rec["account_alias"] = rec["ACCOUNT_ALIAS"]
                rec["country"] = rec["country_format"]
                rec["id"] = rec["keyid"]
                rec["address1"] = rec["address2"] = rec["state"] = rec["city"] = rec["postal_code"] = ""
                rec["__format"] = "new"
                yield rec
            else:
                row = (row + [""] * 10)[:10]
                rec = dict(zip(OLD_FIELDS, row))
                rec["PARTY_NAME"] = rec.get("company_name", "")
                rec["ACCOUNT_ALIAS"] = rec.get("account_alias", "")
                rec["country_format"] = rec.get("country", "")
                rec["__format"] = "old"
                yield rec


def build_query(rec):
    """Build query using ONLY company name (COMPANY NAME column) as requested for News_Without_Web.xlsx."""
    # Priority: COMPANY NAME (news) -> PARTY_NAME / company_name (backward compat)
    fmt = rec.get("__format", "")
    if fmt == "news":
        company = (rec.get("COMPANY NAME") or rec.get("company_name") or rec.get("PARTY_NAME") or "").strip()
        if company and company.upper() != "NULL":
            return company
        return ""
    # Backward compat for old/new formats: still only company name (no country) per new requirement
    party = (rec.get("PARTY_NAME") or rec.get("COMPANY NAME") or rec.get("company_name") or "").strip()
    if party and party.upper() != "NULL":
        return party
    # Absolute fallback (should not happen)
    fallback_parts = []
    for k in ("company_name", "COMPANY NAME", "PARTY_NAME"):
        v = (rec.get(k) or "").strip()
        if not v or v.upper() == "NULL":
            continue
        fallback_parts.append(v)
        break
    return ", ".join(fallback_parts)


def short_query(rec):
    """Short query fallback: same as build_query (only company name) for news format."""
    return build_query(rec)


def clean_website(url):
    if not url:
        return None
    url = url.strip()
    if url.lower().startswith("http"):
        return truncate_to_base_domain(url)
    return None


def truncate_to_base_domain(url):
    """Truncate URL to base domain (scheme + netloc + '/')"""
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}/"
    except Exception:
        pass
    return url


def ok_domain(url):
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return False
    host = re.sub(r"^www\.", "", host)
    if not host or host in BAD_DOMAINS:
        return False
    return True


def similarity(a, b):
    """Calculate similarity ratio between two strings (0-1)."""
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def verify_match(rec, query):
    """
    Verify if company name matches at least 70% against the search query/result.
    For News_Without_Web.xlsx we only have COMPANY NAME (no country), so we match on company name alone.
    For old/new formats, fallback to PARTY_NAME + country if available, otherwise company alone.
    Returns 'high' if match >= 70%, else 'low'.
    """
    fmt = rec.get("__format", "")
    if fmt == "news":
        company_name = (rec.get("COMPANY NAME") or rec.get("company_name") or rec.get("PARTY_NAME") or "").strip()
        if not company_name:
            return "low"
        input_combined = company_name
        found_combined = query.strip()
        if not found_combined:
            return "low"
        match_ratio = similarity(input_combined, found_combined)
        return "high" if match_ratio >= 0.7 else "low"
    # backward compat for other formats
    company_name = (rec.get("PARTY_NAME") or rec.get("company_name") or rec.get("COMPANY NAME") or "").strip()
    country = (rec.get("country_format") or rec.get("country") or "").strip()
    if company_name and country:
        input_combined = f"{company_name} {country}".strip()
    elif company_name:
        input_combined = company_name
    else:
        return "low"
    found_combined = query.strip()
    if not found_combined:
        return "low"
    match_ratio = similarity(input_combined, found_combined)
    return "high" if match_ratio >= 0.7 else "low"


DEBUG = os.environ.get("DEBUG") in ("1", "true", "yes", "on")


def cgroup_memory_limit_mb():
    """Best-effort read of the container memory limit (cgroup v2/v1)."""
    for path in ("/sys/fs/cgroup/memory.max",
                 "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            with open(path) as f:
                raw = f.read().strip()
            if not raw or raw == "max":
                continue
            return int(raw) // (1024 * 1024)
        except Exception:
            continue
    return None


async def bing_api_lookup(req, query):
    """Bing Maps overlay API - now requires proper headers (fixed 2026-08-25).
    Parses both raw website regex AND data-entity JSON fallback."""
    url = OVERLAY.format(q=urllib.parse.quote(query))
    for attempt in range(2):
        try:
            resp = await req.get(url, timeout=20000)
            if not resp.ok:
                if DEBUG:
                    print(f"  [bing] HTTP {resp.status} for "
                          f"{query[:60]!r} (attempt {attempt + 1})", flush=True)
                if resp.status in (429, 403) and attempt == 0:
                    await asyncio.sleep(random.uniform(8, 15))
                    continue
                # try continue to next attempt instead of returning
                await asyncio.sleep(1)
                continue
            txt_raw = await resp.text()
            txt = html.unescape(txt_raw)
            # Method 1: direct regex for website (most common)
            # After unescape, data-entity JSON is plain: "website":"http://..."
            m = re.search(r'"website"\s*:\s*"(http[^"]+)"', txt)
            if m:
                site = clean_website(m.group(1))
                if site and ok_domain(site):
                    name = ""
                    seg = txt[max(0, m.start() - 3000):m.start()]
                    nm = re.findall(r'"title"\s*:\s*"([^"]+)"', seg)
                    if nm:
                        name = html.unescape(nm[-1])
                    else:
                        nm2 = re.findall(r'"name"\s*:\s*"([^"]+)"', seg)
                        if nm2:
                            name = html.unescape(nm2[-1])
                    return name, site
            # Method 2: parse data-entity JSON blobs (overlay lists multiple listings)
            # Extract all data-entity attributes from raw HTML (before unescape they are &quot; encoded)
            # Use raw text to find encoded entities, then unescape and json load
            raw_entities = re.findall(r'data-entity="([^"]+)"', txt_raw)
            # If not found in raw, try in unescaped txt (fallback)
            if not raw_entities:
                raw_entities = re.findall(r'data-entity=\'([^\']+)\'', txt)
            best_site = None
            best_name = ""
            for ent_raw in raw_entities[:5]:  # check first 5 listings
                try:
                    ent_json = html.unescape(ent_raw)
                    d = json.loads(ent_json)
                    ent = d.get("entity") or {}
                    site_cand = (ent.get("website") or "").strip()
                    if site_cand and site_cand.startswith("http") and ok_domain(site_cand):
                        # Prefer first with website that passes domain check
                        best_site = clean_website(site_cand)
                        best_name = ent.get("title") or ent.get("name") or ""
                        break  # take first valid
                except Exception:
                    continue
            if best_site:
                return best_name, best_site
            # Method 3: fallback regex for any http url near website keyword
            if DEBUG:
                # Differentiate between empty listing vs error card
                if "overlay-container error" in txt or "couldn't find" in txt.lower():
                    if DEBUG:
                        print(f"  [bing] overlay error card for {query[:60]!r}", flush=True)
                elif "data-entity" not in txt:
                    print(f"  [bing] empty result page ({len(txt)}b) for "
                          f"{query[:60]!r} - possible IP throttle/block", flush=True)
                else:
                    # has listings but none with website (common for 2/4 test queries)
                    if DEBUG:
                        print(f"  [bing] overlay has {len(raw_entities)} listings but no website for {query[:60]!r}", flush=True)
            return None, None
        except Exception as e:
            if DEBUG:
                print(f"  [bing] exception {e} for {query[:60]!r}", flush=True)
            await asyncio.sleep(1)
    return None, None


def decode_alink(href):
    """Decode bing.com/alink/link?url=... redirect wrappers."""
    try:
        q = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
        return (q.get("url") or [""])[0]
    except Exception:
        return None


async def bing_page_lookup(page, query):
    """Primary: rendered Bing Maps page -> (place_name, website).

    Two layouts occur: an overlay card whose `data-entity` JSON holds the
    website directly, or a search layout where the entity card renders in an
    iframe with `bing.com/alink` redirect anchors. Both are handled here.
    Fast mode: env FAST=1 or --fast reduces deadline 12->6s and poll 1500->600ms.
    """
    fast = os.environ.get("FAST") in ("1", "true", "yes", "on")
    url = "https://www.bing.com/maps?q=" + urllib.parse.quote(query)
    try:
        await page.goto(url, timeout=15000, wait_until="commit")
    except Exception:
        pass
    # give DOM a brief moment before first check
    try:
        await page.wait_for_timeout(800 if not fast else 400)
    except Exception:
        pass
    deadline = time.time() + (6 if fast else 8)
    poll = 600 if fast else 800
    while time.time() < deadline:
        try:
            card = page.locator("[data-entity]").first
            if await card.count():
                raw = html.unescape(await card.get_attribute("data-entity"))
                d = json.loads(raw)
                ent = d.get("entity") or {}
                site = (ent.get("website") or "").strip()
                if site:
                    return (ent.get("title") or "").strip(), clean_website(site)
        except Exception:
            pass
        for f in page.frames:
            try:
                links = f.locator('a[href*="bing.com/alink"]')
                n = await links.count()
                if not n:
                    continue
                for k in range(min(n, 10)):
                    href = await links.nth(k).get_attribute("href")
                    site = decode_alink(href)
                    if site and ok_domain(site):
                        try:
                            txt = (await links.nth(k).inner_text()).strip()
                        except Exception:
                            txt = ""
                        return txt, clean_website(site)
            except Exception:
                continue
        await page.wait_for_timeout(poll)
    # Also try to detect error card quickly and exit early to save time
    try:
        html_content = await page.content()
        if "couldn't find" in html_content.lower() and "overlay-container error" in html_content:
            return None, None
    except Exception:
        pass
    return None, None

# ---------- Bing Web Search fallback (added 2026-08-25) ----------
# When Maps has no website, try Bing Search top3 with domain scoring (same logic as bing_search_top3.py)
import base64

def decode_bing_href(href):
    if not href:
        return href
    href = html.unescape(href)
    if "bing.com/ck/a" not in href:
        return href
    try:
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
        u = (qs.get("u") or [None])[0]
        if not u:
            return href
        if len(u) >= 2 and u[0] == 'a' and u[1].isdigit():
            u = u[2:]
        u += '=' * (-len(u) % 4)
        decoded = base64.b64decode(u).decode("utf-8", errors="replace")
        if decoded.startswith("http"):
            return decoded
    except Exception:
        pass
    return href

def extract_core_domain(host):
    host = host.lower().strip()
    host = re.sub(r"^www\.", "", host).split(":")[0].split("/")[0]
    if not host:
        return ""
    parts = host.split(".")
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return parts[0]
    two_level_suffixes = {"co","com","net","org","ac","gov","edu","ne","or","ltd","plc"}
    if len(parts[-1]) == 2 and parts[-2] in two_level_suffixes and len(parts) >= 3:
        return parts[-3]
    return parts[-2]

def tokenize_company(name):
    if not name:
        return []
    name = name.lower()
    name = re.sub(r"[^0-9a-z\u00c0-\u024f]+", " ", name)
    tokens = name.split()
    filtered = []
    for t in tokens:
        t = t.strip()
        if len(t) < 2:
            continue
        if t in LEGAL_SUFFIXES:
            continue
        filtered.append(t)
    if not filtered:
        filtered = [t for t in tokens if len(t)>=2]
    return filtered

def score_domain(company_name, url):
    if not company_name or not url:
        return 0.0
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
    except:
        return 0.0
    host = re.sub(r"^www\.", "", host).split(":")[0]
    if not host:
        return 0.0
    core = extract_core_domain(host)
    if not core:
        return 0.0
    core = core.lower()
    host_nodot = host.replace(".", "").replace("-", "")
    tokens = tokenize_company(company_name)
    if not tokens:
        return 0.0
    company_concat = "".join(tokens)
    matched_geo_only = True
    best_sub_score = 0
    core_parts = re.split(r"[-_]+", core)
    for tok in tokens:
        is_geo = tok in GEO_TOKENS
        if len(tok) >= 3 and tok in core:
            if not is_geo:
                matched_geo_only = False
            if len(tok) >= 5 and len(tok)/len(core) >= 0.4:
                best_sub_score = max(best_sub_score, 0.93)
            else:
                best_sub_score = max(best_sub_score, 0.87)
        if len(core) >= 4 and core in tok:
            if not is_geo:
                matched_geo_only = False
            best_sub_score = max(best_sub_score, 0.86)
        if len(tok) >= 3 and any(tok == cp or tok in cp for cp in core_parts):
            if not is_geo:
                matched_geo_only = False
            best_sub_score = max(best_sub_score, 0.88)
    if best_sub_score > 0:
        if matched_geo_only and len(tokens) > 1:
            return 0.35
        return best_sub_score
    if 2 <= len(core) <= 4:
        initials = "".join(t[0] for t in tokens if t)
        if core == initials[:len(core)]:
            return 0.90
        if core == initials:
            return 0.92
        if tokens and core == tokens[0][:len(core)] and len(core)>=2:
            return 0.65
    host_for_coverage = host_nodot
    matches = 0
    geo_matches = 0
    for tok in tokens:
        if len(tok) >= 3 and tok in host_for_coverage:
            matches += 1
            if tok in GEO_TOKENS:
                geo_matches += 1
        elif len(tok) >= 3 and any(tok in part for part in core_parts):
            matches += 1
            if tok in GEO_TOKENS:
                geo_matches += 1
    if matches > 0 and matches == geo_matches and len(tokens) > 1:
        coverage = 0
    else:
        effective_matches = matches - geo_matches if (matches - geo_matches) > 0 else matches
        if matches == geo_matches:
            coverage = matches / len(tokens) * 0.4
        else:
            coverage = effective_matches / len([t for t in tokens if t not in GEO_TOKENS]) if any(t not in GEO_TOKENS for t in tokens) else matches/len(tokens)
    if coverage >= 0.5:
        return 0.78 + coverage * 0.15
    if coverage >= 0.33 and len(tokens) >= 3:
        return 0.62 + coverage * 0.2
    ratio_core = difflib.SequenceMatcher(None, company_concat, core).ratio()
    ratio_host = difflib.SequenceMatcher(None, company_concat, host_nodot).ratio()
    ratio = max(ratio_core, ratio_host)
    if coverage > 0:
        ratio = max(ratio, coverage * 0.7)
    return ratio

def pick_best_url(company_name, urls, threshold=0.5):
    if not urls:
        return None, 0.0
    scored=[]
    for u in urls:
        if not u or not ok_domain(u):
            continue
        if "bing.com" in u or "microsoft.com" in u:
            continue
        s = score_domain(company_name, u)
        scored.append((s, u))
    if not scored:
        return None, 0.0
    scored.sort(reverse=True)
    best_score, best_url = scored[0]
    if best_score < threshold:
        return None, best_score
    best_url = truncate_to_base_domain(best_url)
    return best_url, best_score

def _ok_bing_url(url):
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
    except:
        return False
    host = re.sub(r"^www\.", "", host)
    if not host or host in BAD_DOMAINS:
        return False
    for bad in BAD_DOMAINS:
        if host==bad or host.endswith("."+bad):
            return False
    return True

async def bing_search_fallback(req, company_name, query, threshold=0.55):
    """Fallback when Maps has no website: query Bing Search top3 and pick best domain match."""
    if not query or not company_name:
        return None, None
    # Use httpx via req's underlying client if possible; otherwise create new
    q = urllib.parse.quote(query)
    url = f"https://www.bing.com/search?q={q}&count=10&setlang=en"
    headers = {
        "User-Agent": UA,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.bing.com/",
    }
    txt = ""
    try:
        # req is HttpxRequest with _client that now has headers; but for search we need fresh headers
        # Create a temporary client to avoid polluting req's default Bing Maps headers
        proxy = os.environ.get("PROXY_URL") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or None
        async with httpx.AsyncClient(timeout=15, follow_redirects=True, proxy=proxy, headers=headers) as c:
            r = await c.get(url)
            if r.status_code != 200:
                return None, None
            txt = r.text
    except Exception:
        return None, None
    if "Our systems have detected unusual traffic" in txt:
        return None, None
    urls=[]
    blocks = re.findall(r'<li class="b_algo[^>]*>.*?</li>', txt, flags=re.DOTALL)
    if blocks:
        for b in blocks:
            m = re.search(r'<a[^>]+href="(https?://[^"]+)"', b)
            if not m:
                continue
            href = html.unescape(m.group(1)).replace("&amp;", "&")
            decoded = decode_bing_href(href)
            if "bing.com" in decoded:
                continue
            if decoded.startswith("http") and _ok_bing_url(decoded):
                urls.append(decoded)
            if len(urls) >= 3:
                break
    else:
        for m in re.finditer(r'href="(https?://[^"]+)"', txt):
            href = html.unescape(m.group(1))
            if "bing.com" in href or "microsoft.com" in href:
                continue
            decoded = decode_bing_href(href)
            if decoded.startswith("http") and decoded not in urls and _ok_bing_url(decoded):
                urls.append(decoded)
            if len(urls) >= 3:
                break
    urls = urls[:3]
    if not urls:
        return None, None
    best_url, best_score = pick_best_url(company_name, urls, threshold=threshold)
    if best_url:
        return company_name, best_url  # return as (name, site) to mimic Maps
    return None, None


async def bing_lookup(page, req, query, company_name=None):
    """Hybrid: Bing overlay API fast-path (httpx) -> Bing Maps page scraping -> Bing Search top3 fallback.
    Fixed 2026-08-25: added Search fallback to restore 1/sec after Maps overlay degraded.
    Works without browser (overlay+search) and with browser (page) for low-disk."""
    # Disable overlay via --no-overlay or env NO_OVERLAY=1
    use_overlay = not (os.environ.get("NO_OVERLAY") in ("1", "true", "yes", "on"))
    # Fast path: overlay API (no browser needed, ~0.5s)
    if use_overlay and req is not None:
        try:
            name, site = await bing_api_lookup(req, query)
            if site and ok_domain(site):
                return name, site
        except Exception:
            pass
    # Fallback: rendered Bing Maps page (requires Playwright page)
    if page is not None:
        try:
            name, site = await bing_page_lookup(page, query)
            if site and ok_domain(site):
                return name, site
        except Exception:
            pass
    # NEW Fallback 2026-08-25: Bing Search top3 with domain scoring (works via httpx only, no browser needed)
    # This restores throughput when Maps has no listing (e.g., KENCANA, AE Technology showed no website in Maps)
    # Use company_name for scoring; fallback threshold 0.55 same as bing_search_top3
    if req is not None and company_name:
        try:
            # Check env var to disable search fallback if needed
            if os.environ.get("DISABLE_BING_SEARCH") not in ("1","true","yes","on"):
                name2, site2 = await bing_search_fallback(req, company_name, query, threshold=0.55)
                if site2 and ok_domain(site2):
                    return name2, site2
        except Exception:
            pass
    return None, None


async def google_lookup(page, query):
    """Fallback: rendered Google Maps -> (place_name, website)."""
    url = "https://www.google.com/maps/search/?api=1&query=" + urllib.parse.quote(query)
    try:
        await page.goto(url, timeout=30000, wait_until="domcontentloaded")
    except Exception:
        try:
            await page.wait_for_timeout(3000)
        except Exception:
            pass
    try:
        await page.wait_for_selector(".Nv2PK", timeout=8000)
    except Exception:
        pass
    website = None
    try:
        link = page.locator('a[href^="http"]').filter(has_text="Website").first
        await link.wait_for(state="visible", timeout=4000)
        website = await link.get_attribute("href")
    except Exception:
        try:
            await page.locator(".Nv2PK").first.click(timeout=4000)
            await page.wait_for_timeout(1000)
            link = page.locator('a[href^="http"]').filter(has_text="Website").first
            await link.wait_for(state="visible", timeout=4000)
            website = await link.get_attribute("href")
        except Exception:
            pass
    name = ""
    try:
        h1 = page.locator("h1").first
        await h1.wait_for(state="visible", timeout=2000)
        name = (await h1.inner_text()).strip()
    except Exception:
        pass
    return name, clean_website(website)


async def process_one(rec, req, page):
    query = build_query(rec)
    if not query:
        return [rec["id"], rec["keyid"], query, "", "", "", "", "none", "no query"]
    error = ""
    b_name, b_site = None, None
    g_name, g_site = None, None
    company_name = rec.get("PARTY_NAME") or rec.get("company_name") or ""
    b_name, b_site = await bing_lookup(page, req, query, company_name=company_name)
    if not (b_site and ok_domain(b_site)):
        short = short_query(rec)
        if short != query:
            b2, s2 = await bing_lookup(page, req, short, company_name=company_name)
            if s2 and ok_domain(s2) and not b_site:
                b_name, b_site = b2, s2
    if b_site and ok_domain(b_site):
        status = "bing"
        verification = verify_match(rec, query)
        return [rec["id"], rec["keyid"], query,
                verification, b_site or "",
                "", "",
                status, error.strip("; ")]
    elif page is not None:
        g_name, g_site = await google_lookup(page, query)
        if g_site and ok_domain(g_site):
            status = "google"
            verification = verify_match(rec, query)
            return [rec["id"], rec["keyid"], query,
                    "", "",
                    verification, g_site or "",
                    status, error.strip("; ")]
        else:
            status = "none"
            return [rec["id"], rec["keyid"], query,
                    "", "",
                    "", "",
                    status, error.strip("; ")]
    else:
        status = "none"
        return [rec["id"], rec["keyid"], query,
                "", "",
                "", "",
                status, error.strip("; ")]


async def run_worker(chunk, recs, req, page, sink, min_delay, max_delay):
    for i in chunk:
        rec = recs[i]
        row = await process_one(rec, req, page)
        await sink.push(row, rec)
        if min_delay:
            await asyncio.sleep(random.uniform(min_delay, max_delay))


async def launch_browser(workers, args):
    """Launch one browser with `workers` tabs; auto-install Chromium if missing."""
    pages = [None] * workers
    req = None
    playwright_ctx = None
    mem_mb = cgroup_memory_limit_mb()
    if mem_mb is not None and mem_mb < 1024:
        print(f"Warning: container memory limit is only {mem_mb} MB; "
              f"Chromium needs roughly 1 GB. Skipping browser (Bing overlay API "
              f"only; page scraping and Google fallback disabled). Raise the server "
              f"memory or preinstall Chromium via the panel install script to "
              f"enable full pipeline.", flush=True)
        return pages, req, playwright_ctx
    for attempt in range(2):
        try:
            playwright_ctx = await async_playwright().start()
            ctx = await playwright_ctx.chromium.launch_persistent_context(
                str(Path(args.profile)), headless=not args.headful,
                user_agent=UA, viewport={"width": 1280, "height": 800},
                locale="en-US",
                args=["--disable-blink-features=AutomationControlled"],
            )
            new_pages = []
            for _ in range(workers):
                pg = await ctx.new_page()
                await pg.add_init_script(
                    "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
                await pg.route("**/*", lambda route: route.abort()
                               if route.request.resource_type in ("image", "media", "font")
                               else route.continue_())
                new_pages.append(pg)
            pages = new_pages
            req = ctx.request
            try:
                await pages[0].goto("https://www.bing.com/maps?q=test",
                                    timeout=45000, wait_until="domcontentloaded")
                await pages[0].wait_for_timeout(2500)
            except Exception:
                pass
            break
        except Exception as e:
            if playwright_ctx:
                try:
                    await playwright_ctx.stop()
                except Exception:
                    pass
                playwright_ctx = None
            if attempt == 0 and "Executable doesn't exist" in str(e):
                print("Playwright browser missing - installing chromium ...",
                      flush=True)
                cmd = [sys.executable, "-m", "playwright", "install"]
                if not args.headful:
                    cmd.append("--only-shell")
                cmd.append("chromium")
                if os.name == "posix" and hasattr(os, "geteuid") and os.geteuid() == 0:
                    cmd.insert(3, "--with-deps")
                rc = -1
                try:
                    rc = subprocess.run(cmd, check=False, timeout=900).returncode
                except Exception:
                    pass
                if rc in (137, -9):
                    print("Warning: browser install was killed by the OS "
                          "(out of memory). Falling back to Bing overlay API only; "
                          "page scraping and Google fallback disabled. Raise the server "
                          "memory or preinstall Chromium via the panel install script "
                          "(python -m playwright install chromium).",
                          flush=True)
                    break
                continue
            print(f"Warning: could not launch browser ({e}). "
                  f"Bing overlay API only; page scraping and Google fallback disabled.", flush=True)
            break
    return pages, req, playwright_ctx


async def amain(args):
    # Honor --no-overlay / --fast flags via env for bing_lookup
    if getattr(args, "no_overlay", False):
        os.environ["NO_OVERLAY"] = "1"
    if getattr(args, "fast", False):
        os.environ["FAST"] = "1"
    # Initialize Supabase
    sb_ok = init_supabase()
    sb_client = httpx.Client(timeout=30) if sb_ok else None

    global SRC
    SRC = str(Path(args.input).resolve())

    recs = [r for r in read_records()]

    out_path = Path(args.out)
    # Auto-switch output if existing file header mismatches input format (news vs new 4-col vs old 10-col)
    if out_path.exists() and out_path.stat().st_size > 0 and recs:
        try:
            with open(out_path, newline="", encoding="utf-8-sig") as f:
                existing_hdr = next(csv.reader(f), None)
                if existing_hdr:
                    ehdr_norm = [_norm_hdr(h) for h in existing_hdr]
                    existing_is_news = "company name" in ehdr_norm and "duns_number" in ehdr_norm
                    existing_is_new = "party_name" in ehdr_norm and "country_format" in ehdr_norm
                    if existing_is_news:
                        existing_fmt = "news"
                    elif existing_is_new:
                        existing_fmt = "new"
                    else:
                        existing_fmt = "old"
                    expected_fmt = recs[0].get("__format", "old")
                    # also fallback detection for recs without __format
                    if not expected_fmt or expected_fmt not in ("news", "new", "old"):
                        if "COMPANY NAME" in recs[0] and "ENTITY_TYPE" in recs[0]:
                            expected_fmt = "news"
                        elif "PARTY_NAME" in recs[0] and "country_format" in recs[0]:
                            expected_fmt = "new"
                        else:
                            expected_fmt = "old"
                    if existing_fmt != expected_fmt:
                        base = out_path.stem
                        suffix_map = {"news": "_news", "new": "_companycountry", "old": "_legacy"}
                        target_suffix = suffix_map.get(expected_fmt, "")
                        if target_suffix and not base.endswith(target_suffix):
                            new_out = out_path.with_name(base + target_suffix + out_path.suffix)
                        else:
                            new_out = out_path
                        if new_out != out_path:
                            print(f"Warning: existing output '{out_path.name}' has "
                                  f"{existing_fmt} header but input is {expected_fmt} format. "
                                  f"Switching output to '{new_out.name}' to avoid mixing formats. "
                                  f"Use --out to override.", flush=True)
                            out_path = new_out
        except Exception:
            pass

    done = set()
    if out_path.exists() and out_path.stat().st_size > 0:
        with open(out_path, newline="", encoding="utf-8-sig") as f:
            rd = csv.reader(f)
            hdr = next(rd, None)
            keyid_idx = 1  # default for old 10-col file (ID, KEYID, ...)
            if hdr:
                hdr_norm = [_norm_hdr(h) for h in hdr]
                if "keyid" in hdr_norm:
                    keyid_idx = hdr_norm.index("keyid")
                else:
                    # no header - treat first row as data
                    if len(hdr) > keyid_idx and hdr[keyid_idx]:
                        done.add(hdr[keyid_idx].strip())
            for r in rd:
                if len(r) > keyid_idx and r[keyid_idx]:
                    done.add(r[keyid_idx].strip())

    # Also check Supabase for already processed records
    if sb_client:
        sb_done = await asyncio.to_thread(fetch_done_keyids, sb_client)
        done.update(sb_done)
        print(f"Supabase already has {len(sb_done)} records", flush=True)

    fresh = not out_path.exists() or (out_path.exists() and out_path.stat().st_size == 0)
    total = len(recs)
    todo_idx = [i for i in range(args.start, total)
                if recs[i]["keyid"] not in done]
    if args.limit:
        todo_idx = todo_idx[:args.limit]
    print(f"total records: {total}, already done: {len(done)}, "
          f"to process now: {len(todo_idx)}", flush=True)
    if not todo_idx:
        print("nothing to do", flush=True)
        if sb_client:
            sb_client.close()
        return

    workers = max(1, min(args.workers, len(todo_idx)))
    chunks = [[] for _ in range(workers)]
    for pos, i in enumerate(todo_idx):
        chunks[pos % workers].append(i)

    stats = {"processed": 0, "bing": 0, "google": 0, "none": 0,
             "errors": 0, "start": time.time()}

    pages = [None] * workers
    playwright_ctx = None
    req = None
    if not args.no_browser:
        pages, req, playwright_ctx = await launch_browser(workers, args)
    if req is None:
        # Hybrid: overlay API works without browser; create Httpx client for fast path
        req = HttpxRequest()
        proxy = (os.environ.get("PROXY_URL") or os.environ.get("HTTPS_PROXY")
                 or os.environ.get("HTTP_PROXY") or None)
        if args.no_browser:
            print(f"  [net] --no-browser: using Bing overlay API via proxy: {proxy or 'none (direct)'} (page fallback disabled)", flush=True)
        elif not pages or pages[0] is None:
            print(f"  [net] Browser not available (memory/disk) — using Bing overlay API via proxy: {proxy or 'none (direct)'}; page fallback disabled", flush=True)
        else:
            print(f"  [net] Bing overlay API via proxy: {proxy or 'none (direct)'} + page fallback", flush=True)

    out_f = open(out_path, "a", newline="", encoding="utf-8-sig")
    wtr = csv.writer(out_f)
    if fresh:
        sample = recs[0] if recs else {}
        fmt = sample.get("__format", "")
        if fmt == "news" or ("COMPANY NAME" in sample and "ENTITY_TYPE" in sample):
            # News_Without_Web.xlsx: ID, keyid, COMPANY NAME, Web Address, ENTITY_TYPE, DUNS_NUMBER, dup, URL
            wtr.writerow(["ID", "keyid", "COMPANY NAME", "Web Address", "ENTITY_TYPE", "DUNS_NUMBER", "dup", "URL"])
        elif fmt == "new" or ("PARTY_NAME" in sample and "country_format" in sample):
            wtr.writerow(["KEYID", "PARTY_NAME", "ACCOUNT_ALIAS", "country_format", "URL", "high/low"])
        else:
            wtr.writerow(["ID", "KEYID", "Company_NAME", "ACCOUNT_ALIAS",
                          "ADDRESS1", "ADDRESS2", "STATE", "CITY",
                          "POSTAL_CODE", "COUNTRY", "URL", "high/low"])
    out_f.flush()
    sink = Sink(wtr, out_f, sb_client, stats, len(todo_idx), args.quiet)
    flusher_task = asyncio.create_task(sink.flusher())
    try:
        tasks = [asyncio.create_task(
            run_worker(chunks[w], recs, req, pages[w], sink,
                       args.min_delay, args.max_delay))
            for w in range(workers)]
        await asyncio.gather(*tasks)
    finally:
        await sink.stop()
        await flusher_task
        out_f.close()
        if sb_client:
            sb_client.close()
        if hasattr(req, "close"):
            try:
                await req.close()
            except Exception:
                pass
        if playwright_ctx:
            try:
                await playwright_ctx.stop()
            except Exception:
                pass

    print(f"done. processed={stats['processed']} bing={stats['bing']} "
          f"google={stats['google']} none={stats['none']} "
          f"errors={stats['errors']}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--limit", type=int, default=0, help="0 = no limit")
    ap.add_argument("--input", default=os.environ.get("INPUT_CSV") or SRC,
                    help="input CSV path (env INPUT_CSV overrides)")
    ap.add_argument("--workers", type=int,
                    default=int(os.environ.get("WORKERS", "20")),
                    help="number of parallel tabs inside ONE browser "
                         "(env WORKERS overrides)")
    ap.add_argument("--out", default=str(OUT))
    ap.add_argument("--profile", default=str(PROFILE))
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--no-browser", action="store_true",
                    default=os.environ.get("NO_BROWSER") in ("1", "true", "yes", "on"),
                    help="skip Google fallback; Bing API only (useful on hosts "
                         "where Chromium cannot be installed; env NO_BROWSER=1 "
                         "also works)")
    ap.add_argument("--no-overlay", action="store_true",
                    default=os.environ.get("NO_OVERLAY") in ("1", "true", "yes", "on"),
                    help="disable Bing overlay API (https://www.bing.com/maps/overlaybfpr); use rendered Maps page only (requires browser; env NO_OVERLAY=1 also works)")
    ap.add_argument("--fast", action="store_true",
                    default=os.environ.get("FAST") in ("1", "true", "yes", "on"),
                    help="fast mode: Maps page deadline 6s vs 8s, poll 600ms vs 800ms, goto commit (env FAST=1)")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--min-delay", type=float,
                    default=float(os.environ.get("MIN_DELAY", "0.3")))
    ap.add_argument("--max-delay", type=float,
                    default=float(os.environ.get("MAX_DELAY", "0.8")))
    args = ap.parse_args()
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        print("\nstopped by user; progress is saved", flush=True)


if __name__ == "__main__":
    main()