#!/usr/bin/env python3
"""
WhatsApp Number Miner - zero-cost US mobile WhatsApp number extractor.
Streams NANPA public data, classifies wireless carriers, probes via self-hosted
GOWA v9.8.0 with anti-ban pacing. Single-file FastAPI backend.

FIXES APPLIED (v3.4.0):
  1. Phase A 20-number sample burst on 732-200-6 (OCN 4036, NEW CINGULAR WIRELESS PCS, LLC - DC)
     with auto-seed probes from backups/npa_732_probes_seed.json into gowa_devices,
     number_probes, wireless_blocks, and sampler.stats on startup.
  2. GET /api/backup/probes?npa=732&format=json|csv endpoint (no auth, matching /api/backup/status)
     returning all number_probes rows for the given NPA plus summary counts.
  3. seed_probes_if_empty(npa_target="732") called inside _startup_auto_seed() reads
     DATA_DIR/npa_732_probes_seed.json, inserts device "spare_1" into gowa_devices,
     inserts probe rows into number_probes, updates wireless_blocks for block 732-200-6,
     and updates sampler.stats.

FIXES APPLIED (v3.3.3):
  1. GOWA v9.8.0 multi-device registry integration: POST /devices to register device
     slots in GOWA DeviceManager before /devices/{id}/login, /devices/{id}/login/code,
     and /devices/{id}/logout so DeviceMiddleware never returns 404 DEVICE_NOT_FOUND.
  2. Server-side QR image fetch & base64 data-URI encoding in GowaClient.login_qr()
     so internal http://127.0.0.1:3000/statics/qrcode/*.png images render directly in
     the browser pairing modal.
  3. Normalize pair_code / code fields in GowaClient.login_code() and start sampler
     in Idle state on container boot until explicitly started.

FIXES APPLIED (v3.3.2):
  1. 8-minute self keep-alive background loop pinging /health so Render free tier
     does not spin down after 15 minutes of inactivity.
  2. Startup auto-seed for NPA 732: loads seed_732.json / backups if present (<1s), or
     auto-ingests NPA 732 in a background thread on boot if wireless_blocks is empty.
  3. /api/backup/blocks (format=json|csv) and /api/backup/status endpoints.
"""

import asyncio
import base64
import csv
import io
import json
import logging
import os
import random
import re
import sqlite3
import tempfile
import threading
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request, Depends, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

# ── Config ──────────────────────────────────────────────────────────────────
APP_KEY = os.getenv("APP_PASSWORD", "")
GOWA_URL = os.getenv("GOWA_URL", "http://127.0.0.1:3000")
GOWA_CREDS = os.getenv("GOWA_BASIC_AUTH", "")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////app/data/miner.db")
TARGET_NPA = os.getenv("TARGET_NPA", "732") or "732"
KEEP_ALIVE_URL = os.getenv("RENDER_EXTERNAL_URL", "https://whatsapp-number-miner.onrender.com")
KEEP_ALIVE_INTERVAL = int(os.getenv("KEEP_ALIVE_INTERVAL", "480"))  # 8 minutes

if not APP_KEY:
    raise RuntimeError("APP_PASSWORD environment variable is required")
if not GOWA_CREDS:
    raise RuntimeError("GOWA_BASIC_AUTH environment variable is required")

if DATABASE_URL.startswith("sqlite"):
    db_path = DATABASE_URL.replace("sqlite:///", "")
    DATA_DIR = str(Path(db_path).parent)
else:
    DATA_DIR = "/app/data"

Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

CO_CODES_URL = (
    "https://reports.nanpa.com/public/"
    "CoCodeAssignment_Utilized_AllStates_Public.zip"
)
THOUSANDS_URL = (
    "https://reports.nanpa.com/public/"
    "ThousandsBlockAssignment_All.zip"
)

# ── OCN Classifier ──────────────────────────────────────────────────────────

WIRELESS_KEYWORDS = [
    "CELLCO", "VERIZON WIRELESS",
    "NEW CINGULAR", "AT&T MOBILITY", "CINGULAR WIRELESS",
    "T-MOBILE", "METRO PCS", "METROPCS",
    "SPRINT", "NEXTEL",
    "US CELLULAR", "USCOC",
    "DISH WIRELESS", "GOOGLE NORTH AMERICA",
    "CRICKET", "BOOST MOBILE", "TRACFONE",
    "CLEARWIRE", "VIRGIN MOBILE",
    "OMNIPOINT", "AERIAL COMMUNICATIONS", "VOICESTREAM", "POWERTEL",
    "SOUTHWESTERN BELL MOBILE", "BELLSOUTH MOBILITY",
    "PACIFIC BELL WIRELESS",
]

WIRELINE_DROP_KEYWORDS = [
    "BANDWIDTH.COM", "SINCH", "ONVOY", "INTELLECTUAL",
    "PEERLESS", "TWILIO", "LEVEL 3", "LUMEN",
    "COMCAST", "CHARTER", "FRONTIER", "CONSOLIDATED",
    "BELLSOUTH TELECOMMUNICATIONS", "SOUTHWESTERN BELL TELEPHONE",
    "PACIFIC BELL TELEPHONE", "QWEST", "CENTURYLINK",
    "WINDSTREAM", "FAIRPOINT", "HAWAIIAN TEL", "GTE",
    "YMAX", "TELEPORT", "BROADVIEW", "XO COMMUNICATIONS",
    "CENTURYLINK COMMUNICATIONS", "BRIGHTLINK",
    "NEUTRAL TANDEM", "INTELIQUENT",
    "VERIZON NEW JERSEY", "VERIZON NEW YORK", "VERIZON CALIFORNIA",
    "VERIZON FLORIDA", "VERIZON NORTH", "VERIZON SOUTH",
    "VERIZON VIRGINIA", "VERIZON PENNSYLVANIA",
    "VERIZON DELAWARE", "VERIZON MARYLAND",
    "VERIZON NEW ENGLAND", "VERIZON WASHINGTON",
    "BELLSOUTH TELECOM", "SOUTHWESTERN BELL",
    "PACIFIC BELL", "NEVADA BELL", "ILLINOIS BELL",
    "MICHIGAN BELL", "OHIO BELL", "WISCONSIN BELL",
    "INDIANA BELL", "SOUTHERN BELL",
    "AT&T CORP.", "AT&T LOCAL",
    "CENTURYLINK", "QWEST CORPORATION",
]

# ── Anti-Ban Pacing ─────────────────────────────────────────────────────────

BURST_MIN_CHECKS = 12
BURST_MAX_CHECKS = 25
BURST_COOLDOWN_MIN = 25 * 60
BURST_COOLDOWN_MAX = 45 * 60
CHECK_DELAY_MEAN = 4.5
CHECK_DELAY_STD = 1.5
CHECK_DELAY_MIN = 2.0
CHATTER_INTERVAL_MIN = 15
CHATTER_INTERVAL_MAX = 30
SAMPLE_SIZE = 20
COOLDOWN_HOURS = 48
CAPS = {"NEW": 150, "WARMING": 500, "MATURE": 1800}
NIGHT_START_HOUR = 23
NIGHT_END_HOUR = 7

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("miner")

# ── Database ────────────────────────────────────────────────────────────────

_db_conn: Optional[sqlite3.Connection] = None
_db_lock = threading.Lock()
_is_postgres = DATABASE_URL.startswith("postgresql://") or DATABASE_URL.startswith("postgres://")

def _open_db() -> sqlite3.Connection:
    if _is_postgres:
        raise RuntimeError(
            "PostgreSQL support requires asyncpg + SQLAlchemy. "
            "Set DATABASE_URL to sqlite:////app/data/miner.db for SQLite."
        )
    path = DATABASE_URL.replace("sqlite:///", "")
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def get_db() -> sqlite3.Connection:
    global _db_conn
    if _db_conn is None:
        with _db_lock:
            if _db_conn is None:
                _db_conn = _open_db()
    return _db_conn

def init_db():
    db = get_db()
    with _db_lock:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS wireless_blocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                npa TEXT NOT NULL,
                nxx TEXT NOT NULL,
                block TEXT NOT NULL,
                ocn TEXT,
                company_name TEXT,
                source TEXT DEFAULT 'unknown',
                status TEXT DEFAULT 'PENDING',
                sample_hits INTEGER DEFAULT 0,
                sample_total INTEGER DEFAULT 0,
                numbers_checked INTEGER DEFAULT 0,
                numbers_found INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(npa, nxx, block)
            );
            CREATE TABLE IF NOT EXISTS gowa_devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT UNIQUE NOT NULL,
                display_name TEXT,
                phone_number TEXT,
                proxy_url TEXT,
                age_days INTEGER DEFAULT 1,
                daily_checks INTEGER DEFAULT 0,
                daily_check_date TEXT,
                status TEXT DEFAULT 'NEW',
                cooldown_until TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                last_check_at TEXT
            );
            CREATE TABLE IF NOT EXISTS number_probes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT UNIQUE NOT NULL,
                npa TEXT,
                nxx TEXT,
                block TEXT,
                is_on_whatsapp INTEGER,
                device_id TEXT,
                probed_at TEXT DEFAULT (datetime('now')),
                error TEXT
            );
            CREATE TABLE IF NOT EXISTS ocn_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ocn TEXT NOT NULL,
                company_name TEXT,
                is_wireless INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(ocn)
            );
            CREATE TABLE IF NOT EXISTS csrf_tokens (
                token TEXT PRIMARY KEY,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_wb_npa ON wireless_blocks(npa);
            CREATE INDEX IF NOT EXISTS idx_wb_status ON wireless_blocks(status);
            CREATE INDEX IF NOT EXISTS idx_np_phone ON number_probes(phone);
            CREATE INDEX IF NOT EXISTS idx_np_whatsapp ON number_probes(is_on_whatsapp);
            CREATE INDEX IF NOT EXISTS idx_np_block ON number_probes(npa, nxx, block);
            CREATE INDEX IF NOT EXISTS idx_gd_device ON gowa_devices(device_id);
        """)
        db.commit()
    return db


def seed_from_json_if_empty(npa_target: str = "732") -> int:
    """Fast startup seed from seed_732.json if wireless_blocks is empty."""
    db = get_db()
    count = db.execute("SELECT COUNT(*) as c FROM wireless_blocks WHERE npa = ?", (npa_target,)).fetchone()["c"]
    if count > 0:
        return count

    candidates = [
        Path(__file__).parent / f"seed_{npa_target}.json",
        Path(DATA_DIR) / f"seed_{npa_target}.json",
        Path(__file__).parent / "backups" / f"npa_{npa_target}_wireless_blocks.json",
    ]
    for seed_path in candidates:
        if seed_path.exists():
            try:
                payload = json.loads(seed_path.read_text(encoding="utf-8"))
                blocks = payload.get("blocks", payload) if isinstance(payload, dict) else payload
                inserted = 0
                with _db_lock:
                    for b in blocks:
                        db.execute(
                            """INSERT OR IGNORE INTO wireless_blocks
                               (npa, nxx, block, ocn, company_name, source, status)
                               VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            (
                                b.get("npa", npa_target),
                                b["nxx"],
                                str(b["block"]),
                                b.get("ocn"),
                                b.get("company_name"),
                                b.get("source", "seed_json"),
                                b.get("status", "PENDING"),
                            ),
                        )
                        inserted += 1
                    db.commit()
                log.info(f"Auto-seeded {inserted} blocks for NPA {npa_target} from {seed_path}")
                return inserted
            except Exception as e:
                log.warning(f"Failed reading seed file {seed_path}: {e}")
    return 0


def seed_probes_if_empty(npa_target: str = "732") -> int:
    """v3.4.0: Seed probe results from backups/npa_732_probes_seed.json on startup.

    Reads the Phase A sample burst JSON, inserts device "spare_1" into gowa_devices,
    inserts probe rows into number_probes, updates wireless_blocks for the sampled block,
    and updates sampler.stats. Only runs if number_probes is empty for the NPA.
    """
    db = get_db()
    existing = db.execute(
        "SELECT COUNT(*) as c FROM number_probes WHERE npa = ?", (npa_target,)
    ).fetchone()["c"]
    if existing > 0:
        log.info(f"seed_probes_if_empty: {existing} probes already exist for NPA {npa_target}, skipping")
        return existing

    candidates = [
        Path(__file__).parent / "backups" / f"npa_{npa_target}_probes_seed.json",
        Path(DATA_DIR) / f"npa_{npa_target}_probes_seed.json",
    ]
    for seed_path in candidates:
        if seed_path.exists():
            try:
                payload = json.loads(seed_path.read_text(encoding="utf-8"))
                probes = payload.get("probes", [])
                if not probes:
                    log.warning(f"seed_probes_if_empty: no probes in {seed_path}")
                    continue

                device_id = payload.get("device_id", "spare_1")
                phone_number = payload.get("phone_number", "19292718201")
                sample_size = payload.get("sample_size", len(probes))
                block_sampled = payload.get("block_sampled", "")

                inserted_probes = 0
                hits = 0

                with _db_lock:
                    # Insert device "spare_1"
                    db.execute(
                        """INSERT OR IGNORE INTO gowa_devices
                           (device_id, phone_number, status, daily_checks, daily_check_date)
                           VALUES (?, ?, 'NEW', ?, date('now'))""",
                        (device_id, phone_number, len(probes)),
                    )

                    # Insert probe rows
                    for p in probes:
                        phone = p.get("phone", "")
                        npa = p.get("npa", npa_target)
                        nxx = p.get("nxx", "")
                        block = p.get("block", "")
                        is_wa = int(p.get("is_on_whatsapp", 0))
                        probed_at = p.get("probed_at", datetime.now(timezone.utc).isoformat())
                        if is_wa:
                            hits += 1
                        db.execute(
                            """INSERT OR IGNORE INTO number_probes
                               (phone, npa, nxx, block, is_on_whatsapp, device_id, probed_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            (phone, npa, nxx, block, is_wa, device_id, probed_at),
                        )
                        inserted_probes += 1

                    # Update wireless_blocks for the sampled block
                    if block_sampled and "-" in block_sampled:
                        parts = block_sampled.split("-")
                        if len(parts) == 3:
                            b_npa, b_nxx, b_blk = parts[0], parts[1], parts[2]
                            new_status = "ACTIVE_SWEEPING" if hits > 0 else "DORMANT_SKIPPED"
                            db.execute(
                                """INSERT OR IGNORE INTO wireless_blocks
                                   (npa, nxx, block, ocn, company_name, source, status,
                                    sample_hits, sample_total, numbers_checked, numbers_found)
                                   VALUES (?, ?, ?, '4036', 'NEW CINGULAR WIRELESS PCS, LLC - DC',
                                    'phase_a_burst', ?, ?, ?, ?, ?)""",
                                (b_npa, b_nxx, b_blk, new_status, hits, sample_size, sample_size, hits),
                            )
                            # Also update if already exists
                            db.execute(
                                """UPDATE wireless_blocks SET
                                   sample_hits=?, sample_total=?, numbers_checked=?,
                                   numbers_found=?, status=?, updated_at=datetime('now')
                                   WHERE npa=? AND nxx=? AND block=?""",
                                (hits, sample_size, sample_size, hits, new_status, b_npa, b_nxx, b_blk),
                            )

                    db.commit()

                # Update sampler.stats
                sampler.stats["numbers_checked"] += sample_size
                sampler.stats["numbers_found"] += hits
                if hits > 0:
                    sampler.stats["blocks_sweeping"] += 1
                    sampler.stats["queries_saved"] += 0
                else:
                    sampler.stats["blocks_skipped"] += 1
                    sampler.stats["queries_saved"] += 980
                sampler.stats["current_device"] = device_id

                log.info(
                    f"seed_probes_if_empty: seeded {inserted_probes} probes for NPA {npa_target} "
                    f"from {seed_path} (hits={hits}, device={device_id}, "
                    f"block={block_sampled}, status={new_status if block_sampled else 'N/A'})"
                )
                return inserted_probes
            except Exception as e:
                log.warning(f"seed_probes_if_empty: failed reading {seed_path}: {e}")
    return 0


def is_wireless_ocn(ocn: str, company_name: str, db: sqlite3.Connection) -> bool:
    row = db.execute(
        "SELECT is_wireless FROM ocn_overrides WHERE ocn = ?", (ocn,)
    ).fetchone()
    if row:
        return bool(row["is_wireless"])
    name_upper = (company_name or "").upper().strip()
    for kw in WIRELINE_DROP_KEYWORDS:
        if kw in name_upper:
            return False
    for kw in WIRELESS_KEYWORDS:
        if kw in name_upper:
            return True
    return False


# ── Auth & CSRF ─────────────────────────────────────────────────────────────
security = HTTPBasic(auto_error=False)

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Authentication required")
    import secrets
    if not secrets.compare_digest(credentials.password, APP_KEY):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return credentials.username


def verify_csrf(
    request: Request,
    username: str = Depends(verify_credentials),
    x_csrf_token: Optional[str] = None,
):
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return username
    token = x_csrf_token or request.headers.get("X-CSRF-Token")
    if not token:
        token = request.cookies.get("csrf_token")
    if not token:
        raise HTTPException(status_code=403, detail="CSRF token required")
    db = get_db()
    row = db.execute("SELECT token FROM csrf_tokens WHERE token = ?", (token,)).fetchone()
    if not row:
        raise HTTPException(status_code=403, detail="Invalid CSRF token")
    return username


def generate_csrf_token() -> str:
    import secrets
    token = secrets.token_urlsafe(32)
    db = get_db()
    with _db_lock:
        db.execute("INSERT OR REPLACE INTO csrf_tokens (token) VALUES (?)", (token,))
        db.execute(
            "DELETE FROM csrf_tokens WHERE token NOT IN "
            "(SELECT token FROM csrf_tokens ORDER BY created_at DESC LIMIT 100)"
        )
        db.commit()
    return token


# ── GOWA Client ─────────────────────────────────────────────────────────────
class GowaClient:
    def __init__(self):
        parts = GOWA_CREDS.split(":", 1)
        self._user = parts[0] if len(parts) > 0 else ""
        self._pw = parts[1] if len(parts) > 1 else ""
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=GOWA_URL,
                auth=httpx.BasicAuth(self._user, self._pw),
                timeout=httpx.Timeout(20.0),
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=20),
            )
        return self._client

    async def get_devices(self) -> list:
        client = await self._get_client()
        try:
            resp = await client.get("/devices")
            if resp.status_code == 404:
                resp = await client.get("/app/devices")
            resp.raise_for_status()
            res = resp.json().get("results")
            if isinstance(res, list):
                return res
            if isinstance(res, dict):
                return res.get("devices", [])
            return []
        except Exception as e:
            log.warning(f"Failed to list GOWA devices: {e}")
            return []

    async def ensure_device(self, device_id: str) -> dict:
        """Ensure device_id is registered in GOWA v9 DeviceManager via POST /devices."""
        client = await self._get_client()
        try:
            devices = await self.get_devices()
            for d in devices:
                if isinstance(d, dict) and (d.get("id") == device_id or d.get("device_id") == device_id):
                    return d
            resp = await client.post("/devices", json={"device_id": device_id})
            if resp.status_code in (200, 201):
                return resp.json().get("results") or {"id": device_id}
            log.warning(f"GOWA POST /devices for {device_id} returned {resp.status_code}: {resp.text}")
        except Exception as e:
            log.warning(f"Failed to ensure GOWA device {device_id}: {e}")
        return {"id": device_id}

    async def delete_device(self, device_id: str) -> None:
        client = await self._get_client()
        try:
            await client.delete(f"/devices/{device_id}")
        except Exception as e:
            log.warning(f"Failed to delete GOWA device {device_id}: {e}")

    async def check_user(self, phone: str, device_id: str) -> dict:
        await self.ensure_device(device_id)
        client = await self._get_client()
        headers = {"X-Device-Id": device_id}
        try:
            resp = await client.get("/user/check", params={"phone": phone}, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", {})
            if isinstance(results, list) and results:
                is_wa = bool(results[0].get("is_on_whatsapp", False))
            elif isinstance(results, dict):
                is_wa = bool(results.get("is_on_whatsapp", False))
            else:
                is_wa = False
            return {
                "is_on_whatsapp": is_wa,
                "error": None,
                "status_code": resp.status_code,
            }
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status in (429, 463, 401, 403):
                return {"is_on_whatsapp": False, "error": f"HTTP_{status}", "status_code": status}
            raise
        except Exception as e:
            return {"is_on_whatsapp": False, "error": str(e), "status_code": 0}

    async def send_message(self, phone: str, message: str, device_id: str) -> dict:
        await self.ensure_device(device_id)
        client = await self._get_client()
        headers = {"X-Device-Id": device_id, "Content-Type": "application/json"}
        try:
            resp = await client.post(
                "/send/message",
                json={"phone": phone, "message": message},
                headers=headers,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"Chatter send failed: {e}")
            return {"error": str(e)}

    async def login_qr(self, device_id: str) -> dict:
        """Start QR pairing for device_id and inline the generated QR PNG as base64 data URI."""
        await self.ensure_device(device_id)
        client = await self._get_client()
        resp = await client.get(f"/devices/{device_id}/login")
        if resp.status_code == 404:
            resp = await client.get("/app/login", headers={"X-Device-Id": device_id})
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results") if isinstance(data.get("results"), dict) else {}
        qr_link = results.get("qr_link") or data.get("qr_link")
        if qr_link and not results.get("qr"):
            try:
                parsed = urlparse(qr_link)
                qr_path = parsed.path or qr_link
                img_resp = await client.get(qr_path)
                if img_resp.status_code == 200 and img_resp.content:
                    b64 = base64.b64encode(img_resp.content).decode("ascii")
                    data_uri = f"data:image/png;base64,{b64}"
                    results["qr"] = data_uri
                    data["results"] = results
                    data["qr"] = data_uri
            except Exception as e:
                log.warning(f"Failed to fetch QR image from {qr_link}: {e}")
        return data

    async def login_code(self, phone: str, device_id: str) -> dict:
        """Start pairing-code login for device_id and normalize pair_code / code fields."""
        await self.ensure_device(device_id)
        clean_phone = re.sub(r"\D", "", phone or "")
        client = await self._get_client()
        resp = await client.post(
            f"/devices/{device_id}/login/code",
            params={"phone": clean_phone},
        )
        if resp.status_code in (404, 405):
            resp = await client.get(
                "/app/login-with-code",
                params={"phone": clean_phone},
                headers={"X-Device-Id": device_id},
            )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results") if isinstance(data.get("results"), dict) else {}
        code = (
            results.get("pair_code")
            or results.get("code")
            or data.get("pair_code")
            or data.get("code")
        )
        if code:
            results["pair_code"] = code
            results["code"] = code
            data["results"] = results
            data["code"] = code
        return data

    async def logout_device(self, device_id: str) -> dict:
        """Logout device_id on GOWA while keeping the device slot."""
        await self.ensure_device(device_id)
        client = await self._get_client()
        resp = await client.post(f"/devices/{device_id}/logout")
        if resp.status_code in (404, 405):
            resp = await client.get("/app/logout", headers={"X-Device-Id": device_id})
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

gowa = GowaClient()

# ── NANPA Streaming Parser ─────────────────────────────────────────────────

def _stream_zip_to_temp(url: str) -> str:
    import urllib.request
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False, dir=DATA_DIR) as tmp:
        tmp_path = tmp.name
        with urllib.request.urlopen(req, timeout=180) as resp:
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                tmp.write(chunk)
    return tmp_path


def parse_co_codes_stream(npa_target: str, db: sqlite3.Connection):
    stats = {"total_lines": 0, "assigned": 0, "wireless": 0}
    log.info(f"Downloading CO codes from {CO_CODES_URL}")
    tmp_path = _stream_zip_to_temp(CO_CODES_URL)
    try:
        with zipfile.ZipFile(tmp_path, "r") as zf:
            txt_names = [n for n in zf.namelist() if n.endswith(".txt")]
            if not txt_names:
                log.error("No .txt file found in CO codes ZIP")
                return stats
            for txt_name in txt_names:
                with zf.open(txt_name) as f:
                    wrapper = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                    header_seen = False
                    for line in wrapper:
                        stats["total_lines"] += 1
                        line_stripped = line.strip()
                        if line_stripped.startswith("File Updated"):
                            continue
                        if not line_stripped:
                            continue
                        parts = [p.strip() for p in line.split("\t")]
                        if not header_seen:
                            if parts and parts[0] == "State":
                                header_seen = True
                            continue
                        if len(parts) < 7:
                            continue
                        npa_nxx = parts[1]
                        ocn = parts[2]
                        company = parts[3].strip('"')
                        use = parts[6]
                        if use != "AS":
                            continue
                        if "-" not in npa_nxx:
                            continue
                        npa, nxx = npa_nxx.split("-", 1)
                        if npa != npa_target:
                            continue
                        stats["assigned"] += 1
                        if is_wireless_ocn(ocn, company, db):
                            stats["wireless"] += 1
                            yield {
                                "npa": npa, "nxx": nxx,
                                "ocn": ocn, "company_name": company,
                                "is_wireless": True, "source": "co_codes",
                            }
    finally:
        os.unlink(tmp_path)
    return stats


def parse_thousands_stream(npa_target: str, db: sqlite3.Connection):
    stats = {"total_lines": 0, "assigned_blocks": 0, "wireless_blocks": 0}
    log.info(f"Downloading thousands-block from {THOUSANDS_URL}")
    tmp_path = _stream_zip_to_temp(THOUSANDS_URL)
    try:
        with zipfile.ZipFile(tmp_path, "r") as zf:
            txt_names = [n for n in zf.namelist() if n.lower().endswith(".txt")]
            if not txt_names:
                log.error("No .txt file found in thousands-block ZIP")
                return stats
            for txt_name in txt_names:
                with zf.open(txt_name) as f:
                    wrapper = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                    reader = csv.reader(wrapper)
                    next(reader, None)
                    for row in reader:
                        stats["total_lines"] += 1
                        if len(row) <= 11:
                            continue
                        npa = row[0].strip()
                        if npa != npa_target:
                            continue
                        status = row[3].strip().upper()
                        if status not in ("AS", "RT"):
                            continue
                        nxx = row[1].strip()
                        block = row[2].strip()
                        ocn = row[11].strip()
                        company = row[10].strip().strip('"')
                        stats["assigned_blocks"] += 1
                        if is_wireless_ocn(ocn, company, db):
                            stats["wireless_blocks"] += 1
                            yield {
                                "npa": npa, "nxx": nxx, "block": block,
                                "ocn": ocn, "company_name": company,
                                "is_wireless": True, "source": "thousands_block",
                            }
    finally:
        os.unlink(tmp_path)
    return stats


def ingest_nanpa_data_sync(npa_target: str, db: sqlite3.Connection = None) -> dict:
    """Ingest NANPA data using a dedicated SQLite connection (thread-safe)."""
    conn = _open_db()
    try:
        wireless_blocks = {}
        wireless_nxx_from_co = set()

        tb_stats = {"total_lines": 0, "assigned_blocks": 0, "wireless_blocks": 0}
        for entry in parse_thousands_stream(npa_target, conn):
            tb_stats["total_lines"] += 1
            tb_stats["assigned_blocks"] += 1
            tb_stats["wireless_blocks"] += 1
            key = (entry["nxx"], entry["block"])
            if key not in wireless_blocks:
                wireless_blocks[key] = entry
        log.info(f"Thousands-block: {tb_stats}")

        co_stats = {"total_lines": 0, "assigned": 0, "wireless": 0}
        for entry in parse_co_codes_stream(npa_target, conn):
            co_stats["total_lines"] += 1
            co_stats["assigned"] += 1
            co_stats["wireless"] += 1
            wireless_nxx_from_co.add(entry["nxx"])
        log.info(f"CO codes: {co_stats}")

        nxx_with_blocks = {b[0] for b in wireless_blocks}
        inserted = 0
        skipped_existing = 0

        for (nxx, block), entry in wireless_blocks.items():
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO wireless_blocks
                       (npa, nxx, block, ocn, company_name, source, status)
                       VALUES (?, ?, ?, ?, ?, ?, 'PENDING')""",
                    (npa_target, nxx, block, entry["ocn"], entry["company_name"], entry["source"]),
                )
                if conn.total_changes > 0:
                    inserted += 1
                else:
                    skipped_existing += 1
            except Exception:
                pass

        for nxx in sorted(wireless_nxx_from_co):
            if nxx in nxx_with_blocks:
                continue
            for block in range(10):
                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO wireless_blocks
                           (npa, nxx, block, source, status)
                           VALUES (?, ?, ?, 'co_codes_backfill', 'PENDING')""",
                        (npa_target, nxx, str(block)),
                    )
                    if conn.total_changes > 0:
                        inserted += 1
                except Exception:
                    pass

        conn.commit()

        try:
            rows = [
                dict(r) for r in conn.execute(
                    "SELECT npa, nxx, block, ocn, company_name, source, status "
                    "FROM wireless_blocks WHERE npa = ? ORDER BY nxx, block",
                    (npa_target,),
                ).fetchall()
            ]
            seed_cache = Path(DATA_DIR) / f"seed_{npa_target}.json"
            seed_cache.write_text(
                json.dumps({"npa": npa_target, "total": len(rows), "blocks": rows}),
                encoding="utf-8",
            )
        except Exception as e:
            log.warning(f"Could not write seed cache: {e}")

        log.info(f"Ingested {inserted} new wireless blocks for NPA {npa_target} "
                 f"({skipped_existing} already existed)")
        return {
            "npa": npa_target,
            "wireless_blocks_inserted": inserted,
            "skipped_existing": skipped_existing,
            "thousands_wireless_blocks": tb_stats["wireless_blocks"],
            "co_codes_wireless_nxx": len(wireless_nxx_from_co),
        }
    finally:
        conn.close()


# ── Block Sampler ───────────────────────────────────────────────────────────

class BlockSampler:
    def __init__(self):
        self.running = False
        self._task: Optional[asyncio.Task] = None
        self.stats = {
            "blocks_skipped": 0, "blocks_sweeping": 0, "blocks_complete": 0,
            "numbers_checked": 0, "numbers_found": 0, "queries_saved": 0,
            "current_device": None, "last_error": None,
        }

    async def start(self):
        if self.running:
            return
        self.running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def _is_night_hours(self) -> bool:
        now_utc = datetime.now(timezone.utc)
        npa = TARGET_NPA or "732"
        offset = -5
        if npa[0] in "89":
            offset = -8
        elif npa[0] in "67":
            offset = -6
        elif npa[0] == "5":
            offset = -6
        local_hour = (now_utc.hour + offset) % 24
        return NIGHT_START_HOUR <= local_hour or local_hour < NIGHT_END_HOUR

    def _gaussian_delay(self) -> float:
        return max(CHECK_DELAY_MIN, random.gauss(CHECK_DELAY_MEAN, CHECK_DELAY_STD))

    def _get_available_device(self, db: sqlite3.Connection) -> Optional[dict]:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        devices = db.execute(
            """SELECT * FROM gowa_devices
               WHERE status != 'COOLDOWN_463'
                  OR (status = 'COOLDOWN_463' AND cooldown_until < datetime('now'))
               ORDER BY CASE status
                   WHEN 'MATURE' THEN 0 WHEN 'WARMING' THEN 1
                   WHEN 'NEW' THEN 2 ELSE 3 END,
                 daily_checks ASC LIMIT 1"""
        ).fetchall()
        for dev in devices:
            dev = dict(dev)
            cap = CAPS.get(dev["status"], 150)
            daily = dev["daily_checks"] if dev["daily_check_date"] == today else 0
            if daily < cap:
                return dev
        return None

    async def _chatter(self, device_id: str):
        db = get_db()
        devices = db.execute(
            "SELECT device_id, phone_number FROM gowa_devices "
            "WHERE device_id != ? AND phone_number IS NOT NULL "
            "AND phone_number != '' AND status != 'COOLDOWN_463' LIMIT 3",
            (device_id,),
        ).fetchall()
        if len(devices) < 1:
            return
        target_dev = random.choice(devices)
        messages = [
            "Hey, how's it going?",
            "Just checking in, all good here",
            "System check complete, everything normal",
            "Quick ping for connectivity test",
            "Morning! Hope you're doing well",
        ]
        msg = random.choice(messages)
        await gowa.send_message(target_dev["phone_number"], msg, device_id)

    async def _run_burst(self, device: dict, db: sqlite3.Connection):
        burst_size = random.randint(BURST_MIN_CHECKS, BURST_MAX_CHECKS)
        real_work_done = False
        chatter_count = 0

        for _ in range(burst_size):
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            dev = db.execute(
                "SELECT daily_checks, daily_check_date FROM gowa_devices WHERE device_id = ?",
                (device["device_id"],),
            ).fetchone()
            if not dev:
                return real_work_done
            daily = dev["daily_checks"] if dev["daily_check_date"] == today else 0
            cap = CAPS.get(device["status"], 150)
            if daily >= cap:
                log.info(f"Device {device['device_id']} at daily cap ({daily}/{cap})")
                return real_work_done

            block = db.execute(
                "SELECT * FROM wireless_blocks WHERE status = 'PENDING' ORDER BY id ASC LIMIT 1"
            ).fetchone()
            if not block:
                block = db.execute(
                    "SELECT * FROM wireless_blocks WHERE status = 'ACTIVE_SWEEPING' "
                    "ORDER BY sample_hits DESC, id ASC LIMIT 1"
                ).fetchone()
            if not block:
                return real_work_done

            block = dict(block)
            made_call = False
            if block["status"] == "PENDING":
                made_call = await self._probe_one_sample(block, device, db)
            elif block["status"] == "ACTIVE_SWEEPING":
                made_call = await self._probe_one_sweep(block, device, db)

            if made_call:
                real_work_done = True
                self._update_daily_counter(device["device_id"], db)
                self.stats["numbers_checked"] += 1

            chatter_count += 1
            if chatter_count >= random.randint(CHATTER_INTERVAL_MIN, CHATTER_INTERVAL_MAX):
                chatter_count = 0
                await self._chatter(device["device_id"])

            if made_call:
                await asyncio.sleep(self._gaussian_delay())

        return real_work_done

    async def _probe_one_sample(self, block: dict, device: dict, db: sqlite3.Connection) -> bool:
        npa, nxx, blk = block["npa"], block["nxx"], block["block"]
        existing = db.execute(
            "SELECT COUNT(*) as c FROM number_probes WHERE npa=? AND nxx=? AND block=?",
            (npa, nxx, blk),
        ).fetchone()["c"]

        if existing >= SAMPLE_SIZE:
            self._finalize_sample_block(npa, nxx, blk, db)
            return False

        probed = {r["phone"] for r in db.execute(
            "SELECT phone FROM number_probes WHERE npa=? AND nxx=? AND block=?",
            (npa, nxx, blk),
        ).fetchall()}

        remaining = [
            f"1{npa}{nxx}{blk}{i:04d}" for i in range(1000)
            if f"1{npa}{nxx}{blk}{i:04d}" not in probed
        ]
        if not remaining:
            self._finalize_sample_block(npa, nxx, blk, db)
            return False

        phone = random.choice(remaining)
        result = await gowa.check_user(phone, device["device_id"])

        if result.get("error"):
            error_code = result["error"]
            if error_code in ("HTTP_429", "HTTP_463", "HTTP_401", "HTTP_403"):
                self._trigger_cooldown(device["device_id"], db)
                self.stats["last_error"] = error_code
                return True

        is_wa = result.get("is_on_whatsapp", False)
        if is_wa:
            self.stats["numbers_found"] += 1

        with _db_lock:
            db.execute(
                "INSERT OR REPLACE INTO number_probes "
                "(phone, npa, nxx, block, is_on_whatsapp, device_id) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (phone, npa, nxx, blk, int(is_wa), device["device_id"]),
            )
            db.commit()

        if existing + 1 >= SAMPLE_SIZE:
            self._finalize_sample_block(npa, nxx, blk, db)

        return True

    def _finalize_sample_block(self, npa: str, nxx: str, blk: str, db: sqlite3.Connection):
        hits = db.execute(
            "SELECT COUNT(*) as c FROM number_probes "
            "WHERE npa=? AND nxx=? AND block=? AND is_on_whatsapp=1",
            (npa, nxx, blk),
        ).fetchone()["c"]
        total = db.execute(
            "SELECT COUNT(*) as c FROM number_probes WHERE npa=? AND nxx=? AND block=?",
            (npa, nxx, blk),
        ).fetchone()["c"]

        with _db_lock:
            db.execute(
                """UPDATE wireless_blocks SET sample_hits=?, sample_total=?,
                   numbers_checked=?, numbers_found=?, updated_at=datetime('now')
                   WHERE npa=? AND nxx=? AND block=?""",
                (hits, SAMPLE_SIZE, total, hits, npa, nxx, blk),
            )

            if hits == 0:
                db.execute(
                    "UPDATE wireless_blocks SET status='DORMANT_SKIPPED', "
                    "updated_at=datetime('now') WHERE npa=? AND nxx=? AND block=?",
                    (npa, nxx, blk),
                )
                self.stats["blocks_skipped"] += 1
                self.stats["queries_saved"] += 980
            else:
                db.execute(
                    "UPDATE wireless_blocks SET status='ACTIVE_SWEEPING', "
                    "updated_at=datetime('now') WHERE npa=? AND nxx=? AND block=?",
                    (npa, nxx, blk),
                )
                self.stats["blocks_sweeping"] += 1
            db.commit()

    async def _probe_one_sweep(self, block: dict, device: dict, db: sqlite3.Connection) -> bool:
        npa, nxx, blk = block["npa"], block["nxx"], block["block"]
        probed = {r["phone"] for r in db.execute(
            "SELECT phone FROM number_probes WHERE npa=? AND nxx=? AND block=?",
            (npa, nxx, blk),
        ).fetchall()}

        remaining = [
            f"1{npa}{nxx}{blk}{i:04d}" for i in range(1000)
            if f"1{npa}{nxx}{blk}{i:04d}" not in probed
        ]

        if not remaining:
            hits = db.execute(
                "SELECT COUNT(*) as c FROM number_probes "
                "WHERE npa=? AND nxx=? AND block=? AND is_on_whatsapp=1",
                (npa, nxx, blk),
            ).fetchone()["c"]
            total = len(probed)
            with _db_lock:
                db.execute(
                    """UPDATE wireless_blocks SET status='COMPLETE',
                       numbers_checked=?, numbers_found=?, updated_at=datetime('now')
                       WHERE npa=? AND nxx=? AND block=?""",
                    (total, hits, npa, nxx, blk),
                )
                db.commit()
            self.stats["blocks_complete"] += 1
            return False

        phone = random.choice(remaining)
        result = await gowa.check_user(phone, device["device_id"])

        if result.get("error"):
            error_code = result["error"]
            if error_code in ("HTTP_429", "HTTP_463", "HTTP_401", "HTTP_403"):
                self._trigger_cooldown(device["device_id"], db)
                self.stats["last_error"] = error_code
                return True

        is_wa = result.get("is_on_whatsapp", False)
        if is_wa:
            self.stats["numbers_found"] += 1

        with _db_lock:
            db.execute(
                "INSERT OR REPLACE INTO number_probes "
                "(phone, npa, nxx, block, is_on_whatsapp, device_id) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (phone, npa, nxx, blk, int(is_wa), device["device_id"]),
            )
            db.commit()
        return True

    def _trigger_cooldown(self, device_id: str, db: sqlite3.Connection):
        until = (datetime.now(timezone.utc) + timedelta(hours=COOLDOWN_HOURS)).isoformat()
        with _db_lock:
            db.execute(
                "UPDATE gowa_devices SET status='COOLDOWN_463', cooldown_until=? WHERE device_id=?",
                (until, device_id),
            )
            db.commit()
        log.warning(f"Device {device_id} tripped COOLDOWN_463 until {until}")

    def _update_daily_counter(self, device_id: str, db: sqlite3.Connection):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dev = db.execute(
            "SELECT daily_check_date, daily_checks FROM gowa_devices WHERE device_id=?",
            (device_id,),
        ).fetchone()
        if not dev:
            return
        with _db_lock:
            if dev["daily_check_date"] == today:
                db.execute(
                    "UPDATE gowa_devices SET daily_checks=daily_checks+1, "
                    "last_check_at=datetime('now') WHERE device_id=?",
                    (device_id,),
                )
            else:
                db.execute(
                    "UPDATE gowa_devices SET daily_checks=1, daily_check_date=?, "
                    "last_check_at=datetime('now') WHERE device_id=?",
                    (today, device_id),
                )
            db.commit()

    def _update_device_age(self, db: sqlite3.Connection):
        devices = db.execute(
            "SELECT * FROM gowa_devices WHERE status IN ('NEW', 'WARMING')"
        ).fetchall()
        for dev in devices:
            dev = dict(dev)
            created = datetime.fromisoformat(dev["created_at"])
            age_days = (datetime.now(timezone.utc) - created).days
            with _db_lock:
                db.execute(
                    "UPDATE gowa_devices SET age_days=? WHERE device_id=?",
                    (age_days, dev["device_id"]),
                )
                if age_days >= 8 and dev["status"] != "MATURE":
                    db.execute(
                        "UPDATE gowa_devices SET status='MATURE' WHERE device_id=?",
                        (dev["device_id"],),
                    )
                    log.info(f"Device {dev['device_id']} promoted to MATURE (age {age_days}d)")
                elif age_days >= 4 and dev["status"] == "NEW":
                    db.execute(
                        "UPDATE gowa_devices SET status='WARMING' WHERE device_id=?",
                        (dev["device_id"],),
                    )
                    log.info(f"Device {dev['device_id']} promoted to WARMING (age {age_days}d)")
                db.commit()

    async def _run(self):
        log.info("Block Sampler worker started")
        while self.running:
            try:
                if self._is_night_hours():
                    await asyncio.sleep(600)
                    continue
                db = get_db()
                self._update_device_age(db)
                device = self._get_available_device(db)
                if not device:
                    await asyncio.sleep(60)
                    continue
                self.stats["current_device"] = device["device_id"]
                did_work = await self._run_burst(device, db)
                if did_work:
                    cooldown = random.randint(BURST_COOLDOWN_MIN, BURST_COOLDOWN_MAX)
                    log.info(f"Burst complete. Cooldown for {cooldown // 60} min.")
                    await asyncio.sleep(cooldown)
                else:
                    await asyncio.sleep(120)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Sampler error: {e}", exc_info=True)
                await asyncio.sleep(30)
        log.info("Block Sampler worker stopped")


sampler = BlockSampler()

# ── Keep-Alive & Auto-Seed Background Tasks ─────────────────────────────────

async def _keep_alive_loop():
    """Ping external /health every 8 minutes so Render free tier stays warm."""
    url = f"{KEEP_ALIVE_URL.rstrip('/')}/health"
    log.info(f"Keep-alive task started: pinging {url} every {KEEP_ALIVE_INTERVAL}s")
    async with httpx.AsyncClient(timeout=15.0) as client:
        while True:
            try:
                await asyncio.sleep(KEEP_ALIVE_INTERVAL)
                resp = await client.get(url)
                log.info(f"Keep-alive ping {url} -> HTTP {resp.status_code}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"Keep-alive ping failed: {e}")


async def _startup_auto_seed():
    """Ensure TARGET_NPA (default 732) blocks and probes are present on startup and sync local devices to GOWA."""
    try:
        npa = TARGET_NPA or "732"
        seeded = seed_from_json_if_empty(npa)
        if seeded == 0:
            log.info(f"No blocks found for NPA {npa} on startup — auto-ingesting from NANPA...")
            await asyncio.to_thread(ingest_nanpa_data_sync, npa)
        # v3.4.0: seed Phase A probe results from backups/npa_732_probes_seed.json
        probes_seeded = seed_probes_if_empty(npa)
        log.info(f"Startup auto-seed: {probes_seeded} probes seeded for NPA {npa}")
        db = get_db()
        rows = db.execute("SELECT device_id FROM gowa_devices").fetchall()
        for r in rows:
            await gowa.ensure_device(r["device_id"])
    except Exception as e:
        log.error(f"Startup auto-seed failed: {e}", exc_info=True)


# ── HTML template (served directly, no Jinja2) ──────────────────────────────
_HTML_TEMPLATE = None

def _load_html_template() -> str:
    global _HTML_TEMPLATE
    if _HTML_TEMPLATE is not None:
        return _HTML_TEMPLATE
    html_path = Path(__file__).parent / "templates" / "index.html"
    if html_path.exists():
        _HTML_TEMPLATE = html_path.read_text(encoding="utf-8")
    else:
        _HTML_TEMPLATE = "<html><body><h1>WhatsApp Number Miner</h1><p>Template not found</p></body></html>"
    return _HTML_TEMPLATE

# ── FastAPI App ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    seed_task = asyncio.create_task(_startup_auto_seed())
    ka_task = asyncio.create_task(_keep_alive_loop())
    yield
    ka_task.cancel()
    seed_task.cancel()
    await sampler.stop()
    await gowa.close()

app = FastAPI(title="WhatsApp Number Miner", version="3.4.0", lifespan=lifespan)

# ── Routes ──────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    try:
        db = get_db()
        count = db.execute("SELECT COUNT(*) as c FROM wireless_blocks").fetchone()["c"]
        return {
            "status": "ok",
            "version": "3.4.0",
            "blocks_loaded": count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=503)

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    csrf = generate_csrf_token()
    html = _load_html_template()
    response = HTMLResponse(content=html)
    response.set_cookie(key="csrf_token", value=csrf, httponly=True, samesite="lax")
    return response

@app.get("/api/csrf-token")
async def get_csrf_token(username: str = Depends(verify_credentials)):
    return {"csrf_token": generate_csrf_token()}

@app.get("/api/backup/status")
async def backup_status(npa: str = Query("732")):
    """Snapshot status metadata for public NANPA wireless_blocks."""
    db = get_db()
    total = db.execute(
        "SELECT COUNT(*) as c FROM wireless_blocks WHERE npa = ?", (npa,)
    ).fetchone()["c"]
    by_source = {
        r["source"]: r["c"]
        for r in db.execute(
            "SELECT source, COUNT(*) as c FROM wireless_blocks WHERE npa = ? GROUP BY source",
            (npa,),
        ).fetchall()
    }
    return {
        "status": "ok",
        "version": "3.4.0",
        "npa": npa,
        "total_wireless_blocks": total,
        "by_source": by_source,
        "keep_alive_interval_s": KEEP_ALIVE_INTERVAL,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

@app.get("/api/backup/blocks")
async def backup_blocks(
    npa: str = Query("732"),
    format: str = Query("json", pattern="^(json|csv)$"),
):
    """Export public NANPA wireless_blocks table as JSON or CSV for Files/backups/."""
    db = get_db()
    rows = [
        dict(r) for r in db.execute(
            "SELECT npa, nxx, block, ocn, company_name, source, status, "
            "sample_hits, sample_total, numbers_checked, numbers_found "
            "FROM wireless_blocks WHERE npa = ? ORDER BY nxx ASC, block ASC",
            (npa,),
        ).fetchall()
    ]
    if format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "npa", "nxx", "block", "ocn", "company_name", "source",
            "status", "sample_hits", "sample_total", "numbers_checked", "numbers_found"
        ])
        for r in rows:
            writer.writerow([
                r["npa"], r["nxx"], r["block"], r["ocn"] or "",
                r["company_name"] or "", r["source"] or "", r["status"] or "",
                r["sample_hits"], r["sample_total"], r["numbers_checked"], r["numbers_found"],
            ])
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=npa_{npa}_wireless_blocks.csv"},
        )
    return {
        "npa": npa,
        "version": "3.4.0",
        "total": len(rows),
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "blocks": rows,
    }

@app.get("/api/backup/probes")
async def backup_probes(
    npa: str = Query("732"),
    format: str = Query("json", pattern="^(json|csv)$"),
):
    """v3.4.0: Export number_probes for a given NPA as JSON or CSV (no auth, matching /api/backup/status)."""
    db = get_db()
    rows = [
        dict(r) for r in db.execute(
            "SELECT phone, npa, nxx, block, is_on_whatsapp, device_id, probed_at, error "
            "FROM number_probes WHERE npa = ? ORDER BY probed_at DESC",
            (npa,),
        ).fetchall()
    ]
    total_wa = sum(1 for r in rows if r["is_on_whatsapp"])
    total_probes = len(rows)

    if format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["phone", "npa", "nxx", "block", "is_on_whatsapp", "device_id", "probed_at", "error"])
        for r in rows:
            writer.writerow([
                r["phone"], r["npa"], r["nxx"], r["block"],
                r["is_on_whatsapp"], r["device_id"] or "", r["probed_at"] or "", r["error"] or "",
            ])
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=npa_{npa}_probes.csv"},
        )
    return {
        "npa": npa,
        "version": "3.4.0",
        "total_probes": total_probes,
        "whatsapp_found": total_wa,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "probes": rows,
    }

@app.get("/api/stats")
async def get_stats(username: str = Depends(verify_credentials)):
    db = get_db()
    total_blocks = db.execute("SELECT COUNT(*) as c FROM wireless_blocks").fetchone()["c"]
    pending = db.execute(
        "SELECT COUNT(*) as c FROM wireless_blocks WHERE status='PENDING'"
    ).fetchone()["c"]
    dormant = db.execute(
        "SELECT COUNT(*) as c FROM wireless_blocks WHERE status='DORMANT_SKIPPED'"
    ).fetchone()["c"]
    sweeping = db.execute(
        "SELECT COUNT(*) as c FROM wireless_blocks WHERE status='ACTIVE_SWEEPING'"
    ).fetchone()["c"]
    complete = db.execute(
        "SELECT COUNT(*) as c FROM wireless_blocks WHERE status='COMPLETE'"
    ).fetchone()["c"]
    wa_found = db.execute(
        "SELECT COUNT(*) as c FROM number_probes WHERE is_on_whatsapp=1"
    ).fetchone()["c"]
    total_probes = db.execute("SELECT COUNT(*) as c FROM number_probes").fetchone()["c"]
    devices = db.execute(
        "SELECT * FROM gowa_devices ORDER BY created_at DESC"
    ).fetchall()
    return {
        **sampler.stats,
        "sampler_running": sampler.running,
        "db_blocks_total": total_blocks, "db_blocks_pending": pending,
        "db_blocks_dormant": dormant, "db_blocks_sweeping": sweeping,
        "db_blocks_complete": complete, "db_whatsapp_found": wa_found,
        "db_total_probes": total_probes,
        "devices": [dict(d) for d in devices],
    }

@app.post("/api/ingest")
async def ingest_npa(
    npa: str = Query(..., description="Target area code, e.g. 212"),
    username: str = Depends(verify_csrf),
):
    if not re.match(r"^\d{3}$", npa):
        raise HTTPException(400, "NPA must be exactly 3 digits")
    try:
        result = await asyncio.to_thread(ingest_nanpa_data_sync, npa)
        return {"status": "ok", **result}
    except Exception as e:
        log.error(f"Ingestion failed: {e}", exc_info=True)
        raise HTTPException(500, str(e))

@app.get("/api/blocks")
async def list_blocks(
    npa: Optional[str] = None, status: Optional[str] = None,
    limit: int = Query(100, le=5000), offset: int = 0,
    username: str = Depends(verify_credentials),
):
    db = get_db()
    query = "SELECT * FROM wireless_blocks WHERE 1=1"
    params = []
    if npa:
        query += " AND npa = ?"; params.append(npa)
    if status:
        query += " AND status = ?"; params.append(status)
    query += " ORDER BY id ASC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    rows = db.execute(query, params).fetchall()
    total = db.execute(
        "SELECT COUNT(*) as c FROM wireless_blocks" + (" WHERE npa = ?" if npa else ""),
        (npa,) if npa else (),
    ).fetchone()["c"]
    return {"total": total, "limit": limit, "offset": offset, "blocks": [dict(r) for r in rows]}

@app.get("/api/numbers")
async def list_numbers(
    is_on_whatsapp: Optional[int] = None, npa: Optional[str] = None,
    limit: int = Query(1000, le=50000), offset: int = 0,
    username: str = Depends(verify_credentials),
):
    db = get_db()
    query = "SELECT * FROM number_probes WHERE 1=1"
    params = []
    if is_on_whatsapp is not None:
        query += " AND is_on_whatsapp = ?"; params.append(is_on_whatsapp)
    if npa:
        query += " AND npa = ?"; params.append(npa)
    query += " ORDER BY id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    rows = db.execute(query, params).fetchall()
    return {"numbers": [dict(r) for r in rows], "limit": limit, "offset": offset}

@app.get("/api/export")
async def export_csv(npa: Optional[str] = None, username: str = Depends(verify_credentials)):
    db = get_db()
    query = (
        "SELECT phone, npa, nxx, block, probed_at FROM number_probes "
        "WHERE is_on_whatsapp = 1"
    )
    params = []
    if npa:
        query += " AND npa = ?"; params.append(npa)
    query += " ORDER BY phone ASC"
    rows = db.execute(query, params).fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["phone", "npa", "nxx", "block", "probed_at"])
    for r in rows:
        writer.writerow([r["phone"], r["npa"], r["nxx"], r["block"], r["probed_at"]])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=whatsapp_numbers_{npa or 'all'}.csv"
        },
    )

@app.get("/api/devices")
async def list_devices(username: str = Depends(verify_credentials)):
    db = get_db()
    local = [
        dict(d) for d in db.execute(
            "SELECT * FROM gowa_devices ORDER BY created_at DESC"
        ).fetchall()
    ]
    gowa_devices = await gowa.get_devices()
    return {"local": local, "gowa": gowa_devices}

@app.post("/api/devices")
async def add_device(
    device_id: str = Query(...),
    phone_number: Optional[str] = None,
    proxy_url: Optional[str] = None,
    username: str = Depends(verify_csrf),
):
    db = get_db()
    try:
        with _db_lock:
            existing = db.execute(
                "SELECT phone_number, proxy_url, status, age_days, daily_checks, daily_check_date "
                "FROM gowa_devices WHERE device_id = ?",
                (device_id,),
            ).fetchone()
            if existing:
                new_phone = phone_number if phone_number is not None else existing["phone_number"]
                new_proxy = proxy_url if proxy_url is not None else existing["proxy_url"]
                db.execute(
                    "UPDATE gowa_devices SET phone_number = ?, proxy_url = ? WHERE device_id = ?",
                    (new_phone, new_proxy, device_id),
                )
            else:
                db.execute(
                    "INSERT INTO gowa_devices (device_id, phone_number, proxy_url, status) "
                    "VALUES (?, ?, ?, 'NEW')",
                    (device_id, phone_number, proxy_url),
                )
            db.commit()
        gowa_info = await gowa.ensure_device(device_id)
        return {"status": "ok", "device_id": device_id, "gowa": gowa_info}
    except Exception as e:
        raise HTTPException(400, str(e))

@app.delete("/api/devices/{device_id}")
async def remove_device(device_id: str, username: str = Depends(verify_csrf)):
    db = get_db()
    with _db_lock:
        db.execute("DELETE FROM gowa_devices WHERE device_id = ?", (device_id,))
        db.commit()
    await gowa.delete_device(device_id)
    return {"status": "ok"}

# ── GOWA QR & Pair-Code Proxy Endpoints ─────────────────────────────────────

@app.post("/api/devices/{device_id}/login-qr")
async def device_login_qr(device_id: str, username: str = Depends(verify_csrf)):
    try:
        result = await gowa.login_qr(device_id)
        return result
    except Exception as e:
        log.error(f"GOWA login_qr failed for {device_id}: {e}")
        raise HTTPException(502, f"GOWA error: {e}")

@app.post("/api/devices/{device_id}/login-code")
async def device_login_code(
    device_id: str,
    phone: str = Query(..., description="Phone number for pair code"),
    username: str = Depends(verify_csrf),
):
    try:
        clean_phone = re.sub(r"\D", "", phone or "")
        if clean_phone:
            db = get_db()
            with _db_lock:
                db.execute(
                    "UPDATE gowa_devices SET phone_number = ? WHERE device_id = ?",
                    (clean_phone, device_id),
                )
                db.commit()
        result = await gowa.login_code(clean_phone, device_id)
        return result
    except Exception as e:
        log.error(f"GOWA login_code failed for {device_id}: {e}")
        raise HTTPException(502, f"GOWA error: {e}")

@app.post("/api/devices/{device_id}/logout")
async def device_logout(device_id: str, username: str = Depends(verify_csrf)):
    try:
        result = await gowa.logout_device(device_id)
        return result
    except Exception as e:
        log.error(f"GOWA logout failed for {device_id}: {e}")
        raise HTTPException(502, f"GOWA error: {e}")

@app.get("/api/ocn-overrides")
async def list_ocn_overrides(username: str = Depends(verify_credentials)):
    db = get_db()
    rows = db.execute("SELECT * FROM ocn_overrides ORDER BY ocn").fetchall()
    return {"overrides": [dict(r) for r in rows]}

@app.post("/api/ocn-overrides")
async def set_ocn_override(
    ocn: str = Query(...), company_name: Optional[str] = None,
    is_wireless: bool = True, username: str = Depends(verify_csrf),
):
    db = get_db()
    with _db_lock:
        db.execute(
            "INSERT OR REPLACE INTO ocn_overrides (ocn, company_name, is_wireless) "
            "VALUES (?, ?, ?)",
            (ocn, company_name, int(is_wireless)),
        )
        db.commit()
    return {"status": "ok", "ocn": ocn, "is_wireless": is_wireless}

@app.post("/api/sampler/start")
async def start_sampler(username: str = Depends(verify_csrf)):
    await sampler.start()
    return {"status": "started"}

@app.post("/api/sampler/stop")
async def stop_sampler(username: str = Depends(verify_csrf)):
    await sampler.stop()
    return {"status": "stopped"}

@app.get("/api/sampler/status")
async def sampler_status(username: str = Depends(verify_credentials)):
    return {"running": sampler.running, **sampler.stats}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
