#!/usr/bin/env python3
"""
e6AI personal archive
- Archives post metadata and tag categories to SQLite.
- Optionally downloads original files referenced by each post.
- Does not request comments, accounts, favorites, messages, votes, notes, or pools.

API behavior follows the supplied e6AI API documentation:
  GET /posts.json, limit <= 320
  first page without `page`; subsequent pages with page=b<lowest post ID>
  custom non-empty User-Agent; adaptive shared limiter starts at 0.8 seconds,
  and increases the interval only after HTTP throttling responses.

Examples:
  python e6ai_archive.py --max-pages 3
  python e6ai_archive.py --download
  python e6ai_archive.py --max-pages 3 --download
  python e6ai_archive.py --restart-scan
  python e6ai_archive.py --download-only
"""

from __future__ import annotations

import argparse
import concurrent.futures
import email.utils
import errno
import hashlib
import json
import logging
import os
import sqlite3
import shutil
import time
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_BASE = "https://e6ai.net"
POSTS_ENDPOINT = f"{API_BASE}/posts.json"
API_LIMIT = 320
DEFAULT_IMAGE_DIR = Path("/var/www/nextcloud/data/__groupfolders/1/files/Furry/Artwork/Unsortiert/TWS_AI_DB")
DEFAULT_DB = Path(__file__).resolve().parent / "archive" / "e6ai_archive.sqlite3"
# e6AI/e621-style APIs commonly call the creator category "artist";
# accept "director" as well for APIs that expose that label directly.
DIRECTOR_TAG_CATEGORIES = {"director", "artist"}
CHARACTER_TAG_CATEGORY = "character"
DEFAULT_USER_AGENT = "e6AI-Personal-Archive/1.0 (metadata and original-file archival)"
MIN_REQUEST_INTERVAL = 0.8
INITIAL_REQUEST_INTERVAL = 0.8
CONNECT_TIMEOUT = 15
READ_TIMEOUT = 60
CHUNK_SIZE = 1024 * 1024
LOG = logging.getLogger("e6ai_archive")


class ArchiveError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def configure_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


class AdaptiveRateLimiter:
    """Ein gemeinsamer, adaptiver Request-Takt für alle Download-Worker."""

    def __init__(self, min_interval: float = MIN_REQUEST_INTERVAL, initial_interval: float = INITIAL_REQUEST_INTERVAL) -> None:
        self.min_interval = min_interval
        self.interval = max(min_interval, initial_interval)
        self.lock = threading.Lock()
        self.next_request_at = 0.0
        self.cooldown_until = 0.0
        self.success_streak = 0
        self.throttle_count = 0

    def wait_turn(self) -> None:
        # Sleep under the lock so workers cannot all wake and burst together.
        with self.lock:
            now = time.monotonic()
            wait_until = max(self.next_request_at, self.cooldown_until)
            if wait_until > now:
                time.sleep(wait_until - now)
            now = time.monotonic()
            self.next_request_at = now + self.interval

    def success(self) -> None:
        with self.lock:
            self.success_streak += 1
            if self.success_streak >= 20:
                self.interval = max(self.min_interval, self.interval - 0.01)
                self.success_streak = 0
                self.throttle_count = 0

    def throttle(self, status: int, retry_after: float | None, attempt: int) -> float:
        """Erhöht den Request-Abstand ausschließlich bei HTTP-Drosselung."""
        with self.lock:
            self.success_streak = 0
            self.throttle_count += 1
            self.interval = min(10.0, max(self.interval * 1.5, self.min_interval))
            if retry_after is not None:
                pause = max(0.0, retry_after)
            elif status == 421:
                pause = min(300.0, 60.0 * (2 ** min(attempt, 2)))
            else:
                pause = min(180.0, 10.0 * (2 ** min(attempt, 4)))
            self.cooldown_until = max(self.cooldown_until, time.monotonic() + pause)
            return pause

    def transient_error(self, attempt: int) -> float:
        """Kurze gemeinsame Pause bei Netzwerkfehlern, ohne den Takt dauerhaft anzuheben."""
        pause = min(30.0, 2.0 * (2 ** min(attempt, 4)))
        with self.lock:
            self.success_streak = 0
            self.cooldown_until = max(self.cooldown_until, time.monotonic() + pause)
        return pause


class RateLimitedSession:
    """Thread-local HTTP sessions with one shared adaptive global rate limiter."""

    RETRYABLE_STATUS = {421, 429, 500, 502, 503, 504, 520, 522, 524, 525}

    def __init__(
        self,
        user_agent: str,
        limiter: AdaptiveRateLimiter | None = None,
        max_attempts: int = 5,
    ) -> None:
        if not user_agent.strip():
            raise ValueError("Der User-Agent darf nicht leer sein.")
        self.user_agent = user_agent
        self.limiter = limiter or AdaptiveRateLimiter()
        self.max_attempts = max_attempts
        self.local = threading.local()
        self.sessions: list[requests.Session] = []
        self.sessions_lock = threading.Lock()

    def _session(self) -> requests.Session:
        session = getattr(self.local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({
                "User-Agent": self.user_agent,
                "Accept": "application/json, */*;q=0.8",
            })
            # Retries are handled here so every retry passes through the shared limiter.
            adapter = HTTPAdapter(max_retries=Retry(total=0), pool_connections=2, pool_maxsize=2)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            self.local.session = session
            with self.sessions_lock:
                self.sessions.append(session)
        return session

    @staticmethod
    def _retry_after_seconds(value: str | None) -> float | None:
        if not value:
            return None
        try:
            return max(0.0, float(value.strip()))
        except (TypeError, ValueError):
            try:
                parsed = email.utils.parsedate_to_datetime(value)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                return None

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        stream = kwargs.pop("stream", False)
        last_error: Exception | None = None
        for attempt in range(self.max_attempts):
            self.limiter.wait_turn()
            try:
                response = self._session().get(
                    url,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                    stream=stream,
                    **kwargs,
                )
            except requests.RequestException as exc:
                last_error = exc
                pause = self.limiter.transient_error(attempt)
                LOG.warning(
                    "Netzwerkfehler bei %s (%s: %s), Versuch %s/%s; "
                    "gemeinsame Pause %.1f s, Request-Abstand bleibt %.2f s.",
                    url, type(exc).__name__, exc, attempt + 1, self.max_attempts,
                    pause, self.limiter.interval,
                )
                if attempt + 1 >= self.max_attempts:
                    break
                continue

            if response.status_code in self.RETRYABLE_STATUS and attempt + 1 < self.max_attempts:
                status = response.status_code
                retry_after = self._retry_after_seconds(response.headers.get("Retry-After"))
                response.close()
                if status in (421, 429, 503):
                    pause = self.limiter.throttle(status, retry_after, attempt)
                    LOG.warning(
                        "HTTP %s: echte Drosselung; Request-Abstand jetzt %.2f s; "
                        "gemeinsame Pause %.1f s (Versuch %s/%s).",
                        status, self.limiter.interval, pause, attempt + 1, self.max_attempts,
                    )
                else:
                    pause = self.limiter.transient_error(attempt)
                    LOG.warning(
                        "HTTP %s bei %s; temporärer Server-/Proxyfehler, "
                        "Request-Abstand bleibt %.2f s; Pause %.1f s (Versuch %s/%s).",
                        status, url, self.limiter.interval, pause, attempt + 1, self.max_attempts,
                    )
                continue

            if response.status_code >= 400:
                body = ""
                if not stream:
                    body = response.text[:1200].replace("\\n", " ")
                status = response.status_code
                response.close()
                raise ArchiveError(
                    f"HTTP {status} bei {url}; Antwort={body!r}"
                )
            self.limiter.success()
            return response

        raise ArchiveError(f"Verbindungsfehler bei {url} nach {self.max_attempts} Versuchen: {last_error}")

    def close(self) -> None:
        with self.sessions_lock:
            sessions, self.sessions = self.sessions, []
        for session in sessions:
            session.close()


def connect_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS posts (
        id              INTEGER PRIMARY KEY,
        created_at      TEXT,
        updated_at      TEXT,
        change_seq      INTEGER,
        rating          TEXT,
        description     TEXT,
        source          TEXT,
        parent_id       INTEGER,
        has_children    INTEGER,
        is_deleted      INTEGER,
        width           INTEGER,
        height          INTEGER,
        extension       TEXT,
        file_size       INTEGER,
        md5             TEXT,
        file_url        TEXT,
        local_path      TEXT,
        downloaded      INTEGER NOT NULL DEFAULT 0,
        verification_status TEXT NOT NULL DEFAULT 'pending',
        download_error  TEXT,
        scanned_at      TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS tags (
        name            TEXT PRIMARY KEY,
        category        TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS post_tags (
        post_id         INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
        tag_name        TEXT NOT NULL REFERENCES tags(name),
        category        TEXT NOT NULL DEFAULT '',
        PRIMARY KEY (post_id, tag_name, category)
    );

    CREATE TABLE IF NOT EXISTS scan_state (
        key             TEXT PRIMARY KEY,
        value           TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_posts_downloaded ON posts(downloaded);
    CREATE INDEX IF NOT EXISTS idx_post_tags_tag ON post_tags(tag_name);
    """)
    # Backwards-compatible migrations for databases created by earlier versions.
    post_columns = {row[1] for row in conn.execute("PRAGMA table_info(posts)")}
    if "verification_status" not in post_columns:
        conn.execute("ALTER TABLE posts ADD COLUMN verification_status TEXT NOT NULL DEFAULT 'pending'")
        # Earlier versions treated a missing MD5 as successful verification.
        # Recheck every old "downloaded" record against the new safe destination;
        # existing files are preserved and reused only after an actual MD5 match.
        conn.execute("""
            UPDATE posts
            SET downloaded=0, verification_status='pending',
                local_path=NULL, download_error=NULL
        """)

    tag_link_info = list(conn.execute("PRAGMA table_info(post_tags)"))
    tag_link_columns = {row[1] for row in tag_link_info}
    if "category" not in tag_link_columns:
        conn.execute("ALTER TABLE post_tags ADD COLUMN category TEXT NOT NULL DEFAULT ''")
        conn.execute("""
            UPDATE post_tags
            SET category = COALESCE((SELECT category FROM tags WHERE tags.name = post_tags.tag_name), '')
        """)

    # Rebuild legacy (post_id, tag_name) keys so identical tag names in
    # different categories can coexist for the same post.
    primary_key_columns = [row[1] for row in sorted(tag_link_info, key=lambda item: item[5]) if row[5]]
    if primary_key_columns != ["post_id", "tag_name", "category"]:
        conn.execute("DROP INDEX IF EXISTS idx_post_tags_tag")
        conn.execute("ALTER TABLE post_tags RENAME TO post_tags_legacy")
        conn.execute("""
            CREATE TABLE post_tags (
                post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
                tag_name TEXT NOT NULL REFERENCES tags(name),
                category TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (post_id, tag_name, category)
            )
        """)
        conn.execute("""
            INSERT OR IGNORE INTO post_tags(post_id, tag_name, category)
            SELECT post_id, tag_name, category FROM post_tags_legacy
        """)
        conn.execute("DROP TABLE post_tags_legacy")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_post_tags_tag ON post_tags(tag_name)")

    conn.commit()
    return conn


def connect_worker_db(path: Path) -> sqlite3.Connection:
    """Open an already-migrated database without repeating schema setup."""
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def state_get(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM scan_state WHERE key=?", (key,)).fetchone()
    return str(row["value"]) if row else default


def state_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO scan_state(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def as_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None



def save_post(conn: sqlite3.Connection, post: dict[str, Any]) -> None:
    post_id = as_int(post.get("id"))
    if post_id is None:
        LOG.warning("Überspringe Beitrag ohne gültige ID.")
        return

    file_info = post.get("file") or {}
    relationships = post.get("relationships") or {}

    # Listen und Objekte als JSON speichern, damit SQLite sie akzeptiert.
    sources = post.get("sources")
    if sources is not None:
        sources = json.dumps(sources, ensure_ascii=False)

    values = (
        post_id,
        post.get("created_at"),
        post.get("updated_at"),
        as_int(post.get("change_seq")),
        post.get("rating"),
        post.get("description"),
        sources,
        as_int(relationships.get("parent_id")),
        int(bool(relationships.get("has_children")))
        if "has_children" in relationships else None,
        int(bool((post.get("flags") or {}).get("deleted"))),
        as_int(file_info.get("width")),
        as_int(file_info.get("height")),
        file_info.get("ext"),
        as_int(file_info.get("size")),
        file_info.get("md5"),
        file_info.get("url"),
        None,
        0,
        None,
        utc_now(),
    )

    conn.execute("""
        INSERT INTO posts (
            id, created_at, updated_at, change_seq, rating, description, source,
            parent_id, has_children, is_deleted, width, height, extension, file_size, md5,
            file_url, local_path, downloaded, download_error, scanned_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            created_at=excluded.created_at,
            updated_at=excluded.updated_at,
            change_seq=excluded.change_seq,
            rating=excluded.rating,
            description=excluded.description,
            source=excluded.source,
            parent_id=excluded.parent_id,
            has_children=excluded.has_children,
            is_deleted=excluded.is_deleted,
            downloaded=CASE
                WHEN posts.md5 IS NOT excluded.md5
                  OR posts.file_url IS NOT excluded.file_url
                THEN 0 ELSE posts.downloaded
            END,
            verification_status=CASE
                WHEN posts.md5 IS NOT excluded.md5
                  OR posts.file_url IS NOT excluded.file_url
                THEN 'pending' ELSE posts.verification_status
            END,
            local_path=CASE
                WHEN posts.md5 IS NOT excluded.md5
                  OR posts.file_url IS NOT excluded.file_url
                THEN NULL ELSE posts.local_path
            END,
            download_error=CASE
                WHEN posts.md5 IS NOT excluded.md5
                  OR posts.file_url IS NOT excluded.file_url
                THEN NULL ELSE posts.download_error
            END,
            width=excluded.width,
            height=excluded.height,
            extension=excluded.extension,
            file_size=excluded.file_size,
            md5=excluded.md5,
            file_url=excluded.file_url,
            scanned_at=excluded.scanned_at
    """, values)

    # Alle bisherigen Tag-Zuordnungen dieses Beitrags ersetzen.
    tags_obj = post.get("tags") or {}
    if not isinstance(tags_obj, dict):
        tags_obj = {}

    conn.execute(
        "DELETE FROM post_tags WHERE post_id=?",
        (post_id,)
    )

    # Sämtliche Tag-Kategorien dynamisch übernehmen.
    for category, names in tags_obj.items():
        if not isinstance(category, str) or not isinstance(names, list):
            continue

        for name in names:
            if not isinstance(name, str) or not name:
                continue

            conn.execute(
                """
                INSERT INTO tags(name, category)
                VALUES (?, ?)
                ON CONFLICT(name) DO UPDATE
                SET category=excluded.category
                """,
                (name, category),
            )

            conn.execute(
                """
                INSERT OR IGNORE INTO post_tags(post_id, tag_name, category)
                VALUES (?, ?, ?)
                """,
                (post_id, name, category),
            )



def fetch_posts(client: RateLimitedSession, page: str | None) -> list[dict[str, Any]]:
    # status:any includes deleted posts as documented by e6AI;
    # some may no longer have a file URL.
    params: dict[str, Any] = {
        "limit": API_LIMIT,
        "tags": "status:any"
    }

    if page:
        params["page"] = page

    response = client.get(POSTS_ENDPOINT, params=params)

    content_type = response.headers.get("Content-Type", "")

    try:
        payload = response.json()
    except requests.exceptions.JSONDecodeError as exc:
        preview = response.text[:1500].replace("\n", " ")
        raise ArchiveError(
            f"API lieferte kein gültiges JSON (HTTP {response.status_code}, "
            f"Content-Type={content_type!r}). Antwortanfang: {preview!r}"
        ) from exc

    # NEU: API kann ein Objekt mit dem Schlüssel "posts" zurückgeben.
    # Beispiel: {"posts": [{...}, {...}]}
    if isinstance(payload, dict):
        posts = payload.get("posts")

        if not isinstance(posts, list):
            preview = json.dumps(payload, ensure_ascii=False)[:1500]
            raise ArchiveError(
                "API-Antwort ist ein JSON-Objekt, enthält aber keine "
                f"gültige 'posts'-Liste: {preview}"
            )

        payload = posts

    # Prüfen, ob tatsächlich eine Liste angekommen ist.
    if not isinstance(payload, list):
        preview = json.dumps(payload, ensure_ascii=False)[:1500]
        raise ArchiveError(
            "Unerwartetes API-Format: erwartet wurde ein JSON-Array "
            "oder ein Objekt mit 'posts'-Array, "
            f"erhalten wurde {type(payload).__name__}: {preview}"
        )

    if any(not isinstance(item, dict) for item in payload):
        raise ArchiveError(
            "API-Array enthält mindestens einen ungültigen Eintrag."
        )

    return payload


def scan_posts(
    conn: sqlite3.Connection,
    client: RateLimitedSession,
    max_pages: int | None,
    restart: bool,
) -> None:
    if restart:
        state_set(conn, "scan_cursor", "")
        state_set(conn, "scan_complete", "0")
        conn.commit()

    if state_get(conn, "scan_complete") == "1" and not restart:
        LOG.info("Der Metadaten-Scan ist bereits als vollständig markiert.")
        LOG.info("Für einen erneuten Durchlauf --restart-scan verwenden.")
        return

    cursor = state_get(conn, "scan_cursor")
    page_count = 0
    total = 0

    while True:
        if max_pages is not None and page_count >= max_pages:
            LOG.info("Seitenlimit (%s) erreicht; Fortsetzung beim nächsten Start.", max_pages)
            break

        page_arg = f"b{cursor}" if cursor else None
        LOG.info("Rufe Beitragsseite ab: %s", page_arg or "neueste Beiträge")
        posts = fetch_posts(client, page_arg)

        if not posts:
            state_set(conn, "scan_complete", "1")
            conn.commit()
            LOG.info("Keine weiteren Beiträge. Scan abgeschlossen.")
            break

        # Persist each page atomically before moving the cursor.
        ids: list[int] = []
        with conn:
            for post in posts:
                save_post(conn, post)
                post_id = as_int(post.get("id"))
                if post_id is not None:
                    ids.append(post_id)

            if not ids:
                raise ArchiveError("API-Seite enthielt keine verwertbaren Post-IDs.")

            lowest_id = min(ids)
            cursor = str(lowest_id)
            state_set(conn, "scan_cursor", cursor)
            state_set(conn, "scan_complete", "0")
            state_set(conn, "last_scan_at", utc_now())

        page_count += 1
        total += len(posts)
        LOG.info("%s Beiträge verarbeitet; niedrigste ID dieser Seite: %s", len(posts), cursor)

        # A short page means there is no next page in this ordering.
        if len(posts) < API_LIMIT:
            with conn:
                state_set(conn, "scan_complete", "1")
            LOG.info("Letzte Seite erreicht. Scan abgeschlossen.")
            break

    LOG.info("In diesem Lauf verarbeitete Beiträge: %s", total)


def safe_extension(value: Any) -> str:
    ext = str(value or "bin").lower().lstrip(".")
    if not ext.isalnum() or len(ext) > 10:
        return "bin"
    return ext


def allowed_file_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    return parsed.scheme == "https" and (host == "e6ai.net" or host.endswith(".e6ai.net"))


def verify_md5(path: Path, expected: str | None) -> bool:
    # Ohne API-Prüfsumme lässt sich eine Datei nicht zuverlässig verifizieren.
    if not expected or not str(expected).strip():
        return False
    digest = hashlib.md5()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
                digest.update(chunk)
    except OSError:
        return False
    return digest.hexdigest().lower() == str(expected).strip().lower()


def safe_folder_name(value: str) -> str:
    """Turn a tag into one safe path component."""
    cleaned = value.strip().replace("/", "_").replace("\\", "_")
    cleaned = "".join("_" if ord(ch) < 32 or ch in '<>:"|?*' else ch for ch in cleaned)
    cleaned = cleaned.strip(" .")
    return cleaned if cleaned and cleaned not in {".", ".."} else "unknown"


def post_folder(image_dir: Path, conn: sqlite3.Connection, post_id: int) -> Path:
    rows = conn.execute(
        """
        SELECT category, tag_name AS name
        FROM post_tags
        WHERE post_id = ?
        """,
        (post_id,),
    ).fetchall()
    directors = sorted({r["name"] for r in rows if str(r["category"]).casefold() in DIRECTOR_TAG_CATEGORIES})
    characters = sorted({r["name"] for r in rows if str(r["category"]).casefold() == CHARACTER_TAG_CATEGORY})

    director_folder = directors[0] if len(directors) == 1 else ("collabs" if len(directors) > 1 else "unknown")
    character_folder = characters[0] if len(characters) == 1 else ("multiple" if len(characters) > 1 else "unknown")
    return image_dir / safe_folder_name(director_folder) / safe_folder_name(character_folder)


def not_verified_path(image_dir: Path, post_id: int, extension: str) -> Path:
    return image_dir / "not_verified" / f"{post_id}.{extension}"


def available_path(path: Path) -> Path:
    """Return path or a numbered sibling without replacing an existing file."""
    if not path.exists():
        return path
    for number in range(1, 100000):
        candidate = path.with_name(f"{path.stem}.{number}{path.suffix}")
        if not candidate.exists():
            return candidate
    raise ArchiveError(f"Kein freier Dateiname neben {path} gefunden.")


def move_file_no_overwrite(source: Path, destination: Path) -> None:
    """Move/link a file without replacing an existing destination."""
    try:
        os.link(source, destination)
    except OSError as exc:
        fallback_errors = {errno.EXDEV, errno.EPERM, errno.EACCES}
        if hasattr(errno, "ENOTSUP"):
            fallback_errors.add(errno.ENOTSUP)
        if hasattr(errno, "EOPNOTSUPP"):
            fallback_errors.add(errno.EOPNOTSUPP)
        if exc.errno not in fallback_errors:
            raise
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        fd = os.open(destination, flags, 0o644)
        try:
            with os.fdopen(fd, "wb") as target, source.open("rb") as origin:
                shutil.copyfileobj(origin, target, length=CHUNK_SIZE)
                target.flush()
                os.fsync(target.fileno())
        except Exception:
            destination.unlink(missing_ok=True)
            raise
    source.unlink()


def mark_download_error(
    conn: sqlite3.Connection, post_id: int, message: str,
    verification_status: str | None = None,
) -> None:
    with conn:
        if verification_status:
            conn.execute(
                "UPDATE posts SET downloaded=0, verification_status=?, download_error=? WHERE id=?",
                (verification_status, message[:2000], post_id),
            )
        else:
            conn.execute(
                "UPDATE posts SET downloaded=0, download_error=? WHERE id=?",
                (message[:2000], post_id),
            )


def download_one(
    conn: sqlite3.Connection,
    client: RateLimitedSession,
    row: sqlite3.Row,
    image_dir: Path,
) -> bool:
    post_id = int(row["id"])
    url = row["file_url"]
    expected = str(row["md5"] or "").strip()
    if not url:
        mark_download_error(
            conn, post_id,
            "Keine file.url in API-Antwort; Originaldatei nicht abrufbar.",
            verification_status="unavailable",
        )
        return False
    if not allowed_file_url(url):
        mark_download_error(conn, post_id, "Datei-URL außerhalb der e6AI-Domain; aus Sicherheitsgründen ausgelassen.")
        LOG.warning("Post %s ausgelassen: Datei-URL liegt nicht auf e6AI.", post_id)
        return False

    ext = safe_extension(row["extension"])
    is_expected_md5 = bool(expected)
    verified_dir = post_folder(image_dir, conn, post_id)
    unverified_destination = not_verified_path(image_dir, post_id, ext)

    # A file without an API checksum cannot be called verified. Keep it separate.
    destination_dir = verified_dir if is_expected_md5 else unverified_destination.parent
    destination = (verified_dir / f"{post_id}.{ext}") if is_expected_md5 else unverified_destination
    destination_dir.mkdir(parents=True, exist_ok=True)

    # Reuse a correct existing file. If it is corrupt, leave it in place
    # while downloading a replacement; only move it aside if that attempt fails.
    existing_corrupt = False
    if destination.exists():
        if is_expected_md5 and verify_md5(destination, expected):
            with conn:
                conn.execute(
                    "UPDATE posts SET downloaded=1, verification_status='verified', local_path=?, download_error=NULL WHERE id=?",
                    (str(destination), post_id),
                )
            LOG.info("Post %s bereits vorhanden und MD5-geprüft: %s", post_id, destination)
            return True
        if is_expected_md5:
            existing_corrupt = True
            LOG.warning("Post %s: vorhandene Datei hat eine falsche MD5; lade Ersatz, bevor die alte Datei verschoben wird.", post_id)
        else:
            # Without an API checksum, retain the existing unverified file and
            # give the new download a distinct name in not_verified/.
            destination = available_path(destination)

    temp_path = destination.with_name(destination.name + ".part")
    # A .part file can remain after a power loss or forced termination.
    # It is incomplete by definition, so remove it before starting this download.
    temp_path.unlink(missing_ok=True)
    try:
        response = client.get(url, stream=True)
        digest = hashlib.md5()
        try:
            with temp_path.open("xb") as handle:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        handle.write(chunk)
                        digest.update(chunk)
        finally:
            response.close()

        actual = digest.hexdigest()
        verified = is_expected_md5 and actual.lower() == expected.lower()
        if verified:
            destination = verified_dir / f"{post_id}.{ext}"
            destination.parent.mkdir(parents=True, exist_ok=True)
        else:
            destination = available_path(unverified_destination)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if not is_expected_md5:
                reason = "API liefert keine MD5-Prüfsumme; Datei separat unter not_verified gespeichert."
            else:
                reason = f"MD5-Prüfung fehlgeschlagen (erwartet {expected}, erhalten {actual}); Datei separat unter not_verified gespeichert."

        # Only a fully downloaded and MD5-verified replacement may replace
        # the corrupt existing target. os.replace is atomic on the same filesystem.
        if verified and existing_corrupt and destination.exists():
            os.replace(temp_path, destination)
        else:
            move_file_no_overwrite(temp_path, destination)
        temp_path.unlink(missing_ok=True)
        if existing_corrupt and not verified and (verified_dir / f"{post_id}.{ext}").exists():
            # The replacement arrived but failed its checksum: retain both
            # unverified copies outside the normal archive structure.
            old_target = verified_dir / f"{post_id}.{ext}"
            preserved_old = available_path(unverified_destination)
            move_file_no_overwrite(old_target, preserved_old)
            LOG.warning("Post %s: alte fehlerhafte Zieldatei ebenfalls nach not_verified verschoben: %s", post_id, preserved_old)
        with conn:
            conn.execute(
                "UPDATE posts SET downloaded=1, verification_status=?, local_path=?, download_error=? WHERE id=?",
                ("verified" if verified else "not_verified", str(destination), None if verified else reason, post_id),
            )
        if verified:
            LOG.info("Post %s heruntergeladen und MD5-geprüft: %s", post_id, destination)
        else:
            LOG.warning("Post %s gesichert, aber NICHT verifiziert: %s", post_id, destination)
        return True
    except Exception as exc:
        temp_path.unlink(missing_ok=True)
        if existing_corrupt and destination.exists():
            try:
                unverified_destination.parent.mkdir(parents=True, exist_ok=True)
                preserved_path = available_path(unverified_destination)
                move_file_no_overwrite(destination, preserved_path)
                with conn:
                    conn.execute(
                        "UPDATE posts SET downloaded=0, verification_status='not_verified', local_path=?, download_error=? WHERE id=?",
                        (str(preserved_path), str(exc)[:2000], post_id),
                    )
                LOG.warning("Post %s: Ersatzdownload fehlgeschlagen; alte Datei nach not_verified verschoben: %s", post_id, preserved_path)
            except OSError as move_exc:
                mark_download_error(conn, post_id, f"{exc}; alte Datei konnte nicht nach not_verified verschoben werden: {move_exc}")
        else:
            mark_download_error(conn, post_id, str(exc))
        LOG.error("Download für Post %s fehlgeschlagen: %s", post_id, exc)
        return False


def download_pending(
    conn: sqlite3.Connection,
    client: RateLimitedSession,
    image_dir: Path,
    limit: int | None,
    db_path: Path,
    workers: int = 3,
) -> None:
    # Posts ohne Original-URL sind terminal nicht abrufbar und sollen nicht
    # bei jedem Lauf erneut als Download-Aufgabe eingeplant werden.
    with conn:
        conn.execute("""
            UPDATE posts
            SET verification_status='unavailable',
                download_error='Keine file.url in API-Antwort; Originaldatei nicht abrufbar.'
            WHERE downloaded=0 AND (file_url IS NULL OR TRIM(file_url)='')
              AND verification_status != 'unavailable'
        """)
    query = """
        SELECT * FROM posts
        WHERE downloaded=0 AND file_url IS NOT NULL AND TRIM(file_url) != ''
          AND verification_status != 'unavailable'
        ORDER BY id DESC
    """
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    rows = conn.execute(query).fetchall()
    LOG.info("Noch herunterzuladende Dateien: %s", len(rows))
    estimated_bytes = sum(max(0, int(row["file_size"] or 0)) for row in rows)
    unknown_sizes = sum(1 for row in rows if row["file_size"] is None)
    image_dir.mkdir(parents=True, exist_ok=True)
    free_bytes = shutil.disk_usage(image_dir).free
    LOG.info("Geschätzter Platzbedarf für diesen Lauf: %s", format_bytes(estimated_bytes))
    LOG.info("Freier Speicherplatz: %s", format_bytes(free_bytes))
    LOG.info("Parallele Download-Worker: %s; gemeinsames Startintervall: %.2f s",
             workers, client.limiter.interval)
    if unknown_sizes:
        LOG.warning("Für %s Posts fehlt die API-Dateigröße; die Schätzung ist unvollständig.", unknown_sizes)
    if estimated_bytes and free_bytes < estimated_bytes:
        raise ArchiveError(
            f"Zu wenig freier Speicherplatz: benötigt werden laut API mindestens "
            f"{format_bytes(estimated_bytes)}, frei sind {format_bytes(free_bytes)}."
        )

    # Each worker owns its SQLite connection. HTTP sessions are also thread-local,
    # while all workers share the same adaptive request limiter.
    local = threading.local()
    resources: list[tuple[sqlite3.Connection, int]] = []
    resources_lock = threading.Lock()

    def run_one(row: sqlite3.Row) -> bool:
        worker_conn = getattr(local, "conn", None)
        if worker_conn is None:
            worker_conn = connect_worker_db(db_path)
            local.conn = worker_conn
            with resources_lock:
                resources.append((worker_conn, threading.get_ident()))
        return download_one(worker_conn, client, row, image_dir)

    success = 0
    completed = 0
    row_iter = iter(rows)
    max_in_flight = max(workers, workers * 2)
    futures: set[concurrent.futures.Future[bool]] = set()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            for _ in range(min(max_in_flight, len(rows))):
                try:
                    futures.add(executor.submit(run_one, next(row_iter)))
                except StopIteration:
                    break

            while futures:
                done, futures = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for future in done:
                    if future.result():
                        success += 1
                    completed += 1
                    if completed % 100 == 0 or completed == len(rows):
                        LOG.info(
                            "Fortschritt: %s/%s abgeschlossen; erfolgreich: %s; "
                            "Request-Abstand aktuell: %.2f s",
                            completed, len(rows), success, client.limiter.interval,
                        )
                    try:
                        row = next(row_iter)
                    except StopIteration:
                        continue
                    futures.add(executor.submit(run_one, row))
    finally:
        for worker_conn, _thread_id in resources:
            try:
                worker_conn.close()
            except sqlite3.Error:
                pass

    LOG.info("Downloads erfolgreich: %s von %s", success, len(rows))


def format_bytes(value: int) -> str:
    amount = float(max(0, value))
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if amount < 1024 or unit == "TiB":
            return f"{amount:.2f} {unit}"
        amount /= 1024
    return f"{amount:.2f} TiB"


def print_summary(conn: sqlite3.Connection) -> None:
    posts = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    tags = conn.execute("SELECT COUNT(*) FROM tags").fetchone()[0]
    links = conn.execute("SELECT COUNT(*) FROM post_tags").fetchone()[0]
    downloaded = conn.execute("SELECT COUNT(*) FROM posts WHERE downloaded=1").fetchone()[0]
    verified = conn.execute("SELECT COUNT(*) FROM posts WHERE verification_status='verified'").fetchone()[0]
    not_verified = conn.execute("SELECT COUNT(*) FROM posts WHERE verification_status='not_verified'").fetchone()[0]
    unavailable = conn.execute("SELECT COUNT(*) FROM posts WHERE verification_status='unavailable'").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM posts WHERE downloaded=0 AND verification_status!='unavailable'").fetchone()[0]
    total_bytes = conn.execute("SELECT COALESCE(SUM(file_size),0) FROM posts").fetchone()[0]
    downloaded_bytes = conn.execute("SELECT COALESCE(SUM(file_size),0) FROM posts WHERE downloaded=1").fetchone()[0]
    pending_bytes = conn.execute("SELECT COALESCE(SUM(file_size),0) FROM posts WHERE downloaded=0 AND verification_status!='unavailable'").fetchone()[0]
    LOG.info("Datenbankübersicht:")
    LOG.info("  Beiträge: %s", posts)
    LOG.info("  Unterschiedliche Tags: %s", tags)
    LOG.info("  Post-Tag-Verknüpfungen: %s", links)
    LOG.info("  Gesicherte Originaldateien: %s", downloaded)
    LOG.info("  MD5-verifiziert: %s", verified)
    LOG.info("  Unter not_verified gesichert: %s", not_verified)
    LOG.info("  Ohne abrufbare Datei-URL: %s", unavailable)
    LOG.info("  Noch offen / fehlgeschlagen: %s", pending)
    LOG.info("  Geschätzte Gesamtgröße: %s", format_bytes(total_bytes))
    LOG.info("  Geschätzte gesicherte Größe: %s", format_bytes(downloaded_bytes))
    LOG.info("  Geschätzte offene Größe: %s", format_bytes(pending_bytes))
    LOG.info("  Scan-Cursor: %s", state_get(conn, "scan_cursor", "—") or "—")
    LOG.info("  Scan vollständig: %s", "ja" if state_get(conn, "scan_complete") == "1" else "nein")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sichert e6AI-Post-Metadaten, Tags und optional Originalbilder."
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help=f"SQLite-Datei (Standard: {DEFAULT_DB})")
    parser.add_argument("--image-dir", type=Path, default=DEFAULT_IMAGE_DIR, help=f"Bildordner (Standard: {DEFAULT_IMAGE_DIR})")
    parser.add_argument("--user-agent", default=os.environ.get("E6AI_USER_AGENT", DEFAULT_USER_AGENT))
    parser.add_argument("--max-pages", type=int, default=None, help="Maximale Seitenzahl pro Lauf; ohne Angabe vollständig scannen.")
    parser.add_argument("--restart-scan", action="store_true", help="Scan-Cursor zurücksetzen und wieder bei den neuesten Beiträgen beginnen.")
    parser.add_argument("--download", action="store_true", help="Nach dem Metadaten-Scan alle noch fehlenden Originalbilder laden.")
    parser.add_argument("--download-only", action="store_true", help="Nur bereits erfasste, noch fehlende Originalbilder laden.")
    parser.add_argument("--download-limit", type=int, default=None, help="Maximale Anzahl Downloads in diesem Lauf.")
    parser.add_argument("--workers", type=int, default=3, help="Parallele Download-Worker (Standard: 3).")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configure_logging(args.verbose)

    if args.max_pages is not None and args.max_pages < 1:
        LOG.error("--max-pages muss mindestens 1 sein.")
        return 2
    if args.download_limit is not None and args.download_limit < 1:
        LOG.error("--download-limit muss mindestens 1 sein.")
        return 2
    if args.workers < 1 or args.workers > 8:
        LOG.error("--workers muss zwischen 1 und 8 liegen.")
        return 2
    if args.download and args.download_only:
        LOG.error("--download und --download-only können nicht gleichzeitig verwendet werden.")
        return 2

    conn = connect_db(args.db)
    client = RateLimitedSession(args.user_agent)
    try:
        if not args.download_only:
            LOG.info("Starte Metadaten-Scan.")
            scan_posts(conn, client, args.max_pages, args.restart_scan)
        if args.download or args.download_only:
            # A failed scan raises and prevents downloads from starting inadvertently.
            download_pending(conn, client, args.image_dir, args.download_limit, args.db, args.workers)
        print_summary(conn)
        return 0
    except KeyboardInterrupt:
        LOG.warning("Abbruch durch Benutzer. Der gespeicherte Cursor erlaubt die Fortsetzung.")
        print_summary(conn)
        return 130
    except Exception as exc:
        LOG.exception("Archivierung fehlgeschlagen: %s", exc)
        LOG.info("Der zuletzt gespeicherte Cursor bleibt für die Fortsetzung erhalten.")
        print_summary(conn)
        return 1
    finally:
        client.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
