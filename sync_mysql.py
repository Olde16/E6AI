#!/usr/bin/env python3
"""Synchronize SQLite archive metadata and enrich every post from the e6AI API.

SQLite remains read-only. Original files stay at the fixed Nextcloud archive
location; MySQL stores their absolute paths and complete API JSON snapshots.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymysql
from pymysql.cursors import DictCursor

from e6ai_archive import (
    API_BASE, POSTS_ENDPOINT, DEFAULT_USER_AGENT, AdaptiveRateLimiter, RateLimitedSession,
)

LOG = logging.getLogger("sync_mysql")
DEFAULT_SQLITE = Path(__file__).resolve().parent / "archive" / "e6ai_archive.sqlite3"
IMAGE_ROOT = Path("/var/www/nextcloud/data/__groupfolders/1/files/Furry/Artwork/Unsortiert/TWS_AI_DB")
BATCH_SIZE = 500
API_REQUEST_INTERVAL = 0.6


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def mysql_connection():
    required = ("DB_NAME", "DB_USER", "DB_PASSWORD")
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise RuntimeError("Fehlende Umgebungsvariablen: " + ", ".join(missing))
    return pymysql.connect(
        host=os.environ.get("DB_HOST", "127.0.0.1"),
        port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
        charset="utf8mb4",
        autocommit=False,
        cursorclass=DictCursor,
    )


def create_schema(conn) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS posts (
            id BIGINT NOT NULL PRIMARY KEY,
            created_at VARCHAR(64) NULL, updated_at VARCHAR(64) NULL,
            change_seq BIGINT NULL, rating VARCHAR(16) NULL,
            description LONGTEXT NULL, source LONGTEXT NULL, parent_id BIGINT NULL,
            has_children TINYINT NULL, is_deleted TINYINT NOT NULL DEFAULT 0,
            width INT NULL, height INT NULL, extension VARCHAR(16) NULL,
            file_size BIGINT NULL, md5 CHAR(32) NULL, file_md5 CHAR(32) NULL,
            md5_matches TINYINT NULL, md5_checked_at VARCHAR(64) NULL,
            file_url TEXT NULL, absolute_path TEXT NULL,
            file_exists TINYINT NOT NULL DEFAULT 0, downloaded TINYINT NOT NULL DEFAULT 0,
            verification_status VARCHAR(32) NULL, download_error TEXT NULL,
            scanned_at VARCHAR(64) NULL, post_data JSON NOT NULL,
            api_data JSON NULL, api_fetched_at VARCHAR(64) NULL,
            api_fetch_status VARCHAR(32) NULL,
            score_up INT NULL, score_down INT NULL, score_total INT NULL,
            fav_count INT NULL, comment_count INT NULL,
            uploader_id BIGINT NULL, approver_id BIGINT NULL,
            is_favorited TINYINT NULL, locked_tags JSON NULL,
            flags JSON NULL, preview JSON NULL, sample JSON NULL,
            pools JSON NULL, relationships JSON NULL,
            synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_posts_created (created_at), KEY idx_posts_rating (rating),
            KEY idx_posts_parent (parent_id), KEY idx_posts_md5 (md5),
            KEY idx_posts_file_md5 (file_md5), KEY idx_posts_downloaded (downloaded),
            FULLTEXT KEY ft_posts_description (description)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS tags (
            name VARCHAR(512) NOT NULL, category VARCHAR(64) NOT NULL,
            PRIMARY KEY (name, category), KEY idx_tags_category (category)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS post_tags (
            post_id BIGINT NOT NULL, tag_name VARCHAR(512) NOT NULL,
            category VARCHAR(64) NOT NULL DEFAULT '',
            PRIMARY KEY (post_id, tag_name, category),
            KEY idx_post_tags_tag (tag_name), KEY idx_post_tags_category (category),
            CONSTRAINT fk_post_tags_post FOREIGN KEY (post_id)
                REFERENCES posts(id) ON DELETE CASCADE,
            CONSTRAINT fk_post_tags_tag FOREIGN KEY (tag_name, category)
                REFERENCES tags(name, category) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS favorites (
            username VARCHAR(191) NOT NULL, post_id BIGINT NOT NULL,
            first_seen_at VARCHAR(64) NOT NULL, last_seen_at VARCHAR(64) NOT NULL,
            is_favorite TINYINT NOT NULL DEFAULT 1,
            PRIMARY KEY (username, post_id), KEY idx_favorites_post (post_id),
            KEY idx_favorites_active (username, is_favorite),
            CONSTRAINT fk_favorites_post FOREIGN KEY (post_id)
                REFERENCES posts(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
    ]
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
        # Safe additive migrations for databases created by the previous script.
        cur.execute("SHOW COLUMNS FROM posts")
        columns = {row["Field"] for row in cur.fetchall()}
        for name, definition in (
            ("file_md5", "CHAR(32) NULL"),
            ("md5_matches", "TINYINT NULL"),
            ("md5_checked_at", "VARCHAR(64) NULL"),
            ("api_data", "JSON NULL"),
            ("api_fetched_at", "VARCHAR(64) NULL"),
            ("api_fetch_status", "VARCHAR(32) NULL"),
            ("score_up", "INT NULL"), ("score_down", "INT NULL"),
            ("score_total", "INT NULL"), ("fav_count", "INT NULL"),
            ("comment_count", "INT NULL"), ("uploader_id", "BIGINT NULL"),
            ("approver_id", "BIGINT NULL"), ("is_favorited", "TINYINT NULL"),
            ("locked_tags", "JSON NULL"), ("flags", "JSON NULL"),
            ("preview", "JSON NULL"), ("sample", "JSON NULL"),
            ("pools", "JSON NULL"), ("relationships", "JSON NULL"),
        ):
            if name not in columns:
                cur.execute(f"ALTER TABLE posts ADD COLUMN {name} {definition}")
        # Upgrade the old name-only tag key. These relation tables are rebuilt
        # from SQLite during sync, so their old contents need not be preserved.
        cur.execute("SHOW INDEX FROM tags")
        indexes = cur.fetchall()
        primary_columns = [
            row["Column_name"] for row in sorted(
                (item for item in indexes if item["Key_name"] == "PRIMARY"),
                key=lambda item: item["Seq_in_index"],
            )
        ]
        if primary_columns != ["name", "category"]:
            raise RuntimeError(
                "Das vorhandene MySQL-Tags-Schema verwendet noch einen alten Primärschlüssel. "
                "Aus Integritätsgründen werden tags/post_tags nicht automatisch gelöscht oder neu angelegt. "
                "Bitte vor einer Schema-Migration ein Backup erstellen."
            )
    conn.commit()


def table_exists(src: sqlite3.Connection, name: str) -> bool:
    return src.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def batches(cursor, size=BATCH_SIZE):
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        yield rows


def resolve_file(value: Any) -> tuple[str | None, bool]:
    if not value:
        return None, False
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = IMAGE_ROOT / path
    path = path.resolve()
    return str(path), path.is_file()


def normalized_md5(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip().lower()
    return value if len(value) == 32 and all(c in "0123456789abcdef" for c in value) else None

def sync(src: sqlite3.Connection, dst, verbose: bool = False) -> None:
    src.row_factory = sqlite3.Row
    LOG.info("MySQL-Schema prüfen und ggf. ergänzen ...")
    create_schema(dst)
    LOG.info("Synchronisiere ausschließlich neue Posts und fehlende Verknüpfungen ...")

    # Existing MySQL rows, including API-enriched metadata, remain untouched.
    # Relations are added with INSERT IGNORE; no existing rows are deleted.
    total = skipped_existing = missing = 0
    file_index: dict[int, list[Path]] | None = None
    has_favorites = table_exists(src, "favorites")
    post_cursor = src.execute("SELECT * FROM posts ORDER BY id")
    for chunk in batches(post_cursor):
        chunk_ids = [int(row["id"]) for row in chunk]
        mysql_marks = ",".join("%s" for _ in chunk_ids)
        with dst.cursor() as cur:
            cur.execute(
                f"SELECT id FROM posts WHERE id IN ({mysql_marks})",
                chunk_ids,
            )
            existing_ids = {int(item["id"]) for item in cur.fetchall()}
        new_chunk = [row for row in chunk if int(row["id"]) not in existing_ids]
        skipped_existing += len(chunk) - len(new_chunk)
        # Read relations for the whole batch, not only new posts. INSERT IGNORE
        # repairs missing links from an interrupted earlier run without deleting
        # existing links or API-only tags.
        relation_ids = chunk_ids
        sqlite_marks = ",".join("?" for _ in relation_ids)
        tags_by_post: dict[int, list[dict[str, str]]] = {}
        for tag_row in src.execute(
            f"SELECT post_id, tag_name, category FROM post_tags WHERE post_id IN ({sqlite_marks})",
            relation_ids,
        ):
            tags_by_post.setdefault(int(tag_row["post_id"]), []).append({
                "name": tag_row["tag_name"], "category": tag_row["category"] or ""
            })

        favorites_by_post: dict[int, list[dict[str, Any]]] = {}
        if has_favorites:
            for fav_row in src.execute(
                f"SELECT username, post_id, first_seen_at, last_seen_at, is_favorite "
                f"FROM favorites WHERE post_id IN ({sqlite_marks})",
                relation_ids,
            ):
                favorites_by_post.setdefault(int(fav_row["post_id"]), []).append({
                    "username": fav_row["username"],
                    "first_seen_at": fav_row["first_seen_at"],
                    "last_seen_at": fav_row["last_seen_at"],
                    "is_favorite": int(fav_row["is_favorite"]),
                })

        tag_links = [
            (post_id, tag["name"], tag["category"])
            for post_id, post_tag_list in tags_by_post.items()
            for tag in post_tag_list
        ]
        favorite_rows = [
            (fav["username"], post_id, fav["first_seen_at"], fav["last_seen_at"], fav["is_favorite"])
            for post_id, post_favorites in favorites_by_post.items()
            for fav in post_favorites
        ]

        if new_chunk and file_index is None and any(not row["local_path"] for row in new_chunk):
            LOG.info("Erstelle Dateinamen-Index unter %s ...", IMAGE_ROOT)
            file_index = build_file_index(IMAGE_ROOT)
        if file_index is None:
            file_index = {}

        values = []
        for row in new_chunk:
            post = dict(row)
            post_id = int(post["id"])
            absolute_path, exists = resolve_post_file(
                post.get("local_path"), post_id, file_index, post.get("extension")
            )
            expected = normalized_md5(post.get("md5"))
            verified_in_sqlite = post.get("verification_status") == "verified"
            actual = expected if verified_in_sqlite else None
            matches = 1 if verified_in_sqlite else None
            if not exists:
                missing += 1

            post_tags = tags_by_post.get(post_id, [])
            post_favorites = favorites_by_post.get(post_id, [])
            snapshot = {
                "fields": post,
                "tags": post_tags,
                "favorites": post_favorites,
                "local_file": {
                    "absolute_path": absolute_path,
                    "exists_at_sync": exists,
                    "api_md5": expected,
                    "calculated_md5": actual,
                    "md5_matches": matches,
                    "md5_checked_at": None,
                },
            }
            values.append((
                post_id, post.get("created_at"), post.get("updated_at"),
                post.get("change_seq"), post.get("rating"), post.get("description"),
                post.get("source"), post.get("parent_id"), post.get("has_children"),
                post.get("is_deleted") or 0, post.get("width"), post.get("height"),
                post.get("extension"), post.get("file_size"), expected, actual,
                matches, None, post.get("file_url"), absolute_path, int(exists),
                post.get("downloaded") or 0, post.get("verification_status"),
                post.get("download_error"), post.get("scanned_at"),
                json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"), default=str),
            ))
        with dst.cursor() as cur:
            if values:
                cur.executemany(
                    """
                    INSERT INTO posts (
                        id,created_at,updated_at,change_seq,rating,description,source,parent_id,
                        has_children,is_deleted,width,height,extension,file_size,md5,file_md5,
                        md5_matches,md5_checked_at,file_url,absolute_path,file_exists,downloaded,
                        verification_status,download_error,scanned_at,post_data
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                    ON DUPLICATE KEY UPDATE id=posts.id
                    """,
                    values,
                )
            if tag_links:
                cur.executemany(
                    "INSERT IGNORE INTO tags(name,category) VALUES (%s,%s)",
                    [(name, category) for _, name, category in tag_links],
                )
                cur.executemany(
                    "INSERT IGNORE INTO post_tags(post_id,tag_name,category) VALUES (%s,%s,%s)",
                    tag_links,
                )
            if favorite_rows:
                cur.executemany(
                    "INSERT IGNORE INTO favorites(username,post_id,first_seen_at,last_seen_at,is_favorite) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    favorite_rows,
                )
        dst.commit()
        total += len(new_chunk)
        LOG.info(
            "Neue Posts eingefügt: %s; bereits vorhanden übersprungen: %s",
            total, skipped_existing,
        )

    LOG.info(
        "Synchronisierung abgeschlossen: %s neue Posts eingefügt, %s vorhandene Posts unverändert gelassen. "
        "Fehlende Dateien am festen Nextcloud-Pfad: %s. Es wurden keine Dateien geöffnet, verändert oder gehasht; "
        "bestehende MySQL-Postdaten, API-Metadaten und Verknüpfungen wurden nicht gelöscht.",
        total, skipped_existing, missing,
    )



def build_file_index(root: Path) -> dict[int, list[Path]]:
    """Index archive filenames once; never open or hash image contents."""
    index: dict[int, list[Path]] = {}
    if not root.is_dir():
        LOG.warning("Bildarchiv nicht erreichbar; Dateisuche übersprungen: %s", root)
        return index
    for directory, _subdirs, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith(".part"):
                continue
            stem = Path(filename).stem
            # Normal archive names are <post_id>.<extension>. Also accept
            # numbered collision names such as <post_id>.1.<extension>.
            first = stem.split(".", 1)[0]
            if first.isdigit():
                index.setdefault(int(first), []).append(Path(directory) / filename)
    for paths in index.values():
        paths.sort(key=lambda p: (len(p.name.split(".")) != 2, len(str(p)), str(p)))
    LOG.info("Dateiindex aufgebaut: %s Post-IDs gefunden.", len(index))
    return index


def resolve_post_file(sqlite_path: Any, post_id: int, file_index: dict[int, list[Path]], extension: Any = None) -> tuple[str | None, bool]:
    # An explicit SQLite path always wins, even if the file is currently absent.
    if sqlite_path:
        absolute, exists = resolve_file(sqlite_path)
        return absolute, exists
    candidates = file_index.get(post_id, [])
    if not candidates:
        return None, False
    # Prefer the exact <id>.<API-extension> filename where available.
    preferred = f"{post_id}.{str(extension).lstrip('.')}" if extension else None
    chosen_path = next((path for path in candidates if preferred and path.name == preferred), candidates[0])
    chosen = chosen_path.resolve()
    return str(chosen), chosen.is_file()


def json_value(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def fetch_post_detail(client: RateLimitedSession, post_id: int) -> dict[str, Any] | None:
    """Fetch one complete post record using the same HTTP/retry/rate-limit logic as the archive."""
    try:
        response = client.get(f"{API_BASE}/posts/{post_id}.json")
        payload = response.json()
    except Exception as direct_error:
        # Fallback to the documented paginated post search. status:any also
        # includes deleted posts, matching the original archive scan.
        if "HTTP 404" not in str(direct_error):
            raise
        response = client.get(
            POSTS_ENDPOINT,
            params={"tags": f"id:{post_id} status:any", "limit": 1},
        )
        payload = response.json()

    if isinstance(payload, dict) and isinstance(payload.get("post"), dict):
        payload = payload["post"]
    if isinstance(payload, dict) and isinstance(payload.get("posts"), list):
        payload = payload["posts"]
    if isinstance(payload, list):
        payload = next(
            (item for item in payload if isinstance(item, dict) and int(item.get("id", -1)) == post_id),
            None,
        )
    if not isinstance(payload, dict) or int(payload.get("id", -1)) != post_id:
        raise RuntimeError(f"Post {post_id}: unerwartete oder leere API-Antwort")
    return payload


def enrich_mysql_from_api(dst, refresh: bool = False, limit: int | None = None) -> None:
    """Fetch every SQLite-seeded MySQL post missing its API snapshot."""
    query = "SELECT id FROM posts"
    if not refresh:
        query += " WHERE api_fetched_at IS NULL"
    query += " ORDER BY id"
    if limit is not None:
        query += f" LIMIT {int(limit)}"

    with dst.cursor() as cur:
        cur.execute(query)
        post_ids = [int(row["id"]) for row in cur.fetchall()]
    if not post_ids:
        LOG.info("Keine Posts benötigen einen API-Abruf.")
        return

    LOG.info(
        "Starte API-Anreicherung für %s Posts (Request-Abstand %.1f Sekunden).",
        len(post_ids), API_REQUEST_INTERVAL,
    )
    progress_interval = max(1, min(100, len(post_ids) // 20 or 1))
    client = RateLimitedSession(
        DEFAULT_USER_AGENT,
        limiter=AdaptiveRateLimiter(
            min_interval=API_REQUEST_INTERVAL,
            initial_interval=API_REQUEST_INTERVAL,
        ),
    )
    completed = found = not_found = failed = 0
    try:
        for post_id in post_ids:
            fetched_at = utc_now()
            LOG.debug("API-Abruf für Post %s gestartet (%s/%s).", post_id, completed + 1, len(post_ids))
            try:
                data = fetch_post_detail(client, post_id)
                score = data.get("score") or {}
                relationships = data.get("relationships")
                api_tags = data.get("tags") or {}
                api_tag_rows = []
                if isinstance(api_tags, dict):
                    for category, names in api_tags.items():
                        if isinstance(category, str) and isinstance(names, list):
                            api_tag_rows.extend(
                                (post_id, name, category)
                                for name in names if isinstance(name, str) and name
                            )
                with dst.cursor() as cur:
                    if api_tag_rows:
                        cur.executemany(
                            "INSERT IGNORE INTO tags(name,category) VALUES (%s,%s)",
                            [(name, category) for _, name, category in api_tag_rows],
                        )
                        cur.executemany(
                            "INSERT IGNORE INTO post_tags(post_id,tag_name,category) VALUES (%s,%s,%s)",
                            api_tag_rows,
                        )
                    cur.execute(
                        """
                        UPDATE posts SET
                            api_data=%s, api_fetched_at=%s, api_fetch_status='ok',
                            score_up=%s, score_down=%s, score_total=%s,
                            fav_count=%s, comment_count=%s,
                            uploader_id=%s, approver_id=%s, is_favorited=%s,
                            locked_tags=%s, flags=%s, preview=%s, sample=%s,
                            pools=%s, relationships=%s,
                            -- Fill core columns only when the SQLite snapshot lacked a value.
                            created_at=COALESCE(created_at,%s),
                            updated_at=COALESCE(updated_at,%s),
                            change_seq=COALESCE(change_seq,%s),
                            rating=COALESCE(rating,%s),
                            description=COALESCE(description,%s),
                            source=COALESCE(source,%s),
                            parent_id=COALESCE(parent_id,%s),
                            has_children=COALESCE(has_children,%s),
                            is_deleted=GREATEST(is_deleted,COALESCE(%s,0)),
                            width=COALESCE(width,%s), height=COALESCE(height,%s),
                            extension=COALESCE(extension,%s), file_size=COALESCE(file_size,%s),
                            md5=COALESCE(md5,%s), file_url=COALESCE(file_url,%s)
                        WHERE id=%s
                        """,
                        (
                            json_value(data), fetched_at,
                            score.get("up"), score.get("down"), score.get("total"),
                            data.get("fav_count"), data.get("comment_count"),
                            data.get("uploader_id"), data.get("approver_id"),
                            None if "is_favorited" not in data else int(bool(data.get("is_favorited"))),
                            json_value(data.get("locked_tags")), json_value(data.get("flags")),
                            json_value(data.get("preview")), json_value(data.get("sample")),
                            json_value(data.get("pools")), json_value(relationships),
                            data.get("created_at"), data.get("updated_at"), data.get("change_seq"),
                            data.get("rating"), data.get("description"),
                            json_value(data.get("sources")) if isinstance(data.get("sources"), (list, dict)) else data.get("sources"),
                            (relationships or {}).get("parent_id"),
                            None if "has_children" not in (relationships or {}) else int(bool(relationships.get("has_children"))),
                            int(bool((data.get("flags") or {}).get("deleted"))),
                            (data.get("file") or {}).get("width"), (data.get("file") or {}).get("height"),
                            (data.get("file") or {}).get("ext"), (data.get("file") or {}).get("size"),
                            (data.get("file") or {}).get("md5"), (data.get("file") or {}).get("url"),
                            post_id,
                        ),
                    )
                dst.commit()
                found += 1
            except Exception as exc:
                # Record a failed attempt so one unavailable post does not block
                # the rest; allow retry on the next run by leaving fetched_at NULL.
                status = "not_found" if "HTTP 404" in str(exc) else "error"
                with dst.cursor() as cur:
                    cur.execute(
                        "UPDATE posts SET api_fetch_status=%s, api_fetched_at=%s WHERE id=%s",
                        (status, fetched_at if status == "not_found" else None, post_id),
                    )
                dst.commit()
                if status == "not_found":
                    not_found += 1
                    LOG.warning("Post %s ist über die API nicht mehr abrufbar: %s", post_id, exc)
                else:
                    failed += 1
                    LOG.error("API-Abruf für Post %s fehlgeschlagen: %s", post_id, exc)
            completed += 1
            if completed % progress_interval == 0 or completed == len(post_ids):
                LOG.info(
                    "API-Fortschritt: %s/%s; erhalten: %s; nicht gefunden: %s; Fehler: %s",
                    completed, len(post_ids), found, not_found, failed,
                )
    finally:
        client.close()
    LOG.info(
        "API-Anreicherung beendet: %s erfolgreich, %s nicht gefunden, %s Fehler.",
        found, not_found, failed,
    )

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Überträgt SQLite-Metadaten und reichert alle Posts über die e6AI-API an."
    )
    parser.add_argument("--sqlite", type=Path, default=DEFAULT_SQLITE)
    parser.add_argument("--refresh-api", action="store_true",
                        help="Alle Posts erneut von der e6AI-API abrufen, auch bereits angereicherte.")
    parser.add_argument("--api-limit", type=int, default=None,
                        help="API-Abrufe auf diese Anzahl Posts begrenzen (für Testläufe).")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Zusätzliche Details ausgeben (inkl. einzelner API-Abrufe).")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        force=True,
    )
    if args.api_limit is not None and args.api_limit < 1:
        LOG.error("--api-limit muss mindestens 1 sein.")
        return 2
    if not args.sqlite.is_file():
        LOG.error("SQLite-Archiv nicht gefunden: %s", args.sqlite)
        return 2
    LOG.info("SQLite-Archiv: %s", args.sqlite)
    LOG.info("Verbinde mit MySQL ...")
    src = sqlite3.connect(f"file:{args.sqlite.resolve()}?mode=ro", uri=True)
    dst = None
    try:
        dst = mysql_connection()
        LOG.info("MySQL-Verbindung hergestellt.")
        sync(src, dst, verbose=args.verbose)
        LOG.info("Starte API-Phase ...")
        enrich_mysql_from_api(dst, refresh=args.refresh_api, limit=args.api_limit)
        return 0
    except Exception:
        LOG.exception("MySQL-Synchronisierung fehlgeschlagen; SQLite-Quelle blieb unverändert.")
        if dst is not None:
            dst.rollback()
        return 1
    finally:
        src.close()
        if dst is not None:
            dst.close()


if __name__ == "__main__":
    sys.exit(main())
