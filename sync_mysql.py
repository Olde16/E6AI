#!/usr/bin/env python3
"""Synchronize the e6AI SQLite archive into MySQL/MariaDB.

SQLite remains read-only. Original files stay at the fixed Nextcloud archive
location; MySQL stores their absolute paths and independently calculated MD5s.
"""
from __future__ import annotations

import argparse
import hashlib
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

LOG = logging.getLogger("sync_mysql")
DEFAULT_SQLITE = Path(__file__).resolve().parent / "archive" / "e6ai_archive.sqlite3"
IMAGE_ROOT = Path("/var/www/nextcloud/data/__groupfolders/1/files/Furry/Artwork/Unsortiert/TWS_AI_DB")
BATCH_SIZE = 500
HASH_CHUNK_SIZE = 4 * 1024 * 1024


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
        ):
            if name not in columns:
                cur.execute(f"ALTER TABLE posts ADD COLUMN {name} {definition}")
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


def calculate_md5(path: str) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(HASH_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalized_md5(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip().lower()
    return value if len(value) == 32 and all(c in "0123456789abcdef" for c in value) else None


def sync(src: sqlite3.Connection, dst) -> None:
    src.row_factory = sqlite3.Row
    create_schema(dst)

    tags_by_post: dict[int, list[dict[str, str]]] = {}
    for row in src.execute(
        "SELECT post_id, tag_name, category FROM post_tags ORDER BY post_id"
    ):
        tags_by_post.setdefault(int(row["post_id"]), []).append({
            "name": row["tag_name"], "category": row["category"] or ""
        })

    favorites_by_post: dict[int, list[dict[str, Any]]] = {}
    if table_exists(src, "favorites"):
        for row in src.execute(
            "SELECT username, post_id, first_seen_at, last_seen_at, is_favorite "
            "FROM favorites ORDER BY post_id, username"
        ):
            favorites_by_post.setdefault(int(row["post_id"]), []).append({
                "username": row["username"], "first_seen_at": row["first_seen_at"],
                "last_seen_at": row["last_seen_at"], "is_favorite": int(row["is_favorite"]),
            })

    # Rebuild relation tables from the authoritative SQLite snapshot. Posts are
    # upserted first so all foreign-key targets exist before relation inserts.
    total = checked = mismatched = missing = 0
    post_cursor = src.execute("SELECT * FROM posts ORDER BY id")
    for chunk in batches(post_cursor):
        values = []
        tag_links = []
        favorite_rows = []
        for row in chunk:
            post = dict(row)
            post_id = int(post["id"])
            absolute_path, exists = resolve_file(post.get("local_path"))
            expected = normalized_md5(post.get("md5"))
            actual = None
            matches = None
            checked_at = None
            if exists:
                actual = calculate_md5(absolute_path)
                checked_at = utc_now()
                if expected:
                    matches = int(actual == expected)
                    checked += 1
                    mismatched += int(not matches)
                else:
                    mismatched += 1
            else:
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
                    "md5_checked_at": checked_at,
                },
            }
            values.append((
                post_id, post.get("created_at"), post.get("updated_at"),
                post.get("change_seq"), post.get("rating"), post.get("description"),
                post.get("source"), post.get("parent_id"), post.get("has_children"),
                post.get("is_deleted") or 0, post.get("width"), post.get("height"),
                post.get("extension"), post.get("file_size"), expected, actual,
                matches, checked_at, post.get("file_url"), absolute_path, int(exists),
                post.get("downloaded") or 0, post.get("verification_status"),
                post.get("download_error"), post.get("scanned_at"),
                json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"), default=str),
            ))
            tag_links.extend((post_id, t["name"], t["category"]) for t in post_tags)
            favorite_rows.extend((
                f["username"], post_id, f["first_seen_at"], f["last_seen_at"], f["is_favorite"]
            ) for f in post_favorites)

        with dst.cursor() as cur:
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
                ON DUPLICATE KEY UPDATE
                    created_at=VALUES(created_at),updated_at=VALUES(updated_at),
                    change_seq=VALUES(change_seq),rating=VALUES(rating),
                    description=VALUES(description),source=VALUES(source),
                    parent_id=VALUES(parent_id),has_children=VALUES(has_children),
                    is_deleted=VALUES(is_deleted),width=VALUES(width),height=VALUES(height),
                    extension=VALUES(extension),file_size=VALUES(file_size),md5=VALUES(md5),
                    file_md5=VALUES(file_md5),md5_matches=VALUES(md5_matches),
                    md5_checked_at=VALUES(md5_checked_at),file_url=VALUES(file_url),
                    absolute_path=VALUES(absolute_path),file_exists=VALUES(file_exists),
                    downloaded=VALUES(downloaded),verification_status=VALUES(verification_status),
                    download_error=VALUES(download_error),scanned_at=VALUES(scanned_at),
                    post_data=VALUES(post_data)
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
                    """
                    INSERT INTO favorites(username,post_id,first_seen_at,last_seen_at,is_favorite)
                    VALUES (%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE first_seen_at=VALUES(first_seen_at),
                        last_seen_at=VALUES(last_seen_at),is_favorite=VALUES(is_favorite)
                    """,
                    favorite_rows,
                )
        dst.commit()
        total += len(chunk)
        LOG.info("Posts synchronisiert: %s", total)

    # Remove stale relations, including favorites removed from SQLite.
    # Build a fresh exact snapshot after posts have all been upserted.
    with dst.cursor() as cur:
        cur.execute("DELETE FROM post_tags")
        cur.execute("DELETE FROM favorites")
        cur.execute("DELETE FROM tags")
        for row in src.execute("SELECT DISTINCT tag_name, category FROM post_tags"):
            cur.execute("INSERT INTO tags(name,category) VALUES (%s,%s)",
                        (row["tag_name"], row["category"] or ""))
        for row in src.execute("SELECT post_id,tag_name,category FROM post_tags"):
            cur.execute("INSERT INTO post_tags(post_id,tag_name,category) VALUES (%s,%s,%s)",
                        (row["post_id"], row["tag_name"], row["category"] or ""))
        if table_exists(src, "favorites"):
            for row in src.execute(
                "SELECT username,post_id,first_seen_at,last_seen_at,is_favorite FROM favorites"
            ):
                cur.execute(
                    "INSERT INTO favorites(username,post_id,first_seen_at,last_seen_at,is_favorite) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (row["username"], row["post_id"], row["first_seen_at"],
                     row["last_seen_at"], row["is_favorite"]),
                )
    dst.commit()
    LOG.info(
        "Fertig: %s Posts; Dateien mit berechneter MD5: %s; MD5-Abweichungen/ohne Soll-MD5: %s; fehlende Dateien: %s",
        total, checked, mismatched, missing,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Überträgt das e6AI-SQLite-Archiv nach MySQL und prüft Datei-MD5."
    )
    parser.add_argument("--sqlite", type=Path, default=DEFAULT_SQLITE)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    if not args.sqlite.is_file():
        LOG.error("SQLite-Archiv nicht gefunden: %s", args.sqlite)
        return 2
    src = sqlite3.connect(f"file:{args.sqlite.resolve()}?mode=ro", uri=True)
    dst = None
    try:
        dst = mysql_connection()
        sync(src, dst)
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
