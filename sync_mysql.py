#!/usr/bin/env python3
"""Synchronize the existing e6AI SQLite archive into MySQL/MariaDB.

The SQLite archive remains the source of truth and is never modified.
Posts retain searchable columns plus a JSON snapshot of every field currently
stored by e6ai_archive.py, their tag assignments and favorite-account status.
Original files stay on the Nextcloud filesystem; only absolute paths are stored.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pymysql
from pymysql.cursors import DictCursor

LOG = logging.getLogger("sync_mysql")
DEFAULT_SQLITE = Path(__file__).resolve().parent / "archive" / "e6ai_archive.sqlite3"
DEFAULT_IMAGE_ROOT = Path(
    "/var/www/nextcloud/data/__groupfolders/1/files/Furry/Artwork/Unsortiert/TWS_AI_DB"
)
BATCH_SIZE = 500


def mysql_connection():
    required = ("DB_NAME", "DB_USER", "DB_PASSWORD")
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise RuntimeError(
            "Fehlende Umgebungsvariablen: " + ", ".join(missing)
            + ". Erwartet werden DB_NAME, DB_USER und DB_PASSWORD."
        )
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
            created_at VARCHAR(64) NULL,
            updated_at VARCHAR(64) NULL,
            change_seq BIGINT NULL,
            rating VARCHAR(16) NULL,
            description LONGTEXT NULL,
            source LONGTEXT NULL,
            parent_id BIGINT NULL,
            has_children TINYINT NULL,
            is_deleted TINYINT NOT NULL DEFAULT 0,
            width INT NULL,
            height INT NULL,
            extension VARCHAR(16) NULL,
            file_size BIGINT NULL,
            md5 VARCHAR(64) NULL,
            file_url TEXT NULL,
            absolute_path TEXT NULL,
            file_exists TINYINT NOT NULL DEFAULT 0,
            downloaded TINYINT NOT NULL DEFAULT 0,
            verification_status VARCHAR(32) NULL,
            download_error TEXT NULL,
            scanned_at VARCHAR(64) NULL,
            post_data JSON NOT NULL,
            synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_posts_created (created_at),
            KEY idx_posts_rating (rating),
            KEY idx_posts_parent (parent_id),
            KEY idx_posts_md5 (md5),
            KEY idx_posts_downloaded (downloaded),
            FULLTEXT KEY ft_posts_description (description)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS tags (
            name VARCHAR(255) NOT NULL PRIMARY KEY,
            category VARCHAR(64) NOT NULL,
            KEY idx_tags_category (category)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS post_tags (
            post_id BIGINT NOT NULL,
            tag_name VARCHAR(255) NOT NULL,
            category VARCHAR(64) NOT NULL DEFAULT '',
            PRIMARY KEY (post_id, tag_name, category),
            KEY idx_post_tags_tag (tag_name),
            KEY idx_post_tags_category (category),
            CONSTRAINT fk_post_tags_post FOREIGN KEY (post_id)
                REFERENCES posts(id) ON DELETE CASCADE,
            CONSTRAINT fk_post_tags_tag FOREIGN KEY (tag_name)
                REFERENCES tags(name) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS favorites (
            username VARCHAR(191) NOT NULL,
            post_id BIGINT NOT NULL,
            first_seen_at VARCHAR(64) NOT NULL,
            last_seen_at VARCHAR(64) NOT NULL,
            is_favorite TINYINT NOT NULL DEFAULT 1,
            PRIMARY KEY (username, post_id),
            KEY idx_favorites_post (post_id),
            KEY idx_favorites_active (username, is_favorite),
            CONSTRAINT fk_favorites_post FOREIGN KEY (post_id)
                REFERENCES posts(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
    ]
    with conn.cursor() as cur:
        for sql in statements:
            cur.execute(sql)
    conn.commit()


def table_exists(src: sqlite3.Connection, name: str) -> bool:
    return src.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def absolute_file_path(value: Any, image_root: Path) -> tuple[str | None, bool]:
    if not value:
        return None, False
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = image_root / path
    path = path.resolve()
    return str(path), path.is_file()


def batches(rows, size=BATCH_SIZE):
    while True:
        chunk = rows.fetchmany(size)
        if not chunk:
            break
        yield chunk


def sync(src: sqlite3.Connection, dst, image_root: Path) -> None:
    src.row_factory = sqlite3.Row
    create_schema(dst)

    tags_by_post: dict[int, list[dict[str, str]]] = {}
    with src:
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
            "FROM favorites ORDER BY post_id"
        ):
            favorites_by_post.setdefault(int(row["post_id"]), []).append({
                "username": row["username"],
                "first_seen_at": row["first_seen_at"],
                "last_seen_at": row["last_seen_at"],
                "is_favorite": int(row["is_favorite"]),
            })

    posts_cur = src.execute("SELECT * FROM posts ORDER BY id")
    total = 0
    for chunk in batches(posts_cur):
        post_values = []
        tag_values = []
        favorite_values = []
        for row in chunk:
            post = dict(row)
            post_id = int(post["id"])
            absolute_path, file_exists = absolute_file_path(
                post.get("local_path"), image_root
            )
            post_tags = tags_by_post.get(post_id, [])
            post_favorites = favorites_by_post.get(post_id, [])
            snapshot = {
                "fields": post,
                "tags": post_tags,
                "favorites": post_favorites,
                "local_file": {
                    "absolute_path": absolute_path,
                    "exists_at_sync": file_exists,
                },
            }
            post_values.append((
                post_id, post.get("created_at"), post.get("updated_at"),
                post.get("change_seq"), post.get("rating"), post.get("description"),
                post.get("source"), post.get("parent_id"), post.get("has_children"),
                post.get("is_deleted") or 0, post.get("width"), post.get("height"),
                post.get("extension"), post.get("file_size"), post.get("md5"),
                post.get("file_url"), absolute_path, int(file_exists),
                post.get("downloaded") or 0, post.get("verification_status"),
                post.get("download_error"), post.get("scanned_at"),
                json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"), default=str),
            ))
            for tag in post_tags:
                tag_values.append((post_id, tag["name"], tag["category"]))
            for fav in post_favorites:
                favorite_values.append((
                    fav["username"], post_id, fav["first_seen_at"],
                    fav["last_seen_at"], fav["is_favorite"]
                ))

        with dst.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO posts (
                    id, created_at, updated_at, change_seq, rating, description,
                    source, parent_id, has_children, is_deleted, width, height,
                    extension, file_size, md5, file_url, absolute_path, file_exists,
                    downloaded, verification_status, download_error, scanned_at, post_data
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON DUPLICATE KEY UPDATE
                    created_at=VALUES(created_at), updated_at=VALUES(updated_at),
                    change_seq=VALUES(change_seq), rating=VALUES(rating),
                    description=VALUES(description), source=VALUES(source),
                    parent_id=VALUES(parent_id), has_children=VALUES(has_children),
                    is_deleted=VALUES(is_deleted), width=VALUES(width), height=VALUES(height),
                    extension=VALUES(extension), file_size=VALUES(file_size), md5=VALUES(md5),
                    file_url=VALUES(file_url), absolute_path=VALUES(absolute_path),
                    file_exists=VALUES(file_exists), downloaded=VALUES(downloaded),
                    verification_status=VALUES(verification_status),
                    download_error=VALUES(download_error), scanned_at=VALUES(scanned_at),
                    post_data=VALUES(post_data)
                """,
                post_values,
            )
            if tag_values:
                cur.executemany(
                    """
                    INSERT INTO tags(name, category) VALUES (%s,%s)
                    ON DUPLICATE KEY UPDATE category=VALUES(category)
                    """,
                    [(t[1], t[2]) for t in tag_values],
                )
                cur.executemany(
                    """
                    INSERT IGNORE INTO post_tags(post_id, tag_name, category)
                    VALUES (%s,%s,%s)
                    """,
                    tag_values,
                )
            if favorite_values:
                cur.executemany(
                    """
                    INSERT INTO favorites(username, post_id, first_seen_at, last_seen_at, is_favorite)
                    VALUES (%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        first_seen_at=VALUES(first_seen_at),
                        last_seen_at=VALUES(last_seen_at),
                        is_favorite=VALUES(is_favorite)
                    """,
                    favorite_values,
                )
        dst.commit()
        total += len(chunk)
        LOG.info("Posts synchronisiert: %s", total)

    # Remove stale tag/favorite links only after all posts were copied successfully.
    # Replace post_tags wholesale to reflect the SQLite source exactly.
    with dst.cursor() as cur:
        cur.execute("DELETE FROM post_tags")
        cur.execute("DELETE FROM tags")
    dst.commit()
    # Rebuild tag tables from the source after the post pass.
    with dst.cursor() as cur:
        for row in src.execute("SELECT name, category FROM tags"):
            cur.execute(
                "INSERT INTO tags(name, category) VALUES (%s,%s) "
                "ON DUPLICATE KEY UPDATE category=VALUES(category)",
                (row["name"], row["category"]),
            )
        for row in src.execute("SELECT post_id, tag_name, category FROM post_tags"):
            cur.execute(
                "INSERT IGNORE INTO post_tags(post_id, tag_name, category) VALUES (%s,%s,%s)",
                (row["post_id"], row["tag_name"], row["category"]),
            )
    dst.commit()
    LOG.info("Abgeschlossen: %s Posts; SQLite bleibt unverändert.", total)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Überträgt das bestehende e6AI-SQLite-Archiv nach MySQL."
    )
    parser.add_argument("--sqlite", type=Path, default=DEFAULT_SQLITE)
    parser.add_argument("--image-root", type=Path, default=DEFAULT_IMAGE_ROOT)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    if not args.sqlite.is_file():
        LOG.error("SQLite-Archiv nicht gefunden: %s", args.sqlite)
        return 2

    src = sqlite3.connect(f"file:{args.sqlite.resolve()}?mode=ro", uri=True)
    dst = None
    try:
        dst = mysql_connection()
        sync(src, dst, args.image_root)
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
