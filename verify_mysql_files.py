#!/usr/bin/env python3
"""Run a separate integrity check of archived files against the stored e6AI MD5.

This script is deliberately NOT part of sync_mysql.py. It reads each file only
when explicitly run and never changes the API-provided reference column `md5`.
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from datetime import datetime, timezone

import pymysql
from pymysql.cursors import DictCursor

LOG = logging.getLogger("verify_mysql_files")
CHUNK_SIZE = 4 * 1024 * 1024


def connect():
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


def md5_file(path: str) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prüft vorhandene Archivdateien gegen die gespeicherte e6AI-MD5."
    )
    parser.add_argument("--limit", type=int, default=None,
                        help="Optional nur die ersten N noch nicht geprüften Dateien prüfen.")
    parser.add_argument("--all", action="store_true",
                        help="Alle Dateien erneut prüfen, auch bereits geprüfte.")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit muss mindestens 1 sein.")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    conn = None
    try:
        conn = connect()
        with conn.cursor() as cur:
            query = """
                SELECT id, absolute_path, md5
                FROM posts
                WHERE absolute_path IS NOT NULL AND absolute_path <> ''
            """
            if not args.all:
                query += " AND md5_checked_at IS NULL"
            query += " ORDER BY id"
            if args.limit is not None:
                query += f" LIMIT {int(args.limit)}"
            cur.execute(query)
            rows = cur.fetchall()

        checked = matched = mismatched = missing = no_reference = 0
        for row in rows:
            post_id = row["id"]
            path = row["absolute_path"]
            expected = (row["md5"] or "").strip().lower()
            actual = None
            matches = None
            status = "missing"
            try:
                if not os.path.isfile(path):
                    status = "missing"
                    missing += 1
                else:
                    actual = md5_file(path)
                    checked += 1
                    if len(expected) == 32:
                        matches = int(actual == expected)
                        status = "match" if matches else "mismatch"
                        matched += int(matches == 1)
                        mismatched += int(matches == 0)
                    else:
                        status = "no_reference_md5"
                        no_reference += 1
            except OSError as exc:
                status = "read_error"
                LOG.warning("Post %s: Datei konnte nicht gelesen werden: %s", post_id, exc)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE posts
                    SET file_md5=%s, md5_matches=%s, md5_checked_at=%s
                    WHERE id=%s
                    """,
                    (actual, matches, datetime.now(timezone.utc).isoformat(timespec="seconds"), post_id),
                )
            if checked and checked % 250 == 0:
                conn.commit()
                LOG.info("Geprüft: %s Dateien; Übereinstimmung: %s; Abweichung: %s",
                         checked, matched, mismatched)
            if status in ("mismatch", "read_error"):
                LOG.warning("Post %s: %s", post_id, status)

        conn.commit()
        LOG.info(
            "Prüfung beendet: %s gelesen, %s übereinstimmend, %s abweichend, "
            "%s fehlend, %s ohne gültige Referenz-MD5.",
            checked, matched, mismatched, missing, no_reference,
        )
        return 0 if mismatched == 0 else 2
    except Exception:
        LOG.exception("Integritätsprüfung fehlgeschlagen.")
        if conn is not None:
            conn.rollback()
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
