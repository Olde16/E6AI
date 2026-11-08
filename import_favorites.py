#!/usr/bin/env python3
"""
Importiert die Favoriten eines e6AI-Kontos in die bestehende Archiv-SQLite-Datenbank.

- Fragt Benutzername und API-Schlüssel interaktiv ab (Schlüssel verdeckt).
- Sucht /posts.json mit fav:<username> und paginiert über page=b<ID>.
- Übernimmt Post-Metadaten und Tags in die vorhandenen Tabellen.
- Führt eine eigene favorites-Tabelle je Benutzerkonto.
- Markiert entfernte Favoriten erst nach einem vollständig erfolgreichen Abruf.
- Lädt keine Dateien herunter und verändert keine Archivdateien.
"""

from __future__ import annotations

import argparse
import getpass
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from e6ai_archive import (
    API_BASE,
    API_LIMIT,
    AdaptiveRateLimiter,
    ArchiveError,
    RateLimitedSession,
    connect_db,
    save_post,
)

LOG = logging.getLogger("import_favorites")
# /favorites.json liefert laut API-Dokumentation keine Pagination-Parameter.
# Stattdessen verwenden wir die dokumentierte Post-Suche fav:<username>,
# deren page=b<ID>-Cursor ausdrücklich unterstützt wird.
FAVORITES_ENDPOINT = f"{API_BASE}/posts.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_favorites_table(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS favorites (
        username       TEXT NOT NULL,
        post_id        INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
        first_seen_at  TEXT NOT NULL,
        last_seen_at   TEXT NOT NULL,
        is_favorite    INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (username, post_id)
    );
    CREATE INDEX IF NOT EXISTS idx_favorites_user_active
        ON favorites(username, is_favorite);
    """)
    conn.commit()


def extract_posts(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        posts = payload.get("posts")
        if posts is None and isinstance(payload.get("post"), dict):
            posts = [payload["post"]]
    elif isinstance(payload, list):
        posts = payload
    else:
        posts = None

    if not isinstance(posts, list):
        raise ArchiveError(
            "Unerwartetes Antwortformat von /favorites.json; "
            "erwartet wurde eine Liste oder ein Objekt mit 'posts'."
        )
    return [post for post in posts if isinstance(post, dict) and post.get("id") is not None]


def import_favorites(
    conn: sqlite3.Connection,
    client: RateLimitedSession,
    username: str,
    api_key: str,
) -> int:
    ensure_favorites_table(conn)
    # Microseconds make this run marker unique even across quick repeated runs.
    sync_id = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    page: str | None = None
    seen_ids: set[int] = set()
    total = 0
    page_number = 0

    while True:
        params: dict[str, Any] = {
            "limit": API_LIMIT,
            "tags": f"fav:{username}",
        }
        if page:
            params["page"] = page

        response = client.get(
            FAVORITES_ENDPOINT,
            params=params,
            auth=(username, api_key),
        )
        try:
            payload = response.json()
        except requests.exceptions.JSONDecodeError as exc:
            preview = response.text[:1000].replace("\n", " ")
            raise ArchiveError(
                f"e6AI lieferte kein gültiges JSON: {preview!r}"
            ) from exc
        finally:
            response.close()

        posts = extract_posts(payload)
        if not posts:
            break

        page_ids: list[int] = []
        with conn:
            for post in posts:
                try:
                    post_id = int(post["id"])
                except (TypeError, ValueError):
                    continue
                if post_id in seen_ids:
                    continue
                seen_ids.add(post_id)
                page_ids.append(post_id)

                # Speichert vollständige Post-Metadaten und Tag-Kategorien
                # in denselben Tabellen wie der reguläre Archiv-Scanner.
                save_post(conn, post)
                conn.execute("""
                    INSERT INTO favorites (
                        username, post_id, first_seen_at, last_seen_at, is_favorite
                    ) VALUES (?, ?, ?, ?, 1)
                    ON CONFLICT(username, post_id) DO UPDATE SET
                        last_seen_at=excluded.last_seen_at,
                        is_favorite=1
                """, (username, post_id, utc_now(), sync_id))

        page_number += 1
        total += len(page_ids)
        LOG.info(
            "Favoriten-Seite %s verarbeitet: %s neue Posts; insgesamt %s.",
            page_number, len(page_ids), total,
        )

        # Die API verwendet für große Listen den ID-Cursor b<ID>.
        # Bei fehlendem Fortschritt abbrechen, statt in einer Schleife zu hängen.
        if not page_ids:
            raise ArchiveError(
                "Die API lieferte eine nicht-leere Seite, aber keine neuen Post-IDs. "
                "Der Abruf wird als unvollständig behandelt; Favoritenstatus bleibt unverändert."
            )
        next_cursor = min(page_ids)
        if page is not None:
            previous_cursor = int(page[1:])
            if next_cursor >= previous_cursor:
                raise ArchiveError(
                    f"Pagination machte keinen Fortschritt (vorher {previous_cursor}, "
                    f"jetzt {next_cursor}); Favoritenstatus bleibt unverändert."
                )
        page = f"b{next_cursor}"

    # Erst nach vollständig erfolgreichem Abruf werden nicht mehr gelistete
    # Einträge als nicht mehr favorisiert markiert. Bei Fehler/Abbruch bleiben
    # vorherige Favoritenstände unverändert.
    with conn:
        conn.execute(
            "UPDATE favorites SET is_favorite=0 WHERE username=? AND last_seen_at != ?",
            (username, sync_id),
        )

    active = conn.execute(
        "SELECT COUNT(*) FROM favorites WHERE username=? AND is_favorite=1",
        (username,),
    ).fetchone()[0]
    LOG.info("Import abgeschlossen. Aktuelle Favoriten für %s: %s.", username, active)
    return total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Importiert e6AI-Favoriten in die bestehende Archivdatenbank."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).resolve().parent / "archive" / "e6ai_archive.sqlite3",
        help="SQLite-Datei (Standard: archive/e6ai_archive.sqlite3 neben diesem Script).",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    args = parse_args()

    username = input("e6AI-Benutzername: ").strip()
    if not username:
        LOG.error("Der Benutzername darf nicht leer sein.")
        return 2
    api_key = getpass.getpass("e6AI-API-Schlüssel (Eingabe bleibt verborgen): ").strip()
    if not api_key:
        LOG.error("Der API-Schlüssel darf nicht leer sein.")
        return 2

    conn: sqlite3.Connection | None = None
    client: RateLimitedSession | None = None
    try:
        conn = connect_db(args.db)
        ensure_favorites_table(conn)
        limiter = AdaptiveRateLimiter(min_interval=1.0, initial_interval=1.0)
        user_agent = f"E6AI-Favorites-Importer/1.0 (by {username} on e6AI)"
        client = RateLimitedSession(user_agent=user_agent, limiter=limiter)
        import_favorites(conn, client, username, api_key)
        return 0
    except KeyboardInterrupt:
        LOG.warning("Abbruch durch Benutzer. Bereits importierte Seiten bleiben gespeichert.")
        return 130
    except Exception as exc:
        LOG.exception("Favoriten-Import fehlgeschlagen: %s", exc)
        return 1
    finally:
        if client is not None:
            client.close()
        if conn is not None:
            conn.close()
        api_key = ""


if __name__ == "__main__":
    raise SystemExit(main())
