#!/usr/bin/env python3
"""Sort protected legacy files in TWS_AI_DB/old using SQLite metadata and MD5.

Normal execution performs moves without an interactive confirmation.
Use --dry-run for a preview. Verified files belonging to deleted posts go to
deleted/ for later manual sorting. Files without a numeric post ID and destination
conflicts remain in old/; files that cannot be verified go to not_verified/.
"""

from __future__ import annotations

import argparse
import errno
import hashlib
import os
import shutil
import sqlite3
from pathlib import Path

DEFAULT_ROOT = Path("/var/www/nextcloud/data/__groupfolders/1/files/Furry/Artwork/Unsortiert/TWS_AI_DB")
DIRECTOR_CATEGORIES = {"director", "artist"}
CHARACTER_CATEGORY = "character"


def safe_folder_name(value: str) -> str:
    cleaned = value.strip().replace("/", "_").replace("\\", "_")
    cleaned = "".join("_" if ord(ch) < 32 or ch in '<>:"|?*' else ch for ch in cleaned)
    cleaned = cleaned.strip(" .")
    return cleaned if cleaned and cleaned not in {".", ".."} else "unknown"


def target_for_post(
    conn: sqlite3.Connection, root: Path, post_id: int
) -> tuple[Path | None, str | None]:
    post = conn.execute("SELECT md5, is_deleted FROM posts WHERE id=?", (post_id,)).fetchone()
    if post is None:
        return None, None
    if bool(post["is_deleted"]):
        return root / "deleted", post["md5"]

    rows = conn.execute(
        "SELECT category, tag_name FROM post_tags WHERE post_id=?",
        (post_id,),
    ).fetchall()
    directors = sorted({name for category, name in rows if str(category).casefold() in DIRECTOR_CATEGORIES})
    characters = sorted({name for category, name in rows if str(category).casefold() == CHARACTER_CATEGORY})
    director = directors[0] if len(directors) == 1 else ("collabs" if len(directors) > 1 else "unknown")
    character = characters[0] if len(characters) == 1 else ("multiple" if len(characters) > 1 else "unknown")
    return root / safe_folder_name(director) / safe_folder_name(character), post["md5"]


def verify_md5(path: Path, expected: str | None) -> bool:
    if not expected or not str(expected).strip():
        return False
    digest = hashlib.md5()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return False
    return digest.hexdigest().lower() == str(expected).strip().lower()


def move_without_overwrite(source: Path, destination: Path) -> None:
    """Move a file without ever replacing an existing destination."""
    try:
        os.link(source, destination)
        source.unlink()
        return
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            raise
        fallback_errors = {errno.EXDEV, errno.EPERM, errno.EACCES}
        if hasattr(errno, "ENOTSUP"):
            fallback_errors.add(errno.ENOTSUP)
        if hasattr(errno, "EOPNOTSUPP"):
            fallback_errors.add(errno.EOPNOTSUPP)
        if exc.errno not in fallback_errors:
            raise

    temp = destination.with_name(destination.name + ".moving.part")
    try:
        with source.open("rb") as src, temp.open("xb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
            dst.flush()
            os.fsync(dst.fileno())
        shutil.copystat(source, temp)
        os.link(temp, destination)
        temp.unlink()
        source.unlink()
    except Exception:
        temp.unlink(missing_ok=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description="Vorsortierung des Altbestands anhand der e6AI-SQLite-Datenbank.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="Archiv-Stammverzeichnis.")
    parser.add_argument("--db", type=Path, default=None, help="SQLite-Datei; Standard: archive/e6ai_archive.sqlite3 neben diesem Skript")
    parser.add_argument("--dry-run", action="store_true", help="Nur Vorschau anzeigen; keine Dateien verschieben.")
    args = parser.parse_args()

    root = args.root.resolve()
    old_dir = root / "old"
    db_path = args.db or (Path(__file__).resolve().parent / "archive" / "e6ai_archive.sqlite3")
    if not old_dir.is_dir():
        parser.error(f"Altbestand-Ordner nicht gefunden: {old_dir}")
    if not db_path.is_file():
        parser.error(f"SQLite-Datenbank nicht gefunden: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(post_tags)")}
        if "category" not in columns:
            parser.error("Die Datenbank hat noch keine Kategorie pro Post-Tag-Verknüpfung. Bitte zuerst e6ai_archive.py ausführen.")
        scan_state = conn.execute("SELECT value FROM scan_state WHERE key='scan_complete'").fetchone()
        if scan_state is None or scan_state[0] != "1":
            parser.error("Der Metadaten-Scan ist noch nicht vollständig. Erst e6ai_archive.py ohne --max-pages vollständig laufen lassen.")

        moved = 0
        skipped = 0
        conflicts = 0
        print(f"Durchsuche Altbestand: {old_dir}", flush=True)
        file_count = 0
        for dirpath, dirnames, filenames in os.walk(old_dir):
                dirnames.sort()
                filenames.sort()
                for filename in filenames:
                    source = Path(dirpath) / filename
                    if not source.is_file():
                        continue
                    file_count += 1
                    if file_count == 1 or file_count % 1000 == 0:
                        print(f"Gefundene Dateien: {file_count}", flush=True)
                try:
                    post_id = int(source.stem)
                except ValueError:
                    print(f"ÜBERSPRUNGEN (Dateiname ohne numerische Post-ID): {source}")
                    skipped += 1
                    continue

                target_folder, expected_md5 = target_for_post(conn, root, post_id)
                if target_folder is not None and verify_md5(source, expected_md5):
                    destination = target_folder / source.name
                    reason = "gelöschter Beitrag (API-Flag deleted)" if target_folder == root / "deleted" else "MD5 stimmt"
                else:
                    destination = root / "not_verified" / source.name
                    if target_folder is None:
                        reason = "kein Post-Eintrag in SQLite"
                    elif not expected_md5:
                        reason = "MD5 fehlt in SQLite"
                    else:
                        reason = "MD5 stimmt nicht"

                if destination.exists():
                    print(f"KONFLIKT ({reason}; Ziel existiert; Quelle bleibt in old): {source} -> {destination}")
                    conflicts += 1
                    continue

                print(f"{'VORSCHAU' if args.dry_run else 'VERSCHIEBE'} ({reason}): {source} -> {destination}")
                if not args.dry_run:
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    # Hard-link creation is atomic and refuses to overwrite an existing target.
                    move_without_overwrite(source, destination)
                moved += 1

        print()
        print(f"Fertig. {'Vorschau' if args.dry_run else 'Verschoben'}: {moved}; übersprungen: {skipped}; Konflikte: {conflicts}.")
        if args.dry_run:
            print("Es wurde nichts verändert. Ohne --dry-run werden die Dateien verschoben.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
