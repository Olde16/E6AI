#!/usr/bin/env python3
"""Sort the protected legacy files in TWS_AI_DB/old using archive metadata.

Default mode is a preview. Pass --apply to perform moves; no interactive
confirmation is requested because invoking --apply is the authorization.
Files that cannot be matched to a post in SQLite remain in old/ untouched.
"""

from __future__ import annotations

import argparse
import errno
import os
import shutil
import sqlite3
from pathlib import Path

DEFAULT_ROOT = Path("/var/www/nextcloud/data/__groupfolders/1/files/Furry/Artwork/Unsortiert/TWS_AI_DB")
DIRECTOR_CATEGORY = "director"
CHARACTER_CATEGORY = "character"


def safe_folder_name(value: str) -> str:
    cleaned = value.strip().replace("/", "_").replace("\\", "_")
    cleaned = "".join("_" if ord(ch) < 32 or ch in '<>:"|?*' else ch for ch in cleaned)
    cleaned = cleaned.strip(" .")
    return cleaned if cleaned and cleaned not in {".", ".."} else "unknown"


def target_for_post(conn: sqlite3.Connection, root: Path, post_id: int) -> Path | None:
    rows = conn.execute(
        "SELECT category, tag_name FROM post_tags WHERE post_id=?",
        (post_id,),
    ).fetchall()
    if not rows:
        return None

    directors = sorted({name for category, name in rows if str(category).casefold() == DIRECTOR_CATEGORY})
    characters = sorted({name for category, name in rows if str(category).casefold() == CHARACTER_CATEGORY})
    director = directors[0] if len(directors) == 1 else ("collabs" if len(directors) > 1 else "unknown")
    character = characters[0] if len(characters) == 1 else ("multiple" if len(characters) > 1 else "unknown")
    return root / safe_folder_name(director) / safe_folder_name(character)


def move_without_overwrite(source: Path, destination: Path) -> None:
    """Move a file without ever replacing an existing destination."""
    try:
        os.link(source, destination)
        source.unlink()
        return
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            raise
        if exc.errno != errno.EXDEV:
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
    parser.add_argument("--db", type=Path, default=None, help="SQLite-Datei; Standard: <root>/archive/e6ai_archive.sqlite3")
    parser.add_argument("--apply", action="store_true", help="Verschiebt Dateien tatsächlich. Ohne diese Option nur Vorschau.")
    args = parser.parse_args()

    root = args.root.resolve()
    old_dir = root / "old"
    db_path = args.db or (root / "archive" / "e6ai_archive.sqlite3")
    if not old_dir.is_dir():
        parser.error(f"Altbestand-Ordner nicht gefunden: {old_dir}")
    if not db_path.is_file():
        parser.error(f"SQLite-Datenbank nicht gefunden: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(post_tags)")}
        if "category" not in columns:
            parser.error("Die Datenbank hat noch keine Kategorie pro Post-Tag-Verknüpfung. Bitte zuerst e6ai_archive.py aktualisieren/ausführen.")

        moved = 0
        skipped = 0
        conflicts = 0
        for source in sorted(p for p in old_dir.rglob("*") if p.is_file()):
            try:
                post_id = int(source.stem)
            except ValueError:
                print(f"ÜBERSPRUNGEN (Dateiname ohne numerische Post-ID): {source}")
                skipped += 1
                continue

            target_folder = target_for_post(conn, root, post_id)
            if target_folder is None:
                print(f"NICHT ZUORDENBAR (keine Tags/kein DB-Eintrag): {source}")
                skipped += 1
                continue

            destination = target_folder / source.name
            if destination.exists():
                print(f"KONFLIKT (Ziel existiert; Quelle bleibt in old): {source} -> {destination}")
                conflicts += 1
                continue

            print(f"{'VERSCHIEBE' if args.apply else 'VORSCHAU'}: {source} -> {destination}")
            if args.apply:
                destination.parent.mkdir(parents=True, exist_ok=True)
                # Hard-link creation is atomic and refuses to overwrite an existing target.
                move_without_overwrite(source, destination)
            moved += 1

        print()
        print(f"Fertig. {'Verschoben' if args.apply else 'Vorschau'}: {moved}; übersprungen: {skipped}; Konflikte: {conflicts}.")
        if not args.apply:
            print("Es wurde nichts verändert. Zum tatsächlichen Verschieben --apply angeben.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
