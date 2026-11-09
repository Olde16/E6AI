# E6AI Archive

Werkzeuge zur Sicherung der e6AI-Post-Metadaten und Originaldateien.

## Hauptarchiv

Standardziel:

`/var/www/nextcloud/data/__groupfolders/1/files/Furry/Artwork/Unsortiert/TWS_AI_DB/`

Die SQLite-Datenbank bleibt unter `archive/e6ai_archive.sqlite3` neben den Skripten (wie bisher). Der Ordner `TWS_AI_DB/old/` wird vom neuen Download nicht durchsucht oder verändert.

```bash
python -m pip install -r requirements.txt

# Metadaten vollständig erfassen und anschließend fehlende Originaldateien laden
python e6ai_archive.py --download

# Nach Abbruch nur die noch offenen Downloads fortsetzen
python e6ai_archive.py --download-only

# Parallele Downloads (Standard: 3 Worker; global adaptives Request-Limit)
python e6ai_archive.py --download-only --workers 3
```

Jeder Post wird genau einem Ordner zugeordnet:

- ein Director/Artist-Tag + ein Character-Tag: `<Director>/<Character>/<postID>.<ext>`
- mehrere Director/Artist-Tags: `collabs/`
- kein Director/Artist-Tag: `unknown/`
- mehrere Character-Tags: `multiple/`
- kein Character-Tag: `unknown/`
- fehlende oder nicht passende MD5-Prüfung: `not_verified/<postID>.<ext>`

Die API-Kategorie `artist` wird als Director-Zuordnung behandelt; `director` wird ebenfalls akzeptiert. Eine Datei wird nur dann als verifiziert markiert, wenn eine API-MD5 vorhanden ist und exakt übereinstimmt. Nicht verifizierbare, aber abrufbare Dateien bleiben unter `not_verified/` erhalten. Ohne abrufbare Datei-URL bleiben die Metadaten in SQLite und der Fehler wird protokolliert. Bestehende Zieldateien werden niemals überschrieben.

Vor Downloads prüft das Skript den verfügbaren Speicherplatz und vergleicht ihn mit der aus den API-Dateigrößen geschätzten offenen Datenmenge. Die Zusammenfassung zeigt Gesamtgröße, gesicherte Größe und offene Größe.

## Altbestand vorsortieren

Den bisherigen Bestand vorher nach `TWS_AI_DB/old/` legen. Erst den Metadaten-Scan vollständig abschließen; das Sortierskript verweigert die Arbeit bei unvollständigem Scan. Danach verschiebt es die Dateien bei normaler Ausführung. Es fragt nicht interaktiv nach einer weiteren Bestätigung: Das Ausführen ist die Freigabe.

```bash
# Vorschau, ohne Änderungen
python sort_old_files.py --dry-run

# Dateien tatsächlich einsortieren
python sort_old_files.py
```

Dateien mit numerischer Post-ID werden vor der Zuordnung gegen die MD5 in SQLite geprüft. Nur passende Dateien kommen in die Director/Character-Struktur. Fehlende Datenbankeinträge, fehlende MD5 oder MD5-Abweichungen landen in `not_verified/`. Dateien ohne numerische Post-ID und Zielkonflikte bleiben in `old/`; vorhandene Zieldateien werden nicht überschrieben.

Optionen für abweichende Pfade:

```bash
python e6ai_archive.py --db /pfad/archive.sqlite3 --image-dir /pfad/TWS_AI_DB --download
python sort_old_files.py --root /pfad/TWS_AI_DB --db /pfad/archive.sqlite3
```

## Favoriten importieren

Das separate Script `import_favorites.py` liest die Favoriten des angegebenen e6AI-Kontos über die paginierbare Post-Suche `/posts.json?tags=fav:<username>` ein. `/favorites.json` besitzt laut API-Dokumentation keinen dokumentierten Cursor-Parameter und ist deshalb für große Favoritenlisten ungeeignet. Benutzername und API-Schlüssel werden beim Start abgefragt; die API-Schlüsseleingabe bleibt verdeckt. Der Schlüssel wird nicht gespeichert.

Die Favoriten-Postdaten und Tags werden in dieselbe SQLite-Datenbank übernommen. Zusätzlich gibt es die Tabelle `favorites` mit Benutzername, Post-ID und aktuellem Favoritenstatus. Der Import läuft mit dem dokumentierten `page=b<ID>`-Cursor bis zu einer leeren Ergebnisseite. Nur ein vollständig erfolgreicher Durchlauf markiert inzwischen entfernte Favoriten als nicht mehr favorisiert. Bei API-Fehlern oder fehlendem Pagination-Fortschritt wird dieser Abgleich nicht durchgeführt.

Das Script lädt keine Originaldateien herunter und verändert keine Archivdateien. Es verwendet standardmäßig dieselbe Datenbank unter `archive/e6ai_archive.sqlite3`.

```bash
python import_favorites.py
```

Für den API-Abruf wird ein Request-Abstand von mindestens einer Sekunde verwendet.

## MySQL-Kopie auf dem Server

`sync_mysql.py` überträgt die vorhandenen SQLite-Postdaten, Tags, Post-Tag-Zuordnungen und Favoriten nach MySQL. Die SQLite-Datei wird ausschließlich lesend geöffnet und bleibt unverändert. Die Nextcloud-Dateien werden nicht zum SQL-Server kopiert; in MySQL wird nur ihr absoluter Dateipfad gespeichert. Das Feld `post_data` enthält einen JSON-Snapshot aus allen Feldern, Tags und Favoriten, die aktuell in SQLite vorhanden sind.

Zugangsdaten über Umgebungsvariablen setzen:

```bash
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_NAME=e6ai_archive
export DB_USER=DEIN_DB_BENUTZER
export DB_PASSWORD='DEIN_DB_PASSWORT'

python -m pip install -r requirements.txt
python sync_mysql.py
```

Optional:

```bash
python sync_mysql.py --sqlite /pfad/e6ai_archive.sqlite3 --image-root /absoluter/pfad/zum/Nextcloud-Archiv
```

Der Lauf ist wiederholbar: bestehende Posts werden anhand ihrer e6AI-ID aktualisiert. `absolute_path` verweist auf den Nextcloud-Dateipfad; `file_exists` hält fest, ob die Datei beim Synchronisieren dort vorhanden war.
