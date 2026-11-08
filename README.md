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
