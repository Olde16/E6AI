# E6AI Archive

Werkzeuge zur Sicherung der e6AI-Post-Metadaten und Originaldateien.

## Hauptarchiv

Standardziel:

`/var/www/nextcloud/data/__groupfolders/1/files/Furry/Artwork/Unsortiert/TWS_AI_DB/`

Die SQLite-Datenbank liegt unter `archive/e6ai_archive.sqlite3`. Der Ordner `old/` wird vom neuen Download nicht durchsucht oder verändert.

```bash
python -m pip install -r requirements.txt

# Metadaten vollständig erfassen und anschließend fehlende Originaldateien laden
python e6ai_archive.py --download

# Nach Abbruch nur die noch offenen Downloads fortsetzen
python e6ai_archive.py --download-only
```

Jeder Post wird genau einem Ordner zugeordnet:

- ein Director + ein Character: `<Director>/<Character>/<postID>.<ext>`
- mehrere Directors: `collabs/`
- kein Director: `unknown/`
- mehrere Characters: `multiple/`
- kein Character: `unknown/`
- fehlende oder nicht passende MD5-Prüfung: `not_verified/<postID>.<ext>`

Eine Datei wird nur dann als verifiziert markiert, wenn eine API-MD5 vorhanden ist und exakt übereinstimmt. Nicht verifizierbare, aber abrufbare Dateien bleiben unter `not_verified/` erhalten. Ohne abrufbare Datei-URL bleiben die Metadaten in SQLite und der Fehler wird protokolliert. Bestehende Zieldateien werden niemals überschrieben.

## Altbestand vorsortieren

Den bisherigen Bestand vorher manuell nach `TWS_AI_DB/old/` legen. Das Sortierskript verändert ihn standardmäßig nicht:

```bash
python sort_old_files.py
```

Wenn die Vorschau stimmt, werden die Dateien mit folgendem Befehl verschoben:

```bash
python sort_old_files.py --apply
```

Das Skript fragt nicht interaktiv nach einer weiteren Bestätigung. Der Aufruf mit `--apply` ist die Ausführungsfreigabe. Dateien ohne passende Post-ID/Metadaten und Zielkonflikte bleiben in `old/`; vorhandene Zieldateien werden nicht überschrieben.

Optionen für abweichende Pfade:

```bash
python e6ai_archive.py --db /pfad/archive.sqlite3 --image-dir /pfad/TWS_AI_DB --download
python sort_old_files.py --root /pfad/TWS_AI_DB --db /pfad/archive.sqlite3
```
