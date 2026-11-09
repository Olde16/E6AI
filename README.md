# E6AI Archive

Werkzeuge zur Sicherung der e6AI-Post-Metadaten und Originaldateien.

## Hauptarchiv

Standardziel:

`/var/www/nextcloud/data/__groupfolders/1/files/Furry/Artwork/Unsortiert/TWS_AI_DB/`

Die SQLite-Datenbank liegt unter `archive/e6ai_archive.sqlite3`.

```bash
python -m pip install -r requirements.txt

# Metadaten vollständig erfassen und fehlende Originaldateien laden
python e6ai_archive.py --download

# Nach Abbruch nur noch offene Downloads fortsetzen
python e6ai_archive.py --download-only --workers 3
```

Jeder Post wird anhand seiner Tags einem Archivordner zugeordnet. Eine Datei wird beim ursprünglichen Download anhand der von e6AI gelieferten MD5 geprüft. Der Prüfstatus wird in SQLite gespeichert. Die Bilddateien bleiben auf Nextcloud.

## MySQL-Archiv und API-Anreicherung

`sync_mysql.py` überträgt zunächst alle Posts, Tags, Tag-Zuordnungen und Favoriten aus SQLite nach MySQL. SQLite wird ausschließlich lesend geöffnet.

Danach wird für jede Post-ID in MySQL, für die noch kein erfolgreicher API-Abruf gespeichert ist, der vollständige Post-Datensatz über die e6AI-API abgerufen. Die HTTP- und Retry-Logik sowie der benutzerdefinierte User-Agent werden aus dem Hauptprogramm `e6ai_archive.py` wiederverwendet. Der Request-Abstand beträgt eine Sekunde, entsprechend der API-Empfehlung.

Gespeichert werden:
- der vollständige API-Response unverändert als JSON in `posts.api_data`
- API-Abrufzeitpunkt und Abrufstatus
- Score (Up-/Downvotes und Gesamtwert), Favoritenanzahl und Kommentaranzahl
- Uploader- und Approver-ID sowie – falls von der API geliefert – der Favoritenstatus des authentifizierten Benutzers
- Flags, gesperrte Tags, Preview-/Sample-Metadaten, Pool-IDs und Post-Beziehungen als JSON
- zusätzliche aktuelle Tag-Zuordnungen samt Kategorie in `tags` und `post_tags`

Die vorhandenen SQLite-Werte bleiben erhalten. API-Daten ergänzen fehlende Kernwerte; die vollständige API-Antwort bleibt zusätzlich separat erhalten. Der bereits beim Archiv-Download durchgeführte MD5-Abgleich wird aus SQLite übernommen: bei `verification_status='verified'` wird `md5_matches=1` und die bestätigte Prüfsumme in `file_md5` gespeichert. Es findet beim MySQL-Sync keine erneute Dateiprüfung statt.

### Dateipfade

Wenn SQLite für einen Post bereits einen `local_path` enthält, wird genau dieser Pfad übernommen. Nur wenn SQLite keinen Pfad enthält, durchsucht das Skript den fest hinterlegten Nextcloud-Archivordner einmalig und erstellt daraus einen Dateiindex anhand der Post-IDs. Die Bilddateien werden dabei weder geöffnet noch kopiert oder gehasht. MySQL speichert den absoluten Pfad und den Status, ob die Datei zum Sync-Zeitpunkt existiert.

### Start und Fortsetzung

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

Der Lauf ist fortsetzbar: Bereits erfolgreich abgerufene Posts werden bei späteren normalen Läufen nicht erneut angefragt. Fehlgeschlagene API-Abrufe bleiben für einen späteren Versuch offen. Nicht mehr abrufbare Posts (HTTP 404) werden als `not_found` markiert.

Für einen begrenzten Testlauf:

```bash
python sync_mysql.py --api-limit 10
```

Alle Posts erneut von der API abrufen:

```bash
python sync_mysql.py --refresh-api
```

Optional kann ein anderer SQLite-Pfad angegeben werden:

```bash
python sync_mysql.py --sqlite /pfad/e6ai_archive.sqlite3
```

Die MySQL-Zugangsdaten werden ausschließlich aus Umgebungsvariablen gelesen. Der Nextcloud-Stammpfad ist absichtlich fest im Skript hinterlegt und nicht konfigurierbar.
