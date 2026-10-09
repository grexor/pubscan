# pubScan server: build and run the Docker container

The container runs Apache + mod_wsgi and mounts the repository root at `/var/www/site`.
Everything in this folder is deployment only; the application code is in `../pubscan` and `../web`.

## When do I need to rebuild?

| Change | Action |
|---|---|
| `pubscan/index.py` | nothing: mod_wsgi restarts the daemon on the next request after the file changes |
| `web/*` (HTML, CSS, JS) | nothing: served directly from the mounted folder |
| `dbase/requirements.txt` | restart the container (`./run.sh`); pip runs at startup |
| `dbase/Dockerfile`, `dbase/site.conf` | rebuild the image, then restart (`./build.sh && ./run.sh`) |
| new `db/pubscan.db` or `db/names.db` | restart the container (`./run.sh`) so the page-cache warm-up reads the new files |

## Rebuild and restart

```bash
cd dbase
./build.sh      # builds image pubscan3:latest from dbase/Dockerfile (a few minutes the first time)
./run.sh        # removes the old pubscan3 container, recreates logs/ and run/, starts the new one on port 8008
```

`run.sh` deletes `logs/` (including `search.log`) and `run/` before starting. Copy `logs/search.log` elsewhere first if you want to keep it.

Startup sequence inside the container (see the `CMD` in the Dockerfile): create the venv if missing, install
`requirements.txt`, install `../pubscan` in editable mode, start a background `cat` of `db/*.db` to warm the
OS page cache (about 40 s for 14 GB on this host), then start Apache in the foreground.

## Check that it works

```bash
docker ps --filter name=pubscan3                         # STATUS should be "Up"
docker logs pubscan3 | tail -5                           # expect "docker server apache+mod_wsgi is running"
curl -s 'http://127.0.0.1:8008/gw/index.py?action=version'
curl -s 'http://127.0.0.1:8008/gw/index.py?action=get_author_network&author=0009-0001-7888-6687&response_type=json' | tail -1
tail -f logs/error.log                                   # Python tracebacks from mod_wsgi land here
```

The public site at pubscan.org is a host Apache reverse proxy to port 8008 (`dbase/apache/*.conf`); it needs no change
when the container is rebuilt.

## Databases

The container reads `db/pubscan.db` and `db/names.db` (paths set by `pubscan_DB` and `pubscan_DB_names` in the
Dockerfile). The databases are built by `openalex/update.sh` into `openalex/`; copy the finished files into `db/`
and restart the container.

The container opens the files with `immutable=1`, so never modify a database in place while it is being served:
build or copy a new file, then swap and restart.

### Deploying the slim database (authors_all column removed)

`openalex/pubscan_slim.db` is the current database rewritten without the never-used `authors_all` column
(9.4 GB instead of 13.7 GB; same rows, server output verified identical). `openalex/db_publications.py` no longer
produces that column, so future builds are slim by default. To put the slim file into service:

```bash
cd /home/gregor/pubscan3
sqlite3 "file:openalex/pubscan_slim.db?immutable=1" "PRAGMA quick_check;"   # expect: ok
cp openalex/pubscan_slim.db db/pubscan.db.new && mv db/pubscan.db db/pubscan.db.old && mv db/pubscan.db.new db/pubscan.db
cd dbase && ./run.sh            # restart so the container reopens the file and warms the cache
# after checking the site works: rm ../db/pubscan.db.old
```
