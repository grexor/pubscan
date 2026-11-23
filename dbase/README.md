# pubScan server: build and run the Docker container

The container runs Apache + mod_wsgi and mounts the repository root at `/var/www/site`.
Everything in this folder is deployment only; the application code is in `../pubscan` and `../web`.

## When do I need to rebuild?

| Change | Action |
|---|---|
| `pubscan/index.py` | nothing: mod_wsgi restarts the daemon on the next request after the file changes |
| `pubscan/graph.py`, `pubscan/topics.py` | `touch pubscan/index.py`: the modules are loaded when the daemon starts |
| `web/*` (HTML, CSS, JS) | nothing: served directly from the mounted folder |
| `dbase/requirements.txt` | restart the container (`./run.sh`); pip runs at startup |
| `dbase/Dockerfile`, `dbase/site.conf` | rebuild the image, then restart (`./build.sh && ./run.sh`) |
| new data in `db/` (`graph/`, `topics/`, `names.db`) | swap the folders (see below), then `touch pubscan/index.py` so the daemon reopens the files; a restart of the container (`./run.sh`) does the same and reads them into the page cache |

## Rebuild and restart

```bash
cd dbase
./build.sh      # builds image pubscan3:latest from dbase/Dockerfile (a few minutes the first time)
./run.sh        # removes the old pubscan3 container, recreates logs/ and run/, starts the new one on port 8008
```

`run.sh` deletes `logs/` (including `search.log`) and `run/` before starting. Copy `logs/search.log` elsewhere first if you want to keep it.

Startup sequence inside the container (see the `CMD` in the Dockerfile): create the venv if missing, install
`requirements.txt`, install `../pubscan` in editable mode, start a background `cat` of `db/*.db`, `db/graph/*.npy`
and `db/topics/*.npy` to warm the OS page cache (a few minutes for 33 GB on this host), then start Apache in the foreground.

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

## Data files

The server reads three things from `db/`, all built by `openalex/update.sh` (into `openalex/`):

| Path | Size | Content | Built by |
|---|---|---|---|
| `db/graph/` | 12.4 GB | authors, who wrote what, years, ids and the compressed titles (memory-mapped arrays) | `openalex/db_graph.py` |
| `db/topics/` | 18.3 GB | publications per author and topic, in both directions (memory-mapped arrays) | `openalex/db_topics.py` |
| `db/names.db` | 1.8 GB | SQLite FTS5 index of the author names for the search box | `openalex/db_names.py` |

There is no database of publications any more: networks and publication lists come from the arrays in `db/graph/`.
Author names are those of the ORCID records (`openalex/orcid_records.py` reads them from the ORCID Public Data File).
The three belong together: `topics/` and `names.db` refer to the authors of the graph they were built from
(author id = position in `graph/orcids.npy`), so always replace all three with the output of one build, never one alone.
`graph/meta.json` records the source file, the build time and the counts; the page header shows the source date.

The data folder is the folder of `pubscan_DB_names` (set in the Dockerfile); `pubscan_GRAPH` and `pubscan_TOPICS`
can point elsewhere. Without `graph/` the server answers network requests with an error; without `topics/` only the
topic search is missing.

### Leaving out a wrong attribution

OpenAlex sometimes puts a paper of a namesake on an author's list. Add a line to `openalex/exclusions.tab`:
the OpenAlex work id, the ORCID iD and a note, separated by tabs or blanks. The server looks at the file with
the next request (at most every ten seconds) and from then on answers as if the author were not on that
publication: no restart and no rebuild. `openalex/db_graph.py` reads the same file, so the next build is clean too
(until then the topic counts of the author still include the publication).

### Putting a new build into service

The server opens the files read-only and keeps them mapped, so never change them in place: build next to the
served data, then rename.

```bash
cd /home/gregor/pubscan3
# the new build is expected in db/graph.new, db/topics.new and db/names.new.db
mkdir -p db/previous
mv db/graph db/previous/graph && mv db/graph.new db/graph
mv db/topics db/previous/topics && mv db/topics.new db/topics
mv db/names.db db/previous/names.db && mv db/names.new.db db/names.db
touch pubscan/index.py                                      # the daemon restarts on the next request and maps the new files
cat db/*.db db/graph/*.npy db/topics/*.npy > /dev/null      # read them into the page cache (hard disks: cold reads are slow)
# after checking the site: rm -r db/previous
```

Keep the previous build in a subfolder such as `db/previous/` until the new one is checked: the warm-up at container
start reads `db/*.db`, `db/graph/*.npy` and `db/topics/*.npy`, so old files left directly in `db/` would be read into
memory as well.

## Security

The search box is public, so the backend treats every parameter as hostile (see the notes at the top of
`pubscan/index.py`):

* all parameters come from the query string; request bodies are never read and POST/PUT get `405`
* actions are dispatched through an explicit table (`version`, `get_update_date`, `get_author_network`,
  `get_author_publications`, `author_suggest`, `suggest`, `topic_suggest`, `topic_authors`, `author_topics`);
  anything else is `404`
* the search log is written by Python (no shell); the client address is the last `X-Forwarded-For` entry,
  which the host proxy appends (`RequestHeader unset X-Forwarded-For` in `apache/pubscan.org.ssl.conf` drops
  client-sent values); it rotates at 20 MB
* input limits: author parameter 200 chars and must end with an ORCID iD, at most 300 publications per
  author in a network or a publication list, autocomplete 100 chars / 8 tokens with every FTS5 token quoted
* at most 6 network builds run at the same time, 2 per client address; further requests get `503` and the
  page shows the message (`MAX_CONCURRENT_*` in `pubscan/index.py`)
* Apache (`site.conf`): no directory listings, only `web/` and `pubscan/index.py` are reachable, request
  bodies capped at 64 KB, `nosniff` / `X-Frame-Options` / `Referrer-Policy` headers, access and error logs
  rotated by size (`error.log`, `error.log.1`, ...)
* the container only listens on `127.0.0.1:8008` (`run.sh`), so every request passes the host proxy
* the frontend HTML-escapes everything it renders from the server (OpenAlex author names and titles can
  contain markup, e.g. `<sup>`)

Not covered here: per-IP request rate limiting (use fail2ban or mod_qos on the host proxy if floods become a
problem) and mounting the repository read-only inside the container.
