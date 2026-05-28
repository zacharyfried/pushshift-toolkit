# Pushshift Toolkit

Utilities for importing archived Reddit/Pushshift datasets into MySQL for research workflows.

## Status

This repository is preserved as a portfolio and reference artifact. Pushshift's public API and historical file hosting have changed, so the scripts that downloaded fresh monthly dumps from `files.pushshift.io` are deprecated and should not be expected to work for new data.

The maintained part of this repo is the local importer:

- `import_local_reddit_data.py`: recursively scans a local directory for Reddit data files and imports recognized files into MySQL.

The legacy scripts are retained for reference:

- `src/pushshift/download_pushshift_data.py`: old downloader for monthly `.zst` files.
- `src/pushshift/pushshift_download_and_insert.py`: old download/decompress/import pipeline.

## What It Demonstrates

- Recursive file discovery for large local data archives.
- Handling multiple dump formats: `.sql`, `.sql.gz`, and Pushshift-style `.zst` JSON lines.
- MySQL table creation and chunked inserts for large text datasets.
- Idempotent processing through `.done` marker files.
- Practical data-engineering work around research-scale Reddit datasets.

## Repository Structure

```text
.
|-- import_local_reddit_data.py
|-- src/
|   `-- pushshift/
|       |-- download_pushshift_data.py
|       `-- pushshift_download_and_insert.py
`-- README.md
```

## Requirements

- Python 3.7+
- MySQL or MariaDB
- `PyMySQL`
- `zstd` CLI tool for `.zst`
- `gzip`/`zcat` for `.sql.gz`

Install Python dependency:

```bash
python -m pip install pymysql
```

For database credentials, prefer `~/.my.cnf` or another local credentials mechanism over passing passwords directly on the command line.

## Local Import Usage

`import_local_reddit_data.py` searches a directory recursively for:

- `sub_YYYY_MM.sql` or `com_YYYY_MM.sql`
- `sub_YYYY_MM.sql.gz` or `com_YYYY_MM.sql.gz`
- `RS_YYYY-MM.zst` or `RC_YYYY-MM.zst`

Then it imports recognized files into MySQL:

- submissions go into tables named `sub_YYYY_MM`
- comments go into tables named `com_YYYY_MM`

Example:

```bash
python import_local_reddit_data.py \
    --data-dir /data/reddit_dumps \
    --db reddit \
    --host 127.0.0.1 \
    --user myuser \
    --skip-done
```

Arguments:

- `--data-dir`: top-level directory containing Reddit data files.
- `--db`: MySQL database name.
- `--host`: MySQL host, default `127.0.0.1`.
- `--user`: MySQL user. Omit if relying on local MySQL config.
- `--password`: MySQL password. Prefer omitting this and using `~/.my.cnf`.
- `--skip-done`: skip files that already have a `.done` marker.

## File Format Notes

`.sql` and `.sql.gz` files are assumed to be valid SQL dumps that MySQL can execute directly.

`.zst` files are assumed to be Pushshift-style JSON lines. The importer parses them line by line and inserts a minimal normalized subset of fields:

- `message_id`
- `user_id`
- `message`
- `created_utc`
- `subreddit`

If your local dumps use a different schema, adjust `create_table_if_needed()` and the insert logic in `import_local_reddit_data.py`.

## Deprecated Scripts

The scripts under `src/pushshift/` target historical Pushshift hosting behavior and are kept as implementation references. They show date iteration, monthly file naming, decompression, chunked insertion, and error handling, but the original public endpoints are no longer reliable for new data collection.

## Safety Notes

- Do not commit raw Reddit dumps, database exports, credentials, or failed-insert logs.
- Large imports can create many tables and consume substantial disk space.
- Test with a small month or sample directory before running against a large archive.
- Review database schemas before importing data into an existing research database.
