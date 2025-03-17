# Markdown

---

**NOTE (2023)**:

**Pushshift’s public API and file hosting have changed significantly.**

The scripts that directly download new monthly .zst files from [files.pushshift.io](https://files.pushshift.io/) are **deprecated** and will not work anymore. 

This repo has **two main sets of scripts**:

1.	**Legacy Scripts** (deprecated):

•	[download_pushshift_data.py](https://www.notion.so/src/pushshift/download_pushshift_data.py): Used to download .zst files from Pushshift for a given time range (API no longer public).

•	[pushshift_download_and_insert.py](https://www.notion.so/src/pushshift/pushshift_download_and_insert.py): Combined download + decompression + MySQL insertion from live Pushshift.

2.	**Current / Local Data Scripts** (still functional):

•	[import_local_reddit_data.py](https://www.notion.so/import_local_reddit_data.py): Recursively scans a local directory of Reddit data files (.sql, .sql.gz, .zst), then **imports them into MySQL**.

•	**.sql / .sql.gz** are assumed to be raw SQL dumps.

•	**.zst** files are assumed to be **Pushshift-style JSON** and will be parsed line-by-line before insertion.

---

**Table of Contents**

1.	[Repository Structure](https://www.notion.so/Markdown-9bf480aeb83f4c31b0abeed8f0cbc82a?pvs=21)

2.	[Requirements](https://www.notion.so/Markdown-9bf480aeb83f4c31b0abeed8f0cbc82a?pvs=21)

3.	[Usage (Local Import Script)](https://www.notion.so/Markdown-9bf480aeb83f4c31b0abeed8f0cbc82a?pvs=21)

4.	[Legacy Scripts (Deprecated)](https://www.notion.so/Markdown-9bf480aeb83f4c31b0abeed8f0cbc82a?pvs=21)

5.	[FAQ](https://www.notion.so/Markdown-9bf480aeb83f4c31b0abeed8f0cbc82a?pvs=21)

---

**Repository Structure**

A rough outline of key files:

```
pushshift-toolkit/
├── README.md                  <- You are here
├── import_local_reddit_data.py
├── src/
│   └── pushshift/
│       ├── download_pushshift_data.py        (old, no longer functional)
│       └── pushshift_download_and_insert.py  (old, no longer functional)
└── ...
```

•	**import_local_reddit_data.py** – The main updated script that can import locally stored SQL and/or JSON zst dumps into MySQL.

•	**download_pushshift_data.py** and **pushshift_download_and_insert.py** – Original scripts for downloading from the now-changed Pushshift services. Retained here for reference/portfolio.

---

**Requirements**

•	**Python 3.7+** 

•	**PyMySQL** 

•	**zstd** CLI tool

•	**gzip** or **zcat** for .gz

•	**MySQL or MariaDB** server access (local or remote)

•	**(Optional)** A ~/.my.cnf or --defaults-extra-file usage so you don’t expose password in CLI

---

**Usage (Local Import Script)**

**Overview**

import_local_reddit_data.py searches a directory recursively for:

1.	sub_YYYY_MM.sql or com_YYYY_MM.sql (plain .sql)

2.	sub_YYYY_MM.sql.gz or com_YYYY_MM.sql.gz (gzipped .sql)

3.	RS_YYYY-MM.zst or RC_YYYY-MM.zst (Pushshift JSON data)

Then it imports them into your chosen MySQL/MariaDB database:

•	**submissions** get inserted into tables named sub_YYYY_MM.

•	**comments** get inserted into tables named com_YYYY_MM.

**.sql** and **.sql.gz** are assumed to already have valid SQL (like CREATE TABLE... INSERT ...).

**.zst** is assumed to be JSON lines from old Pushshift dumps, so the script does the chunk-by-chunk JSON parse.

**Example Command**

```
python import_local_reddit_data.py \
    --data-dir /data/reddit_dumps \
    --db reddit \
    --host 127.0.0.1 \
    --user myuser \
    --password mypass \
    --skip-done
```

**Arguments**:

	`--data-dir /path/to/data`

Directory that holds your .sql, .sql.gz, and/or .zst files (recursively).

	`--db reddit`

The MySQL database name you want to import into.

	`--user myuser / --password mypass`

Credentials for MySQL (if not using .my.cnf).

	`--host 127.0.0.1`

Database host (default = 127.0.0.1).

	`--skip-done`

If present, the script will skip any file that already has a .done marker (meaning it was successfully imported in a previous run).

**Note**: You must have a running MySQL/MariaDB server, and your user must have privileges to CREATE TABLE and INSERT on the specified database.

---

**Legacy Scripts (Deprecated)**

**download_pushshift_data.py**

**Status**: No longer functional for new data (Pushshift changed their open data hosting).

**Purpose**: Originally used to download .zst monthly dumps from files.pushshift.io for a given date range.

**pushshift_download_and_insert.py**

**Status**: Also no longer functional for new data.

**Purpose**: Combined the download step with immediate decompression and MySQL insertion.

These scripts remain in the repository to showcase:

•	**Examples** of date iteration, large file handling, chunked inserts, etc.

•	**Your** development experience in building a pipeline for massive data ingestion.

If you already have older .zst files downloaded from that era, you can still adapt or reference these scripts. Otherwise, they will not function because the original endpoints are gone.

---

**FAQ**

1.	**Q: Can I still get monthly Reddit dumps from Pushshift?**

**A:** Unfortunately, as of 2023, Pushshift’s free data endpoints have been deprecated for general/academic use. Check their official channels or see if you already have existing .zst archives.

2.	**Q: The script found a .zst but gave JSON parse errors.**

**A:** Make sure it’s genuinely a **Pushshift JSON** .zst. If the file is actually an SQL dump in .zst form, you can decompress it separately and run mysql < file.sql.

3.	**Q: Where are the table schemas for sub_YYYY_MM or com_YYYY_MM?**

**A:** By default, import_local_reddit_data.py attempts to CREATE TABLE IF NOT EXISTS with minimal columns (message_id, user_id, message, created_utc, subreddit). If you have more fields or a template table, adjust the script to match your real schema.

---