#!/usr/bin/env python3
"""
import_local_reddit_data.py

Recursively scans a directory for Reddit data files and imports them into MySQL.

File formats:
  1) Plain or gzipped SQL dumps:
       sub_YYYY_MM.sql, sub_YYYY_MM.sql.gz
       com_YYYY_MM.sql, com_YYYY_MM.sql.gz
     -> Directly piped into MySQL.

  2) Pushshift JSON (zst-compressed):
       RS_YYYY-MM.zst (submissions)
       RC_YYYY-MM.zst (comments)
     -> Decompress line-by-line, parse JSON, insert in chunks.

Usage example:
  python import_local_reddit_data.py \
      --data-dir /path/to/reddit_dumps \
      --db reddit \
      --host 127.0.0.1 \
      --user myuser \
      --password mypass \
      --skip-done

By default, it will skip any file that already has a ".done" marker. Remove
--skip-done if you want to reimport.

You can also omit --user/--password if your ~/.my.cnf handles credentials.
"""

import os
import re
import sys
import argparse
import subprocess
import json
import datetime
import pymysql

# ---------------------------------------------------------------------------
#                      HELPER FUNCTIONS / CONSTANTS
# ---------------------------------------------------------------------------

CHUNK_SIZE = 1000         # how many rows to insert at once for the JSON .zst
MEMORY_LIMIT = "2048MB"   # zstd memory usage limit, adjust if needed

def get_sql_connection(args):
    """
    Returns a PyMySQL connection, possibly using command-line args or fallback.
    If user or password is omitted, it might use ~/.my.cnf if configured server-side.
    """
    return pymysql.connect(
        host=args.host,
        user=args.user if args.user else None,
        password=args.password if args.password else None,
        database=args.db,
        charset='utf8mb4',
        autocommit=False
    )

def create_table_if_needed(cursor, table_name, is_comment):
    """
    Create the table if it doesn't exist. Adjust the DDL as needed for your columns or 
    use a 'CREATE TABLE ... LIKE your_template_table' approach.
    """
    if is_comment:
        # Comments table
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
          `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
          `message_id` VARCHAR(20),
          `user_id` VARCHAR(255),
          `message` TEXT,
          `created_utc` DATETIME,
          `subreddit` VARCHAR(255),
          INDEX(message_id),
          INDEX(subreddit)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    else:
        # Submissions table
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
          `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
          `message_id` VARCHAR(20),
          `user_id` VARCHAR(255),
          `message` TEXT,
          `created_utc` DATETIME,
          `subreddit` VARCHAR(255),
          INDEX(message_id),
          INDEX(subreddit)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    cursor.execute(create_sql)

# ---------------------------------------------------------------------------
#                      MAIN IMPORT LOGIC
# ---------------------------------------------------------------------------

def import_sql_file(sql_path, args):
    """
    Imports a plain .sql file directly into MySQL by piping:
      mysql db < file.sql
    """
    print(f"[INFO] Restoring SQL dump: {sql_path}")
    done_file = sql_path + ".done"
    if args.skip_done and os.path.exists(done_file):
        print(f"  -> {done_file} exists, skipping re-import.")
        return

    cmd = ["mysql"]
    # Add credentials if provided
    if args.user:
        cmd += ["-u", args.user]
    if args.password:
        # WARNING: -p'mypass' can appear in ps output.
        # Safer to rely on ~/.my.cnf or environment if possible.
        cmd += [f"-p{args.password}"]
    cmd += [f"-h{args.host}"]
    cmd += [args.db]

    with open(sql_path, "rb") as fin:
        proc = subprocess.run(cmd, stdin=fin)

    if proc.returncode == 0:
        print(f"  -> Successfully imported {sql_path}")
        with open(done_file, "w") as f:
            f.write("done\n")
    else:
        print(f"[ERROR] Could not import {sql_path}. Return code: {proc.returncode}")

def import_gz_file(gz_path, args):
    """
    Decompress .sql.gz and pipe into MySQL:
      zcat file.sql.gz | mysql ...
    """
    print(f"[INFO] Restoring gzip-compressed SQL: {gz_path}")
    done_file = gz_path + ".done"
    if args.skip_done and os.path.exists(done_file):
        print(f"  -> {done_file} exists, skipping re-import.")
        return

    zcat_cmd = ["zcat", gz_path]
    mysql_cmd = ["mysql"]
    # Add credentials if provided
    if args.user:
        mysql_cmd += ["-u", args.user]
    if args.password:
        mysql_cmd += [f"-p{args.password}"]
    mysql_cmd += [f"-h{args.host}", args.db]

    zcat_proc = subprocess.Popen(zcat_cmd, stdout=subprocess.PIPE)
    mysql_proc = subprocess.Popen(mysql_cmd, stdin=zcat_proc.stdout)
    zcat_proc.stdout.close()
    mysql_proc.communicate()

    zcat_rc = zcat_proc.wait()
    mysql_rc = mysql_proc.returncode
    if zcat_rc == 0 and mysql_rc == 0:
        print(f"  -> Successfully imported {gz_path}")
        with open(done_file, "w") as f:
            f.write("done\n")
    else:
        print(f"[ERROR] Could not import {gz_path}. Return codes: zcat={zcat_rc}, mysql={mysql_rc}")

def import_zst_file(zst_path, args):
    """
    For pushshift .zst with JSON lines:
      - If file name matches RC_YYYY-MM.zst => comments
      - If file name matches RS_YYYY-MM.zst => submissions
    Decompress line-by-line, parse JSON, insert in chunks.
    """
    base_name = os.path.basename(zst_path)
    done_file = zst_path + ".done"
    if args.skip_done and os.path.exists(done_file):
        print(f"  -> {done_file} exists, skipping re-import.")
        return

    # Patterns: RC_YYYY-MM.zst or RS_YYYY-MM.zst
    m = re.match(r'^(RC|RS)_(\d{4})-(\d{2})\.zst$', base_name)
    if not m:
        print(f"[WARN] {base_name} does not match RC_YYYY-MM.zst or RS_YYYY-MM.zst. Skipping.")
        return

    prefix, year_str, month_str = m.groups()
    table_name = f"{'com' if prefix=='RC' else 'sub'}_{year_str}_{month_str}"
    is_comment = (prefix == "RC")

    print(f"[INFO] Parsing {zst_path} => Table: {table_name}")
    conn = get_sql_connection(args)
    cur = conn.cursor()

    # Create table if needed
    create_table_if_needed(cur, table_name, is_comment=is_comment)
    conn.commit()

    # Our insert statement
    insert_sql = f"""
    INSERT INTO `{table_name}`
    (message_id, user_id, message, created_utc, subreddit)
    VALUES (%s, %s, %s, %s, %s)
    """

    # Decompress with zstd
    # e.g. zstd -dc --memory=2048MB <file>
    decompress_cmd = ["zstd", "-dc", f"--memory={MEMORY_LIMIT}", zst_path]
    proc = subprocess.Popen(decompress_cmd, stdout=subprocess.PIPE, text=True, bufsize=1)

    chunk = []
    row_count = 0

    for line in proc.stdout:
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue

        # Extract fields
        if is_comment:
            msg_id = data.get("id")
            user = data.get("author")
            msg = data.get("body")
            c_utc = data.get("created_utc")
            subr = data.get("subreddit")
        else:
            msg_id = data.get("id")
            user = data.get("author")
            msg = data.get("selftext")
            c_utc = data.get("created_utc")
            subr = data.get("subreddit")

        # Convert timestamp
        if c_utc is not None:
            dt_str = datetime.datetime.utcfromtimestamp(c_utc).strftime('%Y-%m-%d %H:%M:%S')
        else:
            dt_str = None

        chunk.append((msg_id, user, msg, dt_str, subr))
        if len(chunk) >= CHUNK_SIZE:
            cur.executemany(insert_sql, chunk)
            conn.commit()
            row_count += len(chunk)
            chunk = []

    # leftover
    if chunk:
        cur.executemany(insert_sql, chunk)
        conn.commit()
        row_count += len(chunk)

    proc.stdout.close()
    proc.wait()

    cur.close()
    conn.close()

    if proc.returncode == 0:
        print(f"[INFO] Inserted ~{row_count} rows into {table_name}")
        with open(done_file, "w") as f:
            f.write("done\n")
    else:
        print(f"[ERROR] zstd exit code={proc.returncode}. Some data may have been inserted.")


# ---------------------------------------------------------------------------
#                      FILE-TYPE DETECTION
# ---------------------------------------------------------------------------

def is_sql_file(fname):
    """
    Returns True if it looks like sub_YYYY_MM.sql or com_YYYY_MM.sql
    (uncompressed).
    """
    # Example: sub_2006_01.sql or com_2023_03.sql
    pattern_sql = re.compile(r'^(sub|com)_(\d{4})_(\d{2})\.sql$')
    return bool(pattern_sql.match(fname))

def is_sql_gz_file(fname):
    """
    Returns True if it looks like sub_YYYY_MM.sql.gz or com_YYYY_MM.sql.gz
    """
    pattern_sql_gz = re.compile(r'^(sub|com)_(\d{4})_(\d{2})\.sql\.gz$')
    return bool(pattern_sql_gz.match(fname))

def is_zst_file(fname):
    """
    Returns True if it looks like RC_YYYY-MM.zst or RS_YYYY-MM.zst
    """
    # Example: RC_2020-10.zst, RS_2021-05.zst
    pattern_zst = re.compile(r'^(RC|RS)_(\d{4})-(\d{2})\.zst$')
    return bool(pattern_zst.match(fname))


# ---------------------------------------------------------------------------
#                          MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Recursively import local Reddit data files (.sql, .sql.gz, or .zst) into MySQL."
    )
    parser.add_argument("--data-dir", default=".", help="Path to top-level directory containing reddit data files.")
    parser.add_argument("--db", default="reddit", help="MySQL database name.")
    parser.add_argument("--host", default="127.0.0.1", help="MySQL host.")
    parser.add_argument("--user", default=None, help="MySQL user (omit if relying on ~/.my.cnf).")
    parser.add_argument("--password", default=None, help="MySQL password (omit if using ~/.my.cnf).")
    parser.add_argument("--skip-done", action="store_true", help="Skip files that have a .done marker.")
    args = parser.parse_args()

    print(f"[INFO] Scanning directory: {args.data_dir}")
    for root, dirs, files in os.walk(args.data_dir):
        for fname in sorted(files):
            if fname.startswith('.'):
                continue  # skip hidden or partial

            full_path = os.path.join(root, fname)

            # SQL dumps
            if is_sql_file(fname):
                import_sql_file(full_path, args)
            elif is_sql_gz_file(fname):
                import_gz_file(full_path, args)
            # Pushshift JSON
            elif is_zst_file(fname):
                import_zst_file(full_path, args)
            else:
                # Not recognized as one of our known patterns
                # You can uncomment this line if you want a warning:
                # print(f"[SKIP] Not a recognized reddit data file: {full_path}")
                pass


if __name__ == "__main__":
    main()