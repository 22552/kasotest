#!/usr/bin/env python3
import argparse
import datetime as dt
import gzip
import io
import json
import sqlite3
from pathlib import Path

SCHEMA = r'''
PRAGMA page_size=4096;
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
PRAGMA temp_store=MEMORY;
PRAGMA locking_mode=EXCLUSIVE;
PRAGMA cache_size=-262144;

CREATE TABLE comments (
    id        INTEGER PRIMARY KEY,
    parent_id INTEGER,
    is_reply  INTEGER NOT NULL CHECK (is_reply IN (0, 1)),
    user      TEXT NOT NULL,
    user_id   INTEGER,
    datetime  TEXT NOT NULL,
    content   TEXT NOT NULL
);
'''

INDEXES = r'''
CREATE INDEX idx_comments_user ON comments(user COLLATE NOCASE);
CREATE INDEX idx_comments_user_id ON comments(user_id);
CREATE INDEX idx_comments_datetime ON comments(datetime DESC);
CREATE INDEX idx_comments_parent_id ON comments(parent_id);
CREATE INDEX idx_comments_parent_datetime ON comments(parent_id, datetime);
CREATE INDEX idx_comments_is_reply_datetime ON comments(is_reply, datetime DESC);

CREATE VIRTUAL TABLE comments_fts USING fts5(
    content,
    user,
    content='comments',
    content_rowid='id',
    tokenize='unicode61 remove_diacritics 2'
);
INSERT INTO comments_fts(comments_fts) VALUES('rebuild');

CREATE TRIGGER comments_ai AFTER INSERT ON comments BEGIN
  INSERT INTO comments_fts(rowid, content, user) VALUES (new.id, new.content, new.user);
END;
CREATE TRIGGER comments_ad AFTER DELETE ON comments BEGIN
  INSERT INTO comments_fts(comments_fts, rowid, content, user)
  VALUES('delete', old.id, old.content, old.user);
END;
CREATE TRIGGER comments_au AFTER UPDATE ON comments BEGIN
  INSERT INTO comments_fts(comments_fts, rowid, content, user)
  VALUES('delete', old.id, old.content, old.user);
  INSERT INTO comments_fts(rowid, content, user) VALUES (new.id, new.content, new.user);
END;

CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
'''


def open_text(path: Path):
    raw = gzip.open(path, 'rb') if path.suffix == '.gz' else path.open('rb')
    return io.TextIOWrapper(raw, encoding='utf-8')


def iter_json_array(fp, chunk_size=1024 * 1024):
    dec = json.JSONDecoder()
    buf = ''
    pos = 0
    started = False

    def fill():
        nonlocal buf, pos
        more = fp.read(chunk_size)
        if pos:
            buf = buf[pos:] + more
            pos = 0
        else:
            buf += more
        return bool(more)

    fill()
    while True:
        while True:
            while pos < len(buf) and buf[pos].isspace():
                pos += 1
            if pos < len(buf):
                break
            if not fill():
                raise ValueError('unexpected EOF')

        if not started:
            if buf[pos] != '[':
                raise ValueError('top-level JSON must be an array')
            pos += 1
            started = True
            continue

        while True:
            while pos < len(buf) and buf[pos].isspace():
                pos += 1
            if pos < len(buf):
                break
            if not fill():
                raise ValueError('unexpected EOF in array')

        if buf[pos] == ']':
            return

        while True:
            try:
                item, end = dec.raw_decode(buf, pos)
                pos = end
                break
            except json.JSONDecodeError:
                if not fill():
                    raise
        yield item

        while True:
            while pos < len(buf) and buf[pos].isspace():
                pos += 1
            if pos < len(buf):
                break
            if not fill():
                raise ValueError('unexpected EOF after item')
        if buf[pos] == ',':
            pos += 1
            continue
        if buf[pos] == ']':
            return
        raise ValueError(f'expected comma or ], got {buf[pos]!r}')


def normalize(item, parent_id=None):
    return (
        int(item['id']),
        parent_id,
        0 if parent_id is None else 1,
        str(item.get('user') or ''),
        int(item.get('user_id') or 0),
        str(item.get('datetime') or ''),
        str(item.get('content') or ''),
    )


def main():
    ap = argparse.ArgumentParser(description='Convert scraper4 JSON/JSON.GZ to indexed SQLite + FTS5')
    ap.add_argument('input')
    ap.add_argument('output')
    args = ap.parse_args()

    src = Path(args.input)
    out = Path(args.output)
    out.unlink(missing_ok=True)

    db = sqlite3.connect(out)
    try:
        db.executescript(SCHEMA)
        insert_sql = '''INSERT OR IGNORE INTO comments
            (id, parent_id, is_reply, user, user_id, datetime, content)
            VALUES (?, ?, ?, ?, ?, ?, ?)'''
        top_count = reply_count = duplicate_count = 0
        batch = []
        batch_size = 5000

        def flush_batch():
            nonlocal duplicate_count
            if not batch:
                return
            before = db.total_changes
            db.executemany(insert_sql, batch)
            duplicate_count += len(batch) - (db.total_changes - before)
            batch.clear()

        db.execute('BEGIN')
        with open_text(src) as fp:
            for top in iter_json_array(fp):
                parent = int(top['id'])
                batch.append(normalize(top))
                top_count += 1
                replies = top.get('replies') or []
                for reply in replies:
                    batch.append(normalize(reply, parent))
                    reply_count += 1
                if len(batch) >= batch_size:
                    flush_batch()
                total = top_count + reply_count
                if total and total % 100000 < len(replies) + 1:
                    print(f'[load] top={top_count:,} replies={reply_count:,} total={total:,}', flush=True)
        flush_batch()
        db.commit()

        total = top_count + reply_count
        print(f'[load] done top={top_count:,} replies={reply_count:,} total={total:,} duplicates={duplicate_count:,}', flush=True)
        print('[index] building B-tree indexes + FTS5...', flush=True)
        db.executescript(INDEXES)

        meta = {
            'source_file': src.name,
            'built_at_utc': dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00', 'Z'),
            'top_level_count': str(top_count),
            'reply_count': str(reply_count),
            'input_item_count': str(total),
            'stored_row_count': str(db.execute('SELECT COUNT(*) FROM comments').fetchone()[0]),
            'duplicate_id_count': str(duplicate_count),
            'schema_version': '1',
        }
        db.executemany('INSERT INTO meta(key, value) VALUES (?, ?)', meta.items())
        db.execute('PRAGMA user_version=1')
        db.execute('ANALYZE')
        db.execute('PRAGMA optimize')
        db.commit()

        print('[check] quick_check:', db.execute('PRAGMA quick_check').fetchone()[0])
        print('[check] rows:', db.execute('SELECT COUNT(*) FROM comments').fetchone()[0])
        print('[check] fts rows:', db.execute('SELECT COUNT(*) FROM comments_fts').fetchone()[0])
    finally:
        db.close()

    print(f'[done] {out} ({out.stat().st_size / 1024 / 1024:.1f} MiB)')


if __name__ == '__main__':
    main()
