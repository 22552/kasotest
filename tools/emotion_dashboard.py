from pathlib import Path
from urllib.parse import urljoin
import hashlib
import shutil
import sqlite3
import subprocess
import time

import gradio as gr
import numpy as np
import pandas as pd
import plotly.express as px
import requests
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

NEW_BASE = "https://raw.githubusercontent.com/22552/kasotest/main/"
OLD_DB_BLOB = "https://git.sr.ht/~kasosuta/dai2db/blob/ce1e6788c9461a550197e1027df59e575450eac8/comment.db.zst"
OLD_DB_URL = OLD_DB_BLOB + "?raw=true"
WORK = Path("/content/dai2db-diff")
WORK.mkdir(exist_ok=True)
CMP = WORK / "compare.db"
MODEL_NAME = "neuralnaut/deberta-wrime-emotions"

EMOTIONS = ["joy", "sadness", "anticipation", "surprise", "anger", "fear", "disgust", "trust"]
EMOTION_JA = {
    "喜び": "joy",
    "悲しみ": "sadness",
    "期待": "anticipation",
    "驚き": "surprise",
    "怒り": "anger",
    "恐れ": "fear",
    "嫌悪": "disgust",
    "信頼": "trust",
}
SOURCE_TABLE = {
    "現在": "new_norm",
    "半年前": "old_norm",
    "追加": "added",
    "消滅": "removed",
}

session = requests.Session()
session.headers["User-Agent"] = "kasotest-emotion-dashboard/1"
_tokenizer = None
_model = None
_device = None


def qi(s):
    return '"' + str(s).replace('"', '""') + '"'


def download(url, dst):
    dst = Path(dst)
    if dst.exists() and dst.stat().st_size > 0:
        print(f"[download] reuse {dst.name}: {dst.stat().st_size / 2**20:.1f} MiB")
        return dst
    tmp = Path(str(dst) + ".tmp")
    tmp.unlink(missing_ok=True)
    with session.get(url, stream=True, timeout=240, allow_redirects=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length") or 0)
        done = 0
        with tmp.open("wb") as f:
            for chunk in r.iter_content(4 * 1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                done += len(chunk)
                if total:
                    print(f"\r[download] {dst.name}: {done/2**20:.1f}/{total/2**20:.1f} MiB", end="")
    print()
    tmp.replace(dst)
    return dst


def is_sqlite(path):
    try:
        with open(path, "rb") as f:
            return f.read(16) == b"SQLite format 3\x00"
    except FileNotFoundError:
        return False


def is_zstd(path):
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"\x28\xb5\x2f\xfd"
    except FileNotFoundError:
        return False


def zstd_decompress(src, dst):
    src, dst = Path(src), Path(dst)
    if is_sqlite(dst):
        print(f"[zstd] reuse {dst.name}: {dst.stat().st_size / 2**20:.1f} MiB")
        return dst
    dst.unlink(missing_ok=True)
    subprocess.run(["zstd", "-t", "--long=31", str(src)], check=True)
    subprocess.run(["zstd", "-d", "--long=31", "-f", str(src), "-o", str(dst)], check=True)
    return dst


def ensure_raw_dbs():
    new_db = WORK / "new.db"
    if not is_sqlite(new_db):
        p0 = download(urljoin(NEW_BASE, "db.zst.part0"), WORK / "db.zst.part0")
        p1 = download(urljoin(NEW_BASE, "db.zst.part1"), WORK / "db.zst.part1")
        joined = WORK / "new.db.zst"
        with joined.open("wb") as out:
            for p in (p0, p1):
                with p.open("rb") as f:
                    shutil.copyfileobj(f, out)
        if not is_zstd(joined):
            raise RuntimeError("新DBの結合結果がZstdではありません")
        zstd_decompress(joined, new_db)

    old_db = WORK / "old.db"
    if not is_sqlite(old_db):
        old_zst = download(OLD_DB_URL, WORK / "comment.db.zst")
        if not is_zstd(old_zst):
            raise RuntimeError("旧DBの取得結果がZstdではありません")
        zstd_decompress(old_zst, old_db)
    return old_db, new_db


ALIASES = {
    "id": ("id", "comment_id"),
    "user": ("user", "username", "author", "author_name"),
    "datetime": ("datetime", "datetime_created", "created_at", "timestamp", "date"),
    "content": ("content", "text", "body"),
}


def detect_table(path):
    db = sqlite3.connect(path)
    best = None
    try:
        tables = [r[0] for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )]
        for table in tables:
            cols = [r[1] for r in db.execute(f"PRAGMA table_info({qi(table)})")]
            lower = {c.lower(): c for c in cols}
            mapping = {}
            for logical, aliases in ALIASES.items():
                for alias in aliases:
                    if alias in lower:
                        mapping[logical] = lower[alias]
                        break
            if "id" not in mapping or "content" not in mapping:
                continue
            score = (100 if table.lower() == "comments" else 0) + len(mapping) * 10
            if best is None or score > best[0]:
                best = (score, table, mapping)
    finally:
        db.close()
    if best is None:
        raise RuntimeError(f"コメントテーブルを検出できません: {path}")
    return best[1], best[2]


def source_select(schema, table, mapping):
    def col(name, cast):
        if name not in mapping:
            return "NULL"
        return f"CAST({qi(mapping[name])} AS {cast})"
    return (
        "SELECT "
        + col("id", "INTEGER") + ", "
        + col("user", "TEXT") + ", "
        + col("datetime", "TEXT") + ", "
        + col("content", "TEXT")
        + f" FROM {schema}.{qi(table)}"
    )


def ensure_compare_db():
    old_db, new_db = ensure_raw_dbs()
    con = sqlite3.connect(CMP)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA cache_size=-262144")

    existing = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"old_norm", "new_norm"}.issubset(existing):
        print("[db] building normalized comparison tables...")
        old_table, old_map = detect_table(old_db)
        new_table, new_map = detect_table(new_db)
        con.executescript("""
        DROP TABLE IF EXISTS old_norm;
        DROP TABLE IF EXISTS new_norm;
        CREATE TABLE old_norm (id INTEGER PRIMARY KEY, user TEXT, datetime TEXT, content TEXT);
        CREATE TABLE new_norm (id INTEGER PRIMARY KEY, user TEXT, datetime TEXT, content TEXT);
        """)
        con.execute("ATTACH DATABASE ? AS olddb", (str(old_db),))
        con.execute("ATTACH DATABASE ? AS newdb", (str(new_db),))
        con.execute("INSERT OR REPLACE INTO old_norm " + source_select("olddb", old_table, old_map))
        con.execute("INSERT OR REPLACE INTO new_norm " + source_select("newdb", new_table, new_map))
        con.commit()
        con.execute("DETACH DATABASE olddb")
        con.execute("DETACH DATABASE newdb")

    con.executescript("""
    CREATE INDEX IF NOT EXISTS idx_old_user ON old_norm(user COLLATE NOCASE);
    CREATE INDEX IF NOT EXISTS idx_old_datetime ON old_norm(datetime);
    CREATE INDEX IF NOT EXISTS idx_new_user ON new_norm(user COLLATE NOCASE);
    CREATE INDEX IF NOT EXISTS idx_new_datetime ON new_norm(datetime);

    DROP TABLE IF EXISTS added;
    CREATE TABLE added AS
      SELECT n.* FROM new_norm n LEFT JOIN old_norm o ON o.id=n.id WHERE o.id IS NULL;
    CREATE UNIQUE INDEX idx_added_id ON added(id);
    CREATE INDEX idx_added_user ON added(user COLLATE NOCASE);
    CREATE INDEX idx_added_datetime ON added(datetime);

    DROP TABLE IF EXISTS removed;
    CREATE TABLE removed AS
      SELECT o.* FROM old_norm o LEFT JOIN new_norm n ON n.id=o.id WHERE n.id IS NULL;
    CREATE UNIQUE INDEX idx_removed_id ON removed(id);
    CREATE INDEX idx_removed_user ON removed(user COLLATE NOCASE);
    CREATE INDEX idx_removed_datetime ON removed(datetime);

    CREATE TABLE IF NOT EXISTS emotion_scores (
      source TEXT NOT NULL,
      id INTEGER NOT NULL,
      user TEXT,
      datetime TEXT,
      content TEXT,
      joy REAL, sadness REAL, anticipation REAL, surprise REAL,
      anger REAL, fear REAL, disgust REAL, trust REAL,
      PRIMARY KEY(source, id)
    );
    CREATE INDEX IF NOT EXISTS idx_emotion_source_user ON emotion_scores(source, user COLLATE NOCASE);
    """)
    con.commit()
    return con


def load_model():
    global _tokenizer, _model, _device
    if _model is not None:
        return
    _device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print("[model]", MODEL_NAME, "device=", _device)
    _tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    _model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
    _model.to(_device)
    _model.eval()


def infer(texts, batch_size=64):
    load_model()
    out = []
    for i in range(0, len(texts), batch_size):
        batch = [str(x or "") for x in texts[i:i + batch_size]]
        enc = _tokenizer(batch, padding=True, truncation=True, max_length=128, return_tensors="pt")
        enc = {k: v.to(_device) for k, v in enc.items()}
        with torch.inference_mode():
            if _device.type == "cuda":
                with torch.autocast(device_type="cuda", dtype=torch.float16):
                    logits = _model(**enc).logits
            else:
                logits = _model(**enc).logits
        scores = torch.clamp(logits.float() * 3.0, 0.0, 3.0).cpu().numpy()
        out.extend(scores.tolist())
    return out


def score_rows(source, rows, con):
    if not rows:
        return 0
    ids = [int(r[0]) for r in rows]
    existing = set()
    for i in range(0, len(ids), 900):
        chunk = ids[i:i + 900]
        marks = ",".join("?" * len(chunk))
        existing.update(r[0] for r in con.execute(
            f"SELECT id FROM emotion_scores WHERE source=? AND id IN ({marks})",
            [source] + chunk,
        ))
    missing = [r for r in rows if int(r[0]) not in existing]
    if not missing:
        return 0
    scores = infer([r[3] for r in missing])
    payload = []
    for row, sc in zip(missing, scores):
        payload.append((source, int(row[0]), row[1], row[2], row[3], *[float(x) for x in sc]))
    con.executemany(
        """INSERT OR REPLACE INTO emotion_scores
        (source,id,user,datetime,content,joy,sadness,anticipation,surprise,anger,fear,disgust,trust)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        payload,
    )
    con.commit()
    return len(payload)


def table_for(source_label):
    return SOURCE_TABLE[source_label]


def get_user_rows(con, source_label, user, limit):
    table = table_for(source_label)
    return con.execute(
        f"SELECT id,user,datetime,content FROM {qi(table)} WHERE user=? COLLATE NOCASE ORDER BY RANDOM() LIMIT ?",
        (user.strip(), int(limit)),
    ).fetchall()


def activity_frames(con, source_label, user):
    table = table_for(source_label)
    rows = con.execute(
        f"SELECT datetime FROM {qi(table)} WHERE user=? COLLATE NOCASE AND datetime IS NOT NULL",
        (user.strip(),),
    ).fetchall()
    if not rows:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None, None
    dt = pd.to_datetime([r[0] for r in rows], errors="coerce", utc=True)
    dt = pd.Series(dt).dropna().dt.tz_convert("Asia/Tokyo")
    daily = dt.dt.floor("D").value_counts().sort_index().rename_axis("date").reset_index(name="comments")
    hourly = dt.dt.hour.value_counts().reindex(range(24), fill_value=0).rename_axis("hour").reset_index(name="comments")
    weekday_names = ["月", "火", "水", "木", "金", "土", "日"]
    heat = pd.DataFrame({"weekday": dt.dt.weekday, "hour": dt.dt.hour})
    heat = heat.groupby(["weekday", "hour"]).size().unstack(fill_value=0).reindex(index=range(7), columns=range(24), fill_value=0)
    heat.index = weekday_names
    first = dt.min()
    last = dt.max()
    return daily, hourly, heat, first, last


def user_profile(source_label, user, max_comments):
    user = (user or "").strip()
    if not user:
        return "ユーザー名を入力してください。", pd.DataFrame(), None, None, None
    con = sqlite3.connect(CMP)
    try:
        table = table_for(source_label)
        total = con.execute(f"SELECT COUNT(*) FROM {qi(table)} WHERE user=? COLLATE NOCASE", (user,)).fetchone()[0]
        if not total:
            return f"`{user}` のコメントがありません。", pd.DataFrame(), None, None, None
        rows = get_user_rows(con, source_label, user, min(int(max_comments), total))
        added = score_rows(source_label, rows, con)
        means = con.execute(
            "SELECT " + ",".join(f"AVG({e})" for e in EMOTIONS) + " FROM emotion_scores WHERE source=? AND user=? COLLATE NOCASE",
            (source_label, user),
        ).fetchone()
        analyzed = con.execute(
            "SELECT COUNT(*) FROM emotion_scores WHERE source=? AND user=? COLLATE NOCASE",
            (source_label, user),
        ).fetchone()[0]
        profile = pd.DataFrame({"感情": list(EMOTION_JA), "平均スコア(0-3)": [round(float(x or 0), 3) for x in means]})
        daily, hourly, heat, first, last = activity_frames(con, source_label, user)
        daily_fig = px.line(daily, x="date", y="comments", title=f"{user}：日別コメント数") if not daily.empty else None
        hour_fig = px.bar(hourly, x="hour", y="comments", title=f"{user}：時間帯別コメント数（JST）") if not hourly.empty else None
        heat_fig = px.imshow(heat, labels=dict(x="時刻(JST)", y="曜日", color="件数"), title=f"{user}：曜日×時間帯") if not heat.empty else None
        summary = (
            f"### {user} — {source_label}\n"
            f"- コメント総数: **{total:,}**\n"
            f"- 感情分析済み: **{analyzed:,}**（今回追加 {added:,}）\n"
            f"- 活動期間: `{first}` ～ `{last}`\n\n"
            "※感情値は文章表現をモデルが推定したスコアで、本人の心理状態を示すものではありません。"
        )
        return summary, profile, daily_fig, hour_fig, heat_fig
    finally:
        con.close()


def prepare_ranking(source_label, emotion_ja, min_posts, max_users, per_user):
    emotion = EMOTION_JA[emotion_ja]
    con = sqlite3.connect(CMP)
    try:
        table = table_for(source_label)
        users = con.execute(
            f"""SELECT user, COUNT(*) AS n FROM {qi(table)}
            WHERE user IS NOT NULL AND user<>''
            GROUP BY user HAVING n>=? ORDER BY n DESC LIMIT ?""",
            (int(min_posts), int(max_users)),
        ).fetchall()
        rows = []
        for user, n in users:
            rows.extend(con.execute(
                f"SELECT id,user,datetime,content FROM {qi(table)} WHERE user=? COLLATE NOCASE ORDER BY RANDOM() LIMIT ?",
                (user, min(int(per_user), int(n))),
            ).fetchall())
        new_count = score_rows(source_label, rows, con)
        df = pd.read_sql_query(
            f"""SELECT user, COUNT(*) AS analyzed, ROUND(AVG({qi(emotion)}),3) AS score
            FROM emotion_scores WHERE source=?
            GROUP BY user HAVING analyzed>=?
            ORDER BY score DESC LIMIT 100""",
            con,
            params=(source_label, min(5, int(per_user))),
        )
        fig = px.bar(df.head(30), x="user", y="score", title=f"{emotion_ja} 平均スコア上位（0-3）") if not df.empty else None
        note = f"対象ユーザー {len(users):,}人 / 今回新規推論 {new_count:,}コメント。ランキングは分析済みコメントの平均。"
        return note, df, fig
    finally:
        con.close()


def filter_comments(source_label, emotion_ja, threshold, user, limit):
    emotion = EMOTION_JA[emotion_ja]
    con = sqlite3.connect(CMP)
    try:
        where = ["source=?", f"{qi(emotion)}>=?"]
        params = [source_label, float(threshold)]
        if (user or "").strip():
            where.append("user=? COLLATE NOCASE")
            params.append(user.strip())
        params.append(int(limit))
        return pd.read_sql_query(
            f"""SELECT id,user,datetime,content,ROUND({qi(emotion)},3) AS {qi(emotion)}
            FROM emotion_scores WHERE {' AND '.join(where)}
            ORDER BY {qi(emotion)} DESC LIMIT ?""",
            con,
            params=params,
        )
    finally:
        con.close()


print("[setup] DB preparation")
con = ensure_compare_db()
counts = {label: con.execute(f"SELECT COUNT(*) FROM {qi(table)}").fetchone()[0] for label, table in SOURCE_TABLE.items()}
con.close()
print("[ready]", counts)

with gr.Blocks(title="第二プロジェクト 感情・活動ダッシュボード") as demo:
    gr.Markdown(
        "# 第二プロジェクト 感情・活動ダッシュボード\n"
        "8感情は WRIME 系モデルによる**文章表現の推定**です。本人の性格・心理状態の判定には使えません。"
    )

    with gr.Tab("ユーザー分析"):
        with gr.Row():
            u_source = gr.Dropdown(list(SOURCE_TABLE), value="現在", label="対象")
            u_user = gr.Textbox(label="ユーザー名")
            u_max = gr.Slider(20, 3000, value=300, step=20, label="感情分析する最大コメント数")
        u_run = gr.Button("分析")
        u_summary = gr.Markdown()
        u_profile = gr.Dataframe(label="8感情平均", interactive=False)
        u_daily = gr.Plot(label="日別")
        u_hour = gr.Plot(label="時間帯")
        u_heat = gr.Plot(label="曜日×時間帯")
        u_run.click(user_profile, [u_source, u_user, u_max], [u_summary, u_profile, u_daily, u_hour, u_heat])

    with gr.Tab("感情ランキング"):
        with gr.Row():
            r_source = gr.Dropdown(list(SOURCE_TABLE), value="現在", label="対象")
            r_emotion = gr.Dropdown(list(EMOTION_JA), value="喜び", label="感情")
            r_min = gr.Slider(1, 500, value=20, step=1, label="元コメント最低件数")
        with gr.Row():
            r_users = gr.Slider(10, 1000, value=200, step=10, label="分析する上位ユーザー数")
            r_per = gr.Slider(5, 300, value=50, step=5, label="1ユーザーあたりサンプル数")
        r_run = gr.Button("ランキング用キャッシュ作成 / 更新")
        r_note = gr.Markdown()
        r_df = gr.Dataframe(label="ランキング", interactive=False)
        r_plot = gr.Plot()
        r_run.click(prepare_ranking, [r_source, r_emotion, r_min, r_users, r_per], [r_note, r_df, r_plot])

    with gr.Tab("感情フィルター"):
        with gr.Row():
            f_source = gr.Dropdown(list(SOURCE_TABLE), value="現在", label="対象")
            f_emotion = gr.Dropdown(list(EMOTION_JA), value="怒り", label="感情")
            f_threshold = gr.Slider(0, 3, value=1.5, step=0.05, label="最低スコア")
        with gr.Row():
            f_user = gr.Textbox(label="ユーザー名（空欄なら全員）")
            f_limit = gr.Slider(10, 1000, value=100, step=10, label="最大表示件数")
        f_run = gr.Button("フィルター")
        f_df = gr.Dataframe(label="分析済みコメント", interactive=False)
        f_run.click(filter_comments, [f_source, f_emotion, f_threshold, f_user, f_limit], f_df)

print("[launch] Gradio")
demo.queue().launch(share=True, debug=False)
