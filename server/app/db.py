from __future__ import annotations

import os
import json
from pathlib import Path
import sqlite3
import gzip
import csv
import secrets
from typing import Iterable, Optional

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "server" / "data"
ASSETS_DIR = ROOT / "server" / "assets"

# Store uploaded binaries on disk (not in SQLite) to keep app.db small.
ORIGINALS_DIR = DATA_DIR / "originals"
DERIVATIVES_DIR = DATA_DIR / "derivatives"
PUBLIC_THUMBS_DIR = DATA_DIR / "public_thumbs"

DB_PATH = DATA_DIR / "app.db"
QUEUE_DB_PATH = DATA_DIR / "queue.db"

def _sqlite_busy_timeout_ms(*env_names: str) -> int:
    for name in env_names:
        raw = os.getenv(name)
        if raw is None:
            continue
        try:
            return max(0, int(raw or "0"))
        except Exception:
            continue
    return 10000

def _connect_path(path: Path, *, foreign_keys: bool = True, busy_timeout_envs: tuple[str, ...] = ("NAI_IM_SQLITE_BUSY_TIMEOUT_MS",)) -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    if foreign_keys:
        conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    busy_timeout_ms = _sqlite_busy_timeout_ms(*busy_timeout_envs)
    conn.execute(f"PRAGMA busy_timeout = {max(0, busy_timeout_ms)};")
    return conn

def _connect() -> sqlite3.Connection:
    return _connect_path(DB_PATH, foreign_keys=True, busy_timeout_envs=("NAI_IM_SQLITE_BUSY_TIMEOUT_MS",))

def _connect_queue() -> sqlite3.Connection:
    return _connect_path(QUEUE_DB_PATH, foreign_keys=False, busy_timeout_envs=("NAI_IM_QUEUE_SQLITE_BUSY_TIMEOUT_MS", "NAI_IM_SQLITE_BUSY_TIMEOUT_MS"))

def init_db() -> None:
    conn = _connect()
    try:
        # Existing DBs may have older tables missing newer columns.
        # CREATE TABLE IF NOT EXISTS will not add columns. If we attempted to
        # create indexes on missing columns here, startup would crash.
        # So _SCHEMA_SQL contains tables only; migrations + indexes happen in migrate_db().
        conn.executescript(_SCHEMA_SQL)
        migrate_db(conn)
        conn.commit()
    finally:
        conn.close()

    qconn = _connect_queue()
    try:
        qconn.executescript(_QUEUE_SCHEMA_SQL)
        migrate_queue_db(qconn)
        qconn.commit()
    finally:
        qconn.close()


def migrate_db(conn: sqlite3.Connection) -> None:
    """Best-effort migrations for early-stage schema tweaks."""
    def _cols(table: str) -> set[str]:
        try:
            return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        except Exception:
            return set()

    def _has_col(table: str, col: str) -> bool:
        return col in _cols(table)

    def _ensure_col(table: str, col: str, ddl: str) -> None:
        try:
            if not _has_col(table, col):
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
        except Exception:
            return

    def _ensure_drop_import_failures_table() -> None:
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS drop_import_failures (
                  path TEXT PRIMARY KEY,
                  size INTEGER NOT NULL,
                  mtime_ns INTEGER NOT NULL,
                  error TEXT,
                  failed_at_utc TEXT NOT NULL
                )
                """
            )
        except Exception:
            return

    def _ensure_index(name: str, table: str, cols: list[str]) -> None:
        try:
            if all(_has_col(table, c) for c in cols):
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {name} ON {table}({', '.join(cols)})"
                )
        except Exception:
            return

    _ensure_drop_import_failures_table()

    # ---- image_tags (older PK variant) ----
    try:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='image_tags'"
        ).fetchone()
        sql = (row["sql"] if row else "") or ""
        if sql and "coalesce(" in sql.lower():
            conn.execute("ALTER TABLE image_tags RENAME TO image_tags_old")
            conn.execute(
                """
                CREATE TABLE image_tags (
                  image_id        INTEGER NOT NULL REFERENCES images(id) ON DELETE CASCADE,
                  tag_canonical   TEXT NOT NULL,
                  tag_text        TEXT,
                  tag_raw         TEXT,
                  category        INTEGER,
                  emphasis_type   TEXT NOT NULL CHECK(emphasis_type IN ('none','braces','numeric')),
                  brace_level     INTEGER NOT NULL DEFAULT 0,
                  numeric_weight  REAL NOT NULL DEFAULT 0,
                  PRIMARY KEY(image_id, tag_canonical, emphasis_type, brace_level, numeric_weight)
                );
                """
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO image_tags(image_id, tag_canonical, tag_text, tag_raw, category, emphasis_type, brace_level, numeric_weight)
                SELECT image_id, tag_canonical, tag_text, tag_raw, category, emphasis_type,
                       COALESCE(brace_level,0) AS brace_level, COALESCE(numeric_weight,0) AS numeric_weight
                FROM image_tags_old;
                """
            )
            conn.execute("DROP TABLE image_tags_old")
    except Exception:
        # Keep going; columns/indexes below are more important for startup.
        pass

    # ---- users (add master role + password setup state) ----
    try:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='users'"
        ).fetchone()
        sql = (row[0] if isinstance(row, tuple) else (row["sql"] if row else "")) if row else ""
        sql_l = (sql or "").lower()

        need_rebuild = False
        # Old CHECK(role IN ('admin','user')) blocks 'master'.
        if sql and "check" in sql_l and "master" not in sql_l:
            need_rebuild = True

        if need_rebuild:
            # IMPORTANT:
            # Do NOT rename users -> users_old.
            # In SQLite, renaming the referenced table rewrites foreign key constraints in *other* tables
            # (images/password_tokens/...) to reference the renamed name. If we then drop users_old,
            # inserts start failing with "no such table: users_old".
            try:
                conn.commit()
            except Exception:
                pass
            conn.execute("PRAGMA foreign_keys = OFF;")
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users_new (
                      id                INTEGER PRIMARY KEY,
                      username          TEXT NOT NULL UNIQUE,
                      username_norm     TEXT,
                      password_hash     TEXT NOT NULL,
                      role              TEXT NOT NULL CHECK(role IN ('master','admin','user')),
                      created_at        TEXT NOT NULL DEFAULT (datetime('now')),
                      disabled          INTEGER NOT NULL DEFAULT 0,
                      must_set_password INTEGER NOT NULL DEFAULT 0,
                      pw_set_at         TEXT
                    );
                    """
                )

                cols_u = _cols("users")
                sel_id = "id" if "id" in cols_u else "NULL"
                sel_username = "username" if "username" in cols_u else "''"
                sel_ph = "password_hash" if "password_hash" in cols_u else "''"
                sel_role = "role" if "role" in cols_u else "'user'"
                sel_created = "created_at" if "created_at" in cols_u else "datetime('now')"
                sel_disabled = "disabled" if "disabled" in cols_u else "0"
                sel_msp = "must_set_password" if "must_set_password" in cols_u else "0"
                sel_pws = "pw_set_at" if "pw_set_at" in cols_u else sel_created

                conn.execute(
                    f"""
                    INSERT INTO users_new(id, username, username_norm, password_hash, role, created_at, disabled, must_set_password, pw_set_at)
                    SELECT {sel_id}, {sel_username}, NULL, {sel_ph},
                           CASE WHEN {sel_role}='master' THEN 'master'
                                WHEN {sel_role} IN ('admin','user') THEN {sel_role}
                                ELSE 'user' END,
                           {sel_created}, COALESCE({sel_disabled},0),
                           COALESCE({sel_msp},0),
                           {sel_pws}
                    FROM users;
                    """
                )
                conn.execute("DROP TABLE users")
                conn.execute("ALTER TABLE users_new RENAME TO users")
                try:
                    conn.commit()
                except Exception:
                    pass
            finally:
                conn.execute("PRAGMA foreign_keys = ON;")
    except Exception:
        pass

    # Add new columns if the users table wasn't rebuilt.
    _ensure_col("users", "must_set_password", "must_set_password INTEGER NOT NULL DEFAULT 0")
    _ensure_col("users", "pw_set_at", "pw_set_at TEXT")
    _ensure_col("users", "username_norm", "username_norm TEXT")

    try:
        import unicodedata

        rows = conn.execute("SELECT id, username, username_norm FROM users").fetchall()
        for r in rows:
            username = str(r["username"] or "")
            norm = unicodedata.normalize("NFKC", username).strip().casefold()
            if str(r["username_norm"] or "") != norm:
                conn.execute("UPDATE users SET username_norm=? WHERE id=?", (norm, int(r["id"])))

        dup = conn.execute(
            """
            SELECT username_norm, COUNT(*) AS n
            FROM users
            WHERE username_norm IS NOT NULL AND TRIM(username_norm) <> ''
            GROUP BY username_norm
            HAVING COUNT(*) > 1
            LIMIT 1
            """
        ).fetchone()
        if not dup:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_norm ON users(username_norm)")
    except Exception:
        pass


    # ---- Repair: foreign keys rewritten to users_old (caused by an earlier migration bug) ----
    # If any table still references users_old, runtime inserts can fail with:
    #   sqlite3.OperationalError: no such table: main.users_old
    def _create_sql_for_new_table(table: str, new_table: str) -> Optional[str]:
        try:
            r = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if not r:
                return None
            sql0 = r["sql"] if isinstance(r, sqlite3.Row) else r[0]
            if not sql0:
                return None
            import re

            s = (sql0 or "").strip()
            # Rename table in CREATE TABLE ... <name>
            name_pat = rf'(?:"{re.escape(table)}"|`{re.escape(table)}`|\[{re.escape(table)}\]|{re.escape(table)})'
            s = re.sub(
                rf'(?i)\bCREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?{name_pat}',
                lambda m: f"CREATE TABLE {('IF NOT EXISTS ' if m.group(1) else '')}{new_table}",
                s,
                count=1,
            )
            # Fix FK targets to users_old.
            ref_pat = r'(?:"users_old"|`users_old`|\[users_old\]|users_old)'
            # The referenced table is often followed immediately by "(id)" without whitespace.
            s = re.sub(rf'(?i)\bREFERENCES\s+{ref_pat}', 'REFERENCES users', s)
            return s
        except Exception:
            return None

    def _fk_refs(table: str, target: str) -> bool:
        try:
            rows = conn.execute(f"PRAGMA foreign_key_list({table})").fetchall()
            return any((rr["table"] if isinstance(rr, sqlite3.Row) else rr[2]) == target for rr in rows)
        except Exception:
            return False

    def _repair_fk_to_users(table: str) -> None:
        if not _fk_refs(table, "users_old"):
            return
        create_sql = _create_sql_for_new_table(table, f"{table}_new")
        if not create_sql:
            return
        cols = [rr["name"] for rr in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if not cols:
            return
        col_list = ", ".join(cols)
        try:
            conn.commit()
        except Exception:
            pass
        conn.execute("PRAGMA foreign_keys = OFF;")
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table}_new")
            conn.execute(create_sql)
            conn.execute(f"INSERT INTO {table}_new({col_list}) SELECT {col_list} FROM {table}")
            conn.execute(f"DROP TABLE {table}")
            conn.execute(f"ALTER TABLE {table}_new RENAME TO {table}")
            try:
                conn.commit()
            except Exception:
                pass
        finally:
            conn.execute("PRAGMA foreign_keys = ON;")

    for _t in ("images", "password_tokens"):
        try:
            _repair_fk_to_users(_t)
        except Exception:
            pass

    # ---- image_files (move originals out of DB; allow disk_path) ----
    try:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='image_files'"
        ).fetchone()
        sql = (row["sql"] if row else "") or ""
        # Old schema: image_files(image_id PK, bytes BLOB NOT NULL)
        # New schema: disk_path/size, bytes optional (for migration only)
        need_rebuild = False
        if sql and "bytes" in sql.lower() and "disk_path" not in sql.lower():
            need_rebuild = True
        if sql and "blob not null" in sql.lower() and "bytes" in sql.lower():
            need_rebuild = True

        if need_rebuild:
            conn.execute("ALTER TABLE image_files RENAME TO image_files_old")
            conn.execute(
                """
                CREATE TABLE image_files (
                  image_id   INTEGER PRIMARY KEY REFERENCES images(id) ON DELETE CASCADE,
                  disk_path  TEXT,
                  size       INTEGER,
                  bytes      BLOB
                );
                """
            )
            conn.execute(
                """
                INSERT INTO image_files(image_id, disk_path, size, bytes)
                SELECT image_id, NULL AS disk_path, LENGTH(bytes) AS size, bytes
                FROM image_files_old;
                """
            )
            conn.execute("DROP TABLE image_files_old")
    except Exception:
        pass

    # ---- image_derivatives (move derivatives out of DB; allow disk_path) ----
    try:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='image_derivatives'"
        ).fetchone()
        sql = (row["sql"] if row else "") or ""
        # Old schema: image_derivatives(... bytes BLOB NOT NULL)
        # New schema: disk_path/size, bytes optional (for migration only)
        need_rebuild = False
        if sql and "bytes" in sql.lower() and "disk_path" not in sql.lower():
            need_rebuild = True
        if sql and "blob not null" in sql.lower() and "bytes" in sql.lower():
            need_rebuild = True

        if need_rebuild:
            conn.execute("ALTER TABLE image_derivatives RENAME TO image_derivatives_old")
            conn.execute(
                """
                CREATE TABLE image_derivatives (
                  id             INTEGER PRIMARY KEY,
                  image_id       INTEGER NOT NULL REFERENCES images(id) ON DELETE CASCADE,
                  kind           TEXT NOT NULL,
                  format         TEXT NOT NULL,
                  width          INTEGER NOT NULL,
                  height         INTEGER NOT NULL,
                  quality        INTEGER,
                  disk_path      TEXT,
                  size           INTEGER,
                  bytes          BLOB,
                  created_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                  UNIQUE(image_id, kind, format)
                );
                """
            )
            conn.execute(
                """
                INSERT INTO image_derivatives(id, image_id, kind, format, width, height, quality, disk_path, size, bytes, created_at_utc)
                SELECT id, image_id, kind, format, width, height, quality, NULL AS disk_path, LENGTH(bytes) AS size, bytes, created_at_utc
                FROM image_derivatives_old;
                """
            )
            conn.execute("DROP TABLE image_derivatives_old")
    except Exception:
        pass

    _ensure_col("image_derivatives", "disk_path", "disk_path TEXT")
    _ensure_col("image_derivatives", "size", "size INTEGER")

    # ---- image_tags columns ----
    _ensure_col("image_tags", "src_mask", "src_mask INTEGER NOT NULL DEFAULT 1")
    _ensure_col("image_tags", "seq", "seq INTEGER NOT NULL DEFAULT 0")

    # ---- images columns (upgrade path) ----
    _ensure_col("images", "software", "software TEXT")
    _ensure_col("images", "model_name", "model_name TEXT")
    _ensure_col("images", "prompt_positive_raw", "prompt_positive_raw TEXT")
    _ensure_col("images", "prompt_negative_raw", "prompt_negative_raw TEXT")
    _ensure_col("images", "prompt_character_raw", "prompt_character_raw TEXT")
    _ensure_col("images", "character_entries_json", "character_entries_json TEXT")
    _ensure_col("images", "main_negative_combined_raw", "main_negative_combined_raw TEXT")
    _ensure_col("images", "seed", "seed INTEGER")
    _ensure_col("images", "params_json", "params_json TEXT")
    _ensure_col("images", "potion_raw", "potion_raw BLOB")
    _ensure_col("images", "has_potion", "has_potion INTEGER NOT NULL DEFAULT 0")
    _ensure_col("images", "uses_potion", "uses_potion INTEGER NOT NULL DEFAULT 0")
    _ensure_col("images", "uses_precise_reference", "uses_precise_reference INTEGER NOT NULL DEFAULT 0")
    _ensure_col("images", "sampler", "sampler TEXT")
    _ensure_col("images", "metadata_raw", "metadata_raw TEXT")
    _ensure_col("images", "main_sig_hash", "main_sig_hash TEXT")
    _ensure_col("images", "full_meta_hash", "full_meta_hash TEXT")
    _ensure_col("images", "dedup_flag", "dedup_flag INTEGER NOT NULL DEFAULT 1")
    _ensure_col("images", "favorite", "favorite INTEGER NOT NULL DEFAULT 0")
    _ensure_col("images", "is_nsfw", "is_nsfw INTEGER NOT NULL DEFAULT 0")
    _ensure_col("images", "reparse_skip", "reparse_skip INTEGER NOT NULL DEFAULT 0")

    try:
        from .services.metadata_extract import detect_generation_usage_from_storage

        if (
            _has_col("images", "uses_potion")
            and _has_col("images", "uses_precise_reference")
            and _has_col("images", "sampler")
            and _has_col("images", "params_json")
            and _has_col("images", "metadata_raw")
            and _has_col("images", "has_potion")
        ):
            rows = conn.execute(
                "SELECT id, params_json, metadata_raw, has_potion, uses_potion, uses_precise_reference, sampler FROM images"
            ).fetchall()
            for r in rows:
                uses_potion_b, uses_precise_reference_b, sampler = detect_generation_usage_from_storage(
                    r["params_json"],
                    r["metadata_raw"],
                )
                uses_potion = 1 if uses_potion_b else 0
                uses_precise_reference = 1 if uses_precise_reference_b else 0
                has_potion = 1 if uses_potion_b else 0
                if (
                    int(r["has_potion"] or 0) == has_potion
                    and int(r["uses_potion"] or 0) == uses_potion
                    and int(r["uses_precise_reference"] or 0) == uses_precise_reference
                    and (r["sampler"] or None) == sampler
                ):
                    continue
                conn.execute(
                    "UPDATE images SET has_potion=?, uses_potion=?, uses_precise_reference=?, sampler=? WHERE id=?",
                    (has_potion, uses_potion, uses_precise_reference, sampler, int(r["id"])),
                )
    except Exception:
        pass

    # ---- backfill seed/full_meta_hash (best-effort, small batch) ----
    # This enables full-meta dedup to work for existing DBs.
    try:
        if _has_col("images", "full_meta_hash") and _has_col("images", "seed"):
            import hashlib, json

            rows = conn.execute(
                """
                SELECT id, software, model_name, prompt_positive_raw, prompt_negative_raw, prompt_character_raw,
                       seed, params_json
                FROM images
                WHERE full_meta_hash IS NULL
                LIMIT 2000
                """
            ).fetchall()
            for r in rows:
                seed = r["seed"]
                if seed is None and r["params_json"]:
                    try:
                        pj = json.loads(r["params_json"])
                        if isinstance(pj, dict) and "seed" in pj:
                            seed = int(pj.get("seed"))
                    except Exception:
                        seed = None

                src = "\n".join(
                    [
                        (r["software"] or ""),
                        (r["model_name"] or ""),
                        (r["prompt_positive_raw"] or ""),
                        (r["prompt_character_raw"] or ""),
                        (r["prompt_negative_raw"] or ""),
                        (str(seed) if seed is not None else ""),
                    ]
                )
                h = hashlib.sha1(src.encode("utf-8", errors="ignore")).hexdigest()
                conn.execute(
                    "UPDATE images SET seed = COALESCE(seed, ?), full_meta_hash = ? WHERE id = ?",
                    (seed, h, int(r["id"])),
                )
    except Exception:
        pass

    # ---- image_files columns (if table already existed) ----
    _ensure_col("image_files", "disk_path", "disk_path TEXT")
    _ensure_col("image_files", "size", "size INTEGER")
    _ensure_col("upload_zip_jobs", "source_kind", "source_kind TEXT NOT NULL DEFAULT 'zip'")
    _ensure_col("upload_zip_jobs", "staging_dir", "staging_dir TEXT")
    _ensure_col("upload_zip_jobs", "bookmark_enabled", "bookmark_enabled INTEGER NOT NULL DEFAULT 0")
    _ensure_col("upload_zip_jobs", "bookmark_list_id", "bookmark_list_id INTEGER")
    _ensure_col("upload_zip_items", "staged_path", "staged_path TEXT")
    _ensure_col("upload_zip_items", "mtime_iso", "mtime_iso TEXT")

    # ---- images.public_id (external/public id for public grid thumbs) ----
    _ensure_col("images", "public_id", "public_id TEXT")

    try:
        rows = conn.execute("SELECT id, public_id FROM images ORDER BY id").fetchall()
        seen_public_ids: set[str] = set()

        def _new_public_id() -> str:
            while True:
                candidate = secrets.token_hex(16)
                if candidate not in seen_public_ids:
                    return candidate

        for row in rows:
            raw_public_id = str(row["public_id"] or "").strip().lower()
            needs_new = (not raw_public_id) or (raw_public_id in seen_public_ids)
            if needs_new:
                raw_public_id = _new_public_id()
                conn.execute("UPDATE images SET public_id=? WHERE id=?", (raw_public_id, int(row["id"])))
            seen_public_ids.add(raw_public_id)
    except Exception:
        pass

    # Keep file_mtime_utc usable for ordering (fallback to uploaded_at_utc if missing).
    try:
        conn.execute(
            """
            UPDATE images
            SET file_mtime_utc = REPLACE(uploaded_at_utc, ' ', 'T') || '+00:00'
            WHERE file_mtime_utc IS NULL OR TRIM(file_mtime_utc) = ''
            """
        )
    except Exception:
        pass

    # ---- backfill per-creator calendar stats (best-effort, one-time) ----
    # New tables introduced after existing installs already had stat_day/month/year.
    # Populate them once if empty so the calendar works immediately.
    try:
        has_img = conn.execute("SELECT 1 FROM images LIMIT 1").fetchone()
        if has_img:
            cdn = int(conn.execute("SELECT COUNT(*) AS n FROM stat_creator_day_counts").fetchone()[0])
            cmn = int(conn.execute("SELECT COUNT(*) AS n FROM stat_creator_month_counts").fetchone()[0])
            cyn = int(conn.execute("SELECT COUNT(*) AS n FROM stat_creator_year_counts").fetchone()[0])
            if cdn == 0:
                conn.execute("DELETE FROM stat_creator_day_counts")
                conn.execute(
                    """
                    INSERT INTO stat_creator_day_counts(creator_id, ymd, image_count)
                    SELECT uploader_user_id AS creator_id, SUBSTR(file_mtime_utc,1,10) AS ymd, COUNT(*) AS image_count
                    FROM images
                    WHERE file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 10
                    GROUP BY uploader_user_id, SUBSTR(file_mtime_utc,1,10)
                    """
                )
            if cmn == 0:
                conn.execute("DELETE FROM stat_creator_month_counts")
                conn.execute(
                    """
                    INSERT INTO stat_creator_month_counts(creator_id, ym, image_count)
                    SELECT uploader_user_id AS creator_id, SUBSTR(file_mtime_utc,1,7) AS ym, COUNT(*) AS image_count
                    FROM images
                    WHERE file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 7
                    GROUP BY uploader_user_id, SUBSTR(file_mtime_utc,1,7)
                    """
                )
            if cyn == 0:
                conn.execute("DELETE FROM stat_creator_year_counts")
                conn.execute(
                    """
                    INSERT INTO stat_creator_year_counts(creator_id, year, image_count)
                    SELECT uploader_user_id AS creator_id, SUBSTR(file_mtime_utc,1,4) AS year, COUNT(*) AS image_count
                    FROM images
                    WHERE file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 4
                    GROUP BY uploader_user_id, SUBSTR(file_mtime_utc,1,4)
                    """
                )
    except Exception:
        pass

    # ---- indexes (create only when columns exist) ----
    _ensure_index("idx_images_mtime", "images", ["file_mtime_utc"])
    _ensure_index("idx_images_mtime_id", "images", ["file_mtime_utc", "id"])
    _ensure_index("idx_images_uploader", "images", ["uploader_user_id"])
    _ensure_index("idx_images_software", "images", ["software"])
    _ensure_index("idx_images_model", "images", ["model_name"])
    _ensure_index("idx_images_dedup", "images", ["dedup_flag"])
    _ensure_index("idx_images_main_sig", "images", ["main_sig_hash"])
    _ensure_index("idx_images_full_meta", "images", ["full_meta_hash"])
    _ensure_index("idx_images_seed", "images", ["seed"])
    _ensure_index("idx_images_favorite", "images", ["favorite"])
    _ensure_index("idx_images_fav_mtime_id", "images", ["favorite", "file_mtime_utc", "id"])

    # Composite indexes for common filter+sort patterns.
    _ensure_index("idx_images_uploader_mtime_id", "images", ["uploader_user_id", "file_mtime_utc", "id"])
    _ensure_index("idx_images_software_mtime_id", "images", ["software", "file_mtime_utc", "id"])
    _ensure_index("idx_images_dedup_mtime_id", "images", ["dedup_flag", "file_mtime_utc", "id"])
    _ensure_index("idx_images_software_fav_mtime_id", "images", ["software", "favorite", "file_mtime_utc", "id"])
    _ensure_index("idx_images_uploader_fav_mtime_id", "images", ["uploader_user_id", "favorite", "file_mtime_utc", "id"])
    _ensure_index("idx_images_is_nsfw", "images", ["is_nsfw"])
    try:
        if _has_col("images", "public_id"):
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_images_public_id ON images(public_id)")
    except Exception:
        pass
    _ensure_index("idx_images_reparse_skip", "images", ["reparse_skip"])
    # ---- bookmark tables ----
    _ensure_index("idx_bookmark_lists_user", "bookmark_lists", ["user_id"])
    _ensure_index("idx_bookmark_lists_user_default", "bookmark_lists", ["user_id", "is_default"])
    _ensure_index("idx_bookmarks_list", "bookmarks", ["list_id"])
    _ensure_index("idx_bookmarks_image", "bookmarks", ["image_id"])

    # ---- per-user visibility/share + lists ----
    _ensure_index("idx_user_settings_share_works", "user_settings", ["share_works"])
    _ensure_index("idx_user_settings_share_bookmarks", "user_settings", ["share_bookmarks"])
    _ensure_index("idx_user_creators_user", "user_creators", ["user_id"])
    _ensure_index("idx_user_creators_creator", "user_creators", ["creator_user_id"])
    _ensure_index("idx_user_bm_creators_user", "user_bookmark_creators", ["user_id"])
    _ensure_index("idx_user_bm_creators_creator", "user_bookmark_creators", ["creator_user_id"])

    # ---- per-creator date stats ----
    _ensure_index("idx_stat_cday_ymd", "stat_creator_day_counts", ["ymd"])
    _ensure_index("idx_stat_cmonth_ym", "stat_creator_month_counts", ["ym"])
    _ensure_index("idx_stat_cyear_year", "stat_creator_year_counts", ["year"])

    # ---- per-creator software stats ----
    _ensure_index("idx_stat_csoft_creator", "stat_creator_software", ["creator_id"])
    _ensure_index("idx_stat_csoft_software", "stat_creator_software", ["software"])


    try:
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='image_derivatives'"
        ).fetchone():
            conn.execute("CREATE INDEX IF NOT EXISTS idx_deriv_kind ON image_derivatives(kind);")
    except Exception:
        pass

    try:
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='derivative_jobs'"
        ).fetchone():
            conn.execute("CREATE INDEX IF NOT EXISTS idx_derivative_jobs_status_requested ON derivative_jobs(status, requested_at_utc);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_derivative_jobs_requested ON derivative_jobs(requested_at_utc);")
    except Exception:
        pass

    try:
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='tag_aliases'"
        ).fetchone():
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tag_alias_canon ON tag_aliases(canonical);")
    except Exception:
        pass

    try:
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='image_tags'"
        ).fetchone():
            conn.execute("CREATE INDEX IF NOT EXISTS idx_image_tags_tag ON image_tags(tag_canonical);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_image_tags_cat ON image_tags(category);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_image_tags_image ON image_tags(image_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_image_tags_tag_image ON image_tags(tag_canonical, image_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_image_tags_image_seq ON image_tags(image_id, seq);")
    except Exception:
        pass


_QUEUE_SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS derivative_jobs (
  image_id          INTEGER PRIMARY KEY,
  need_grid         INTEGER NOT NULL DEFAULT 0,
  need_overlay      INTEGER NOT NULL DEFAULT 0,
  status            TEXT NOT NULL DEFAULT 'queued',
  requested_at_utc  TEXT NOT NULL DEFAULT (datetime('now')),
  started_at_utc    TEXT,
  finished_at_utc   TEXT,
  last_error        TEXT,
  retry_count       INTEGER NOT NULL DEFAULT 0,
  request_count     INTEGER NOT NULL DEFAULT 0,
  last_source       TEXT,
  last_trace_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_derivative_jobs_status_requested ON derivative_jobs(status, requested_at_utc);
CREATE INDEX IF NOT EXISTS idx_derivative_jobs_requested ON derivative_jobs(requested_at_utc);

CREATE TABLE IF NOT EXISTS upload_item_jobs (
  item_id           INTEGER PRIMARY KEY,
  status            TEXT NOT NULL DEFAULT 'queued',
  requested_at_utc  TEXT NOT NULL DEFAULT (datetime('now')),
  started_at_utc    TEXT,
  finished_at_utc   TEXT,
  last_error        TEXT,
  retry_count       INTEGER NOT NULL DEFAULT 0,
  request_count     INTEGER NOT NULL DEFAULT 0,
  last_source       TEXT,
  last_trace_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_upload_item_jobs_status_requested ON upload_item_jobs(status, requested_at_utc);
CREATE INDEX IF NOT EXISTS idx_upload_item_jobs_requested ON upload_item_jobs(requested_at_utc);
"""

def migrate_queue_db(conn: sqlite3.Connection) -> None:
    def _cols(table: str) -> set[str]:
        try:
            return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        except Exception:
            return set()

    def _has_col(table: str, col: str) -> bool:
        return col in _cols(table)

    def _ensure_col(table: str, col: str, ddl: str) -> None:
        try:
            if not _has_col(table, col):
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
        except Exception:
            return

    for name, ddl in (
        ("need_grid", "need_grid INTEGER NOT NULL DEFAULT 0"),
        ("need_overlay", "need_overlay INTEGER NOT NULL DEFAULT 0"),
        ("status", "status TEXT NOT NULL DEFAULT 'queued'"),
        ("requested_at_utc", "requested_at_utc TEXT NOT NULL DEFAULT (datetime('now'))"),
        ("started_at_utc", "started_at_utc TEXT"),
        ("finished_at_utc", "finished_at_utc TEXT"),
        ("last_error", "last_error TEXT"),
        ("retry_count", "retry_count INTEGER NOT NULL DEFAULT 0"),
        ("request_count", "request_count INTEGER NOT NULL DEFAULT 0"),
        ("last_source", "last_source TEXT"),
        ("last_trace_id", "last_trace_id TEXT"),
    ):
        _ensure_col("derivative_jobs", name, ddl)

    for name, ddl in (
        ("status", "status TEXT NOT NULL DEFAULT 'queued'"),
        ("requested_at_utc", "requested_at_utc TEXT NOT NULL DEFAULT (datetime('now'))"),
        ("started_at_utc", "started_at_utc TEXT"),
        ("finished_at_utc", "finished_at_utc TEXT"),
        ("last_error", "last_error TEXT"),
        ("retry_count", "retry_count INTEGER NOT NULL DEFAULT 0"),
        ("request_count", "request_count INTEGER NOT NULL DEFAULT 0"),
        ("last_source", "last_source TEXT"),
        ("last_trace_id", "last_trace_id TEXT"),
    ):
        _ensure_col("upload_item_jobs", name, ddl)

    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_derivative_jobs_status_requested ON derivative_jobs(status, requested_at_utc)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_derivative_jobs_requested ON derivative_jobs(requested_at_utc)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_upload_item_jobs_status_requested ON upload_item_jobs(status, requested_at_utc)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_upload_item_jobs_requested ON upload_item_jobs(requested_at_utc)")
    except Exception:
        pass

def get_conn() -> sqlite3.Connection:
    return _connect()

def get_queue_conn() -> sqlite3.Connection:
    return _connect_queue()

def ensure_bootstrap() -> None:
    """Initial bootstrap:
    - import tag dictionary if empty
    - ensure a single master user exists for upgraded DBs
    """

    conn = _connect()
    try:
        # users
        cur = conn.execute("SELECT COUNT(*) AS n FROM users")
        n = int(cur.fetchone()["n"])
        # DO NOT auto-create default credentials. First-time setup is handled via /setup.html.

        # Upgraded DBs may not have a master role yet. Promote the oldest admin to master.
        if n > 0:
            has_master = conn.execute("SELECT 1 FROM users WHERE role='master' LIMIT 1").fetchone()
            if not has_master:
                row = conn.execute(
                    "SELECT id FROM users WHERE role='admin' ORDER BY id ASC LIMIT 1"
                ).fetchone()
                if row:
                    conn.execute(
                        "UPDATE users SET role='master' WHERE id=?",
                        (int(row["id"] if not isinstance(row, tuple) else row[0]),),
                    )
                    conn.commit()

        # tag dictionaries import
        cur = conn.execute("SELECT COUNT(*) AS n FROM tags_master")
        tn = int(cur.fetchone()["n"])
        cur = conn.execute("SELECT COUNT(*) AS n FROM tag_aliases")
        an = int(cur.fetchone()["n"])

        tag_master = ASSETS_DIR / "tags" / "tag_master.csv.gz"
        tag_alias = ASSETS_DIR / "tags" / "tag_alias.csv.gz"

        if tn == 0 and tag_master.exists():
            _import_tags_master(conn, tag_master)

        # Even if tags_master already exists, we may still want to import aliases
        # (e.g., a previous run partially initialized the DB).
        if an == 0 and tag_alias.exists():
            _import_tag_aliases(conn, tag_alias)

        if tn == 0 or an == 0:
            conn.commit()

        # quality tags set
        # - include extra quality-like tags
        # - support wildcard patterns (e.g. year_*) stored as-is
        # - refresh automatically when the bundled asset changes
        try:
            import hashlib
            from .services.tag_parser import normalize_tag

            cur = conn.execute("SELECT COUNT(*) AS n FROM quality_tags")
            qn = int(cur.fetchone()["n"])

            qfile = ASSETS_DIR / "tags" / "extra-quality-tags.csv"
            if qfile.exists():
                qhash = hashlib.sha1(qfile.read_bytes()).hexdigest()
                old = conn.execute(
                    "SELECT value FROM admin_kv WHERE key='quality_tags_asset_sha1'"
                ).fetchone()
                oldhash = (str(old[0] if isinstance(old, tuple) else old["value"]) if old else "")

                if qn == 0 or oldhash != qhash:
                    conn.execute("DELETE FROM quality_tags")
                    with qfile.open("r", encoding="utf-8", errors="replace") as f:
                        for line in f:
                            line = (line or "").strip()
                            if not line:
                                continue
                            parts = line.split(",")
                            tag = (parts[0] if parts else "").strip()
                            if not tag:
                                continue
                            tn = normalize_tag(tag)
                            if tn:
                                # Accept patterns like "year *" in CSV as "year*".
                                if tn.endswith("_*"):
                                    tn = tn[:-2] + "*"
                                conn.execute(
                                    "INSERT OR IGNORE INTO quality_tags(tag) VALUES (?)",
                                    (tn,),
                                )
                    conn.execute(
                        """
                        INSERT INTO admin_kv(key, value, updated_at)
                        VALUES ('quality_tags_asset_sha1', ?, datetime('now'))
                        ON CONFLICT(key) DO UPDATE SET
                          value=excluded.value,
                          updated_at=datetime('now')
                        """,
                        (qhash,),
                    )
                    conn.commit()
        except Exception:
            # best-effort; app must still boot
            pass

        # stats caches (do NOT rebuild on every request; bootstrap once when empty)
        _ensure_stats_cache(conn)
        conn.commit()
    finally:
        conn.close()


def _ensure_stats_cache(conn: sqlite3.Connection) -> None:
    """Ensure stat_* tables are populated for existing DBs.

    Upload paths bump stats incrementally, but older DBs (or interrupted boots)
    may have empty caches.
    """
    n_images = int(conn.execute("SELECT COUNT(*) AS n FROM images").fetchone()["n"])
    if n_images <= 0:
        return

    # NOTE: UI must query stat_* tables only. These caches are incrementally
    # maintained during upload/reparse, but older DBs (or interrupted runs)
    # can drift. Here we do a cheap consistency check and self-heal.
    from .services import stats as stats_service

    def _count(table: str) -> int:
        return int(conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])

    def _sum(table: str) -> int:
        r = conn.execute(f"SELECT COALESCE(SUM(image_count),0) AS n FROM {table}").fetchone()
        return int((r["n"] if r else 0) or 0)

    # creators: sum must match total images
    creators_rows = _count("stat_creators")
    creators_sum = _sum("stat_creators")
    if creators_rows == 0 or creators_sum != n_images:
        stats_service.rebuild_creators(conn)

    # software: sum must match images that have software
    software_expected = int(
        conn.execute("SELECT COUNT(*) AS n FROM images WHERE software IS NOT NULL AND TRIM(software) <> ''").fetchone()["n"]
    )
    software_rows = _count("stat_software")
    software_sum = _sum("stat_software")
    if software_rows == 0 or software_sum != software_expected:
        stats_service.rebuild_software(conn)

    # day counts: sum must match images that have usable file_mtime_utc
    day_expected = int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM images WHERE file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 10"
        ).fetchone()["n"]
    )
    day_rows = _count("stat_day_counts")
    day_sum = _sum("stat_day_counts")
    if day_rows == 0 or day_sum != day_expected:
        stats_service.rebuild_day_counts(conn)

    # month counts: sum must match images that have usable file_mtime_utc (YYYY-MM)
    month_expected = int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM images WHERE file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 7"
        ).fetchone()["n"]
    )
    month_rows = _count("stat_month_counts")
    month_sum = _sum("stat_month_counts")
    if month_rows == 0 or month_sum != month_expected:
        stats_service.rebuild_month_counts(conn)

    # year counts: sum must match images that have usable file_mtime_utc (YYYY)
    year_expected = int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM images WHERE file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 4"
        ).fetchone()["n"]
    )
    year_rows = _count("stat_year_counts")
    year_sum = _sum("stat_year_counts")
    if year_rows == 0 or year_sum != year_expected:
        stats_service.rebuild_year_counts(conn)

    # tag counts: if row counts mismatch, rebuild.
    # In addition, spot-check top tags quickly (indexed) to catch drift.
    tag_rows = _count("stat_tag_counts")
    tag_expected_rows = int(
        conn.execute("SELECT COUNT(DISTINCT tag_canonical) AS n FROM image_tags").fetchone()["n"]
    )
    need_tag_rebuild = (tag_rows == 0 or tag_rows != tag_expected_rows)
    if not need_tag_rebuild and tag_rows > 0:
        try:
            samples = conn.execute(
                "SELECT tag_canonical, image_count FROM stat_tag_counts ORDER BY image_count DESC LIMIT 20"
            ).fetchall()
            for s in samples:
                canon = str(s["tag_canonical"])
                expected = int(
                    conn.execute(
                        "SELECT COUNT(DISTINCT image_id) AS n FROM image_tags WHERE tag_canonical=?",
                        (canon,),
                    ).fetchone()["n"]
                )
                if expected != int(s["image_count"] or 0):
                    need_tag_rebuild = True
                    break
        except Exception:
            # safest fallback
            need_tag_rebuild = True

    if need_tag_rebuild:
        stats_service.rebuild_tag_counts(conn)

def _import_tags_master(conn: sqlite3.Connection, gz_path: Path) -> None:
    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tag = (row.get("tag") or "").strip()
            if not tag:
                continue
            category = int(row.get("category") or 0)
            post_count = int(row.get("post_count") or 0) if (row.get("post_count") or "").strip().isdigit() else None
            sources = row.get("sources")
            conn.execute(
                "INSERT OR REPLACE INTO tags_master(tag, category, post_count, sources) VALUES (?,?,?,?)",
                (tag, category, post_count, sources),
            )

def _import_tag_aliases(conn: sqlite3.Connection, gz_path: Path) -> None:
    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            alias = (row.get("alias") or "").strip()
            canonical = (row.get("canonical") or "").strip()
            if not alias or not canonical:
                continue
            # Some alias dictionaries reference canonical tags not present in tag_master.
            # Ensure the canonical exists to satisfy the FK constraint.
            conn.execute(
                "INSERT OR IGNORE INTO tags_master(tag, category, post_count, sources) VALUES (?,?,?,?)",
                (canonical, None, None, None),
            )
            conn.execute(
                "INSERT OR REPLACE INTO tag_aliases(alias, canonical) VALUES (?,?)",
                (alias, canonical),
            )

_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
  id            INTEGER PRIMARY KEY,
  username      TEXT NOT NULL UNIQUE,
  username_norm TEXT,
  password_hash TEXT NOT NULL,
  role          TEXT NOT NULL CHECK(role IN ('master','admin','user')),
  created_at    TEXT NOT NULL DEFAULT (datetime('now')),
  disabled      INTEGER NOT NULL DEFAULT 0,
  must_set_password INTEGER NOT NULL DEFAULT 0,
  pw_set_at     TEXT
);

CREATE TABLE IF NOT EXISTS password_tokens (
  token       TEXT PRIMARY KEY,
  user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  kind        TEXT NOT NULL CHECK(kind IN ('setup','reset')),
  created_by  INTEGER REFERENCES users(id),
  created_at  TEXT NOT NULL DEFAULT (datetime('now')),
  expires_at  TEXT NOT NULL,
  used_at     TEXT
);

CREATE TABLE IF NOT EXISTS images (
  id                    INTEGER PRIMARY KEY,
  public_id             TEXT NOT NULL UNIQUE,
  sha256                TEXT NOT NULL UNIQUE,
  original_filename     TEXT NOT NULL,
  ext                   TEXT NOT NULL,
  mime                  TEXT NOT NULL,
  width                 INTEGER,
  height                INTEGER,
  file_mtime_utc        TEXT,
  uploaded_at_utc       TEXT NOT NULL DEFAULT (datetime('now')),
  uploader_user_id      INTEGER NOT NULL REFERENCES users(id),

  software              TEXT,
  model_name            TEXT,

  prompt_positive_raw   TEXT,
  prompt_negative_raw   TEXT,
  prompt_character_raw  TEXT,
  character_entries_json TEXT,
  main_negative_combined_raw TEXT,
  seed                  INTEGER,
  params_json           TEXT,
  potion_raw            BLOB,
  has_potion            INTEGER NOT NULL DEFAULT 0,
  uses_potion           INTEGER NOT NULL DEFAULT 0,
  uses_precise_reference INTEGER NOT NULL DEFAULT 0,
  sampler               TEXT,
  metadata_raw          TEXT,

  main_sig_hash         TEXT,
  full_meta_hash        TEXT,
  dedup_flag            INTEGER NOT NULL DEFAULT 1 CHECK(dedup_flag IN (1,2)),
  favorite              INTEGER NOT NULL DEFAULT 0,
  is_nsfw               INTEGER NOT NULL DEFAULT 0,
  reparse_skip          INTEGER NOT NULL DEFAULT 0
);


CREATE TABLE IF NOT EXISTS bookmark_lists (
  id         INTEGER PRIMARY KEY,
  user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name       TEXT NOT NULL,
  sort_order INTEGER NOT NULL DEFAULT 0,
  is_default INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(user_id, name)
);

CREATE TABLE IF NOT EXISTS bookmarks (
  list_id    INTEGER NOT NULL REFERENCES bookmark_lists(id) ON DELETE CASCADE,
  image_id   INTEGER NOT NULL REFERENCES images(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY(list_id, image_id)
);

-- Per-user sharing settings.
-- share_works: other users can see this user's uploaded images.
-- share_bookmarks: other users can see this user's bookmark lists (read-only) when subscribed.
CREATE TABLE IF NOT EXISTS user_settings (
  user_id         INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  share_works     INTEGER NOT NULL DEFAULT 0,
  share_bookmarks INTEGER NOT NULL DEFAULT 0,
  updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Viewer -> Creator allow-list ("作者一覧").
-- If a creator is registered here, viewer can see that creator's works even when share_works=OFF.
CREATE TABLE IF NOT EXISTS user_creators (
  user_id          INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  creator_user_id  INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at       TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY(user_id, creator_user_id)
);

-- Viewer -> Creator subscription list for bookmark lists ("ブックマーク一覧" creators).
CREATE TABLE IF NOT EXISTS user_bookmark_creators (
  user_id          INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  creator_user_id  INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at       TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY(user_id, creator_user_id)
);

CREATE TABLE IF NOT EXISTS admin_kv (
  key           TEXT PRIMARY KEY,
  value         TEXT NOT NULL,
  updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS maintenance_runs (
  id            INTEGER PRIMARY KEY,
  kind          TEXT NOT NULL,
  params_json   TEXT,
  status        TEXT NOT NULL DEFAULT 'running' CHECK(status IN ('running','done','stopped')),
  created_at    TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
  last_image_id INTEGER NOT NULL DEFAULT 0,
  processed     INTEGER NOT NULL DEFAULT 0,
  updated       INTEGER NOT NULL DEFAULT 0,
  error_count   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS maintenance_errors (
  id            INTEGER PRIMARY KEY,
  run_id        INTEGER NOT NULL REFERENCES maintenance_runs(id) ON DELETE CASCADE,
  image_id      INTEGER,
  stage         TEXT,
  error         TEXT NOT NULL,
  created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_maint_err_run ON maintenance_errors(run_id, id);
CREATE INDEX IF NOT EXISTS idx_maint_err_image ON maintenance_errors(image_id);

CREATE TABLE IF NOT EXISTS image_files (
  image_id       INTEGER PRIMARY KEY REFERENCES images(id) ON DELETE CASCADE,
  disk_path      TEXT,
  size           INTEGER,
  bytes          BLOB
);

CREATE TABLE IF NOT EXISTS image_derivatives (
  id             INTEGER PRIMARY KEY,
  image_id       INTEGER NOT NULL REFERENCES images(id) ON DELETE CASCADE,
  kind           TEXT NOT NULL,
  format         TEXT NOT NULL,
  width          INTEGER NOT NULL,
  height         INTEGER NOT NULL,
  quality        INTEGER,
  disk_path      TEXT,
  size           INTEGER,
  bytes          BLOB,
  created_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(image_id, kind, format)
);

CREATE TABLE IF NOT EXISTS upload_zip_jobs (
  id             INTEGER PRIMARY KEY,
  user_id        INTEGER,
  filename       TEXT,
  source_kind    TEXT NOT NULL DEFAULT 'zip',
  staging_dir    TEXT,
  bookmark_enabled INTEGER NOT NULL DEFAULT 0,
  bookmark_list_id INTEGER,
  total          INTEGER NOT NULL DEFAULT 0,
  done           INTEGER NOT NULL DEFAULT 0,
  failed         INTEGER NOT NULL DEFAULT 0,
  dup            INTEGER NOT NULL DEFAULT 0,
  status         TEXT NOT NULL DEFAULT 'queued',
  created_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
  error          TEXT
);

CREATE TABLE IF NOT EXISTS upload_zip_items (
  id             INTEGER PRIMARY KEY,
  job_id         INTEGER NOT NULL REFERENCES upload_zip_jobs(id) ON DELETE CASCADE,
  seq            INTEGER NOT NULL,
  filename       TEXT,
  state          TEXT,
  image_id       INTEGER,
  message        TEXT,
  staged_path    TEXT,
  mtime_iso      TEXT,
  created_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_upload_zip_items_job_seq ON upload_zip_items(job_id, seq);

CREATE TABLE IF NOT EXISTS tags_master (
  tag           TEXT PRIMARY KEY,
  category      INTEGER,
  post_count    INTEGER,
  sources       TEXT
);

CREATE TABLE IF NOT EXISTS tag_aliases (
  alias         TEXT PRIMARY KEY,
  canonical     TEXT NOT NULL REFERENCES tags_master(tag)
);

CREATE TABLE IF NOT EXISTS quality_tags (
  tag           TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS image_tags (
  image_id        INTEGER NOT NULL REFERENCES images(id) ON DELETE CASCADE,
  tag_canonical   TEXT NOT NULL,
  tag_text        TEXT,
  tag_raw         TEXT,
  category        INTEGER,
  emphasis_type   TEXT NOT NULL CHECK(emphasis_type IN ('none','braces','numeric')),
  brace_level     INTEGER NOT NULL DEFAULT 0,
  numeric_weight  REAL NOT NULL DEFAULT 0,
  src_mask       INTEGER NOT NULL DEFAULT 1,
  seq            INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(image_id, tag_canonical, emphasis_type, brace_level, numeric_weight)
);

CREATE TABLE IF NOT EXISTS stat_creators (
  creator        TEXT PRIMARY KEY,
  image_count    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS stat_software (
  software       TEXT PRIMARY KEY,
  image_count    INTEGER NOT NULL
);

-- Per-creator software counts used by the sidebar filter.
CREATE TABLE IF NOT EXISTS stat_creator_software (
  creator_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  software       TEXT NOT NULL,
  image_count    INTEGER NOT NULL,
  PRIMARY KEY(creator_id, software)
);

CREATE TABLE IF NOT EXISTS stat_day_counts (
  ymd            TEXT PRIMARY KEY,
  image_count    INTEGER NOT NULL
);

-- Per-creator day/month/year counts used by the calendar.
CREATE TABLE IF NOT EXISTS stat_creator_day_counts (
  creator_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  ymd            TEXT NOT NULL,
  image_count    INTEGER NOT NULL,
  PRIMARY KEY(creator_id, ymd)
);

CREATE TABLE IF NOT EXISTS stat_creator_month_counts (
  creator_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  ym             TEXT NOT NULL,
  image_count    INTEGER NOT NULL,
  PRIMARY KEY(creator_id, ym)
);

CREATE TABLE IF NOT EXISTS stat_creator_year_counts (
  creator_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  year           TEXT NOT NULL,
  image_count    INTEGER NOT NULL,
  PRIMARY KEY(creator_id, year)
);

CREATE TABLE IF NOT EXISTS stat_month_counts (
  ym             TEXT PRIMARY KEY,
  image_count     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS stat_year_counts (
  year           TEXT PRIMARY KEY,
  image_count    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS stat_tag_counts (
  tag_canonical  TEXT PRIMARY KEY,
  image_count    INTEGER NOT NULL,
  category       INTEGER
);
"""