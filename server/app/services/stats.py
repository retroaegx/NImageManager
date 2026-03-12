from __future__ import annotations

import sqlite3
from typing import Iterable


def rebuild_all(conn: sqlite3.Connection) -> None:
    """Rebuild stat_* cache tables from current DB state.

    This is an admin-only heavy operation (full scans). Normal UI must NOT rely
    on full scans; it should query stat_* tables instead.
    """
    rebuild_creators(conn)
    rebuild_software(conn)
    rebuild_creator_software(conn)
    rebuild_day_counts(conn)
    rebuild_month_counts(conn)
    rebuild_year_counts(conn)
    rebuild_creator_day_counts(conn)
    rebuild_creator_month_counts(conn)
    rebuild_creator_year_counts(conn)
    rebuild_tag_counts(conn)


def _rebuild_grouped_counts(
    conn: sqlite3.Connection,
    *,
    table: str,
    insert_cols: list[str],
    select_cols: list[str],
    from_sql: str,
    where_sql: str = "",
    group_by: list[str],
) -> None:
    conn.execute(f"DELETE FROM {table}")
    insert_sql = ", ".join(insert_cols + ["image_count"])
    select_sql = ", ".join(select_cols + ["COUNT(*) AS image_count"])
    group_sql = ", ".join(group_by)
    sql = (
        f"INSERT INTO {table}({insert_sql}) "
        f"SELECT {select_sql} {from_sql}"
    )
    if where_sql:
        sql += f" WHERE {where_sql}"
    sql += f" GROUP BY {group_sql}"
    conn.execute(sql)


def rebuild_creators(conn: sqlite3.Connection) -> None:
    _rebuild_grouped_counts(
        conn,
        table="stat_creators",
        insert_cols=["creator"],
        select_cols=["users.username AS creator"],
        from_sql="FROM images JOIN users ON users.id = images.uploader_user_id",
        group_by=["users.username"],
    )


def rebuild_software(conn: sqlite3.Connection) -> None:
    _rebuild_grouped_counts(
        conn,
        table="stat_software",
        insert_cols=["software"],
        select_cols=["software"],
        from_sql="FROM images",
        where_sql="software IS NOT NULL AND TRIM(software) <> ''",
        group_by=["software"],
    )


def rebuild_creator_software(conn: sqlite3.Connection) -> None:
    """Rebuild per-creator software counts.

    Used by the sidebar software filter counts (must respect visibility by summing
    visible creators on the fly).
    """
    _rebuild_grouped_counts(
        conn,
        table="stat_creator_software",
        insert_cols=["creator_id", "software"],
        select_cols=["uploader_user_id AS creator_id", "software"],
        from_sql="FROM images",
        where_sql="software IS NOT NULL AND TRIM(software) <> ''",
        group_by=["uploader_user_id", "software"],
    )


def rebuild_day_counts(conn: sqlite3.Connection) -> None:
    _rebuild_grouped_counts(
        conn,
        table="stat_day_counts",
        insert_cols=["ymd"],
        select_cols=["SUBSTR(file_mtime_utc,1,10) AS ymd"],
        from_sql="FROM images",
        where_sql="file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 10",
        group_by=["SUBSTR(file_mtime_utc,1,10)"],
    )


def rebuild_creator_day_counts(conn: sqlite3.Connection) -> None:
    _rebuild_grouped_counts(
        conn,
        table="stat_creator_day_counts",
        insert_cols=["creator_id", "ymd"],
        select_cols=["uploader_user_id AS creator_id", "SUBSTR(file_mtime_utc,1,10) AS ymd"],
        from_sql="FROM images",
        where_sql="file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 10",
        group_by=["uploader_user_id", "SUBSTR(file_mtime_utc,1,10)"],
    )


def rebuild_month_counts(conn: sqlite3.Connection) -> None:
    _rebuild_grouped_counts(
        conn,
        table="stat_month_counts",
        insert_cols=["ym"],
        select_cols=["SUBSTR(file_mtime_utc,1,7) AS ym"],
        from_sql="FROM images",
        where_sql="file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 7",
        group_by=["SUBSTR(file_mtime_utc,1,7)"],
    )


def rebuild_creator_month_counts(conn: sqlite3.Connection) -> None:
    _rebuild_grouped_counts(
        conn,
        table="stat_creator_month_counts",
        insert_cols=["creator_id", "ym"],
        select_cols=["uploader_user_id AS creator_id", "SUBSTR(file_mtime_utc,1,7) AS ym"],
        from_sql="FROM images",
        where_sql="file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 7",
        group_by=["uploader_user_id", "SUBSTR(file_mtime_utc,1,7)"],
    )


def rebuild_year_counts(conn: sqlite3.Connection) -> None:
    _rebuild_grouped_counts(
        conn,
        table="stat_year_counts",
        insert_cols=["year"],
        select_cols=["SUBSTR(file_mtime_utc,1,4) AS year"],
        from_sql="FROM images",
        where_sql="file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 4",
        group_by=["SUBSTR(file_mtime_utc,1,4)"],
    )


def rebuild_creator_year_counts(conn: sqlite3.Connection) -> None:
    _rebuild_grouped_counts(
        conn,
        table="stat_creator_year_counts",
        insert_cols=["creator_id", "year"],
        select_cols=["uploader_user_id AS creator_id", "SUBSTR(file_mtime_utc,1,4) AS year"],
        from_sql="FROM images",
        where_sql="file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 4",
        group_by=["uploader_user_id", "SUBSTR(file_mtime_utc,1,4)"],
    )


def rebuild_tag_counts(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM stat_tag_counts")
    conn.execute(
        """
        INSERT INTO stat_tag_counts(tag_canonical, image_count, category)
        SELECT tag_canonical,
               COUNT(DISTINCT image_id) AS image_count,
               MAX(category) AS category
        FROM image_tags
        GROUP BY tag_canonical
        """
    )


def recompute_dedup_flags(conn: sqlite3.Connection) -> None:
    """Recompute dedup_flag from main_sig_hash.

    Semantics (project requirement):
    - If a signature has never appeared before, that image is 1.
    - If the same signature appears multiple times, keep **one representative** as 1
      (oldest / smallest id), and mark the rest as 2.

    UI rule: the gallery's "メインプロンプト重複排除" shows only dedup_flag=1.
    """
    conn.execute("UPDATE images SET dedup_flag = 1")

    # Mark duplicates as 2, except one representative (MIN(id)) per signature.
    conn.execute(
        """
        UPDATE images
        SET dedup_flag = 2
        WHERE main_sig_hash IS NOT NULL
          AND TRIM(main_sig_hash) <> ''
          AND main_sig_hash IN (
            SELECT main_sig_hash
            FROM images
            WHERE main_sig_hash IS NOT NULL AND TRIM(main_sig_hash) <> ''
            GROUP BY main_sig_hash
            HAVING COUNT(*) > 1
          )
          AND id NOT IN (
            SELECT MIN(id)
            FROM images
            WHERE main_sig_hash IS NOT NULL AND TRIM(main_sig_hash) <> ''
            GROUP BY main_sig_hash
            HAVING COUNT(*) > 1
          )
        """
    )


def recompute_dedup_flags_for_hashes(conn: sqlite3.Connection, hashes: Iterable[str]) -> None:
    """Incremental recompute for a limited set of signatures.

    This avoids full-table scans during admin reparse batches.

    Strategy:
    - For touched hashes: set all to 1
    - For hashes whose group size > 1: set non-representatives (except MIN(id)) to 2
    """
    hs = [h for h in (hashes or []) if isinstance(h, str) and h.strip()]
    if not hs:
        return

    # SQLite has a variable limit (~999). Chunk to be safe.
    CHUNK = 400
    for i in range(0, len(hs), CHUNK):
        chunk = hs[i:i+CHUNK]
        qmarks = ",".join(["?"] * len(chunk))
        conn.execute(f"UPDATE images SET dedup_flag = 1 WHERE main_sig_hash IN ({qmarks})", chunk)

        # Representative ids for hashes that have duplicates
        rows = conn.execute(
            f"""
            SELECT main_sig_hash, MIN(id) AS rep_id, COUNT(*) AS cnt
            FROM images
            WHERE main_sig_hash IN ({qmarks})
            GROUP BY main_sig_hash
            HAVING COUNT(*) > 1
            """,
            chunk,
        ).fetchall()
        rep_ids = [int(r["rep_id"]) for r in rows if r and r["rep_id"] is not None]
        if not rep_ids:
            continue

        # Mark all duplicates except the representative.
        q_rep = ",".join(["?"] * len(rep_ids))
        conn.execute(
            f"UPDATE images SET dedup_flag = 2 WHERE main_sig_hash IN ({qmarks}) AND id NOT IN ({q_rep})",
            chunk + rep_ids,
        )


def _bump_counter(conn: sqlite3.Connection, table: str, key_cols: tuple[str, ...], key_vals: tuple, *, category: int | None = None) -> None:
    if len(key_cols) != len(key_vals):
        raise ValueError("key column/value length mismatch")
    placeholders = ", ".join(["?"] * len(key_cols))
    cols_sql = ", ".join(key_cols)
    update_where = " AND ".join([f"{col} = ?" for col in key_cols])
    if category is None:
        conn.execute(
            f"""
            INSERT INTO {table}({cols_sql}, image_count)
            VALUES ({placeholders}, 1)
            ON CONFLICT({cols_sql}) DO UPDATE SET image_count = image_count + 1
            """,
            key_vals,
        )
        return
    conn.execute(
        f"""
        INSERT INTO {table}({cols_sql}, image_count, category)
        VALUES ({placeholders}, 1, ?)
        ON CONFLICT({cols_sql}) DO UPDATE SET
          image_count = image_count + 1,
          category = COALESCE({table}.category, excluded.category)
        """,
        key_vals + (category,),
    )


def _dec_counter(conn: sqlite3.Connection, table: str, key_cols: tuple[str, ...], key_vals: tuple) -> None:
    if len(key_cols) != len(key_vals):
        raise ValueError("key column/value length mismatch")
    update_where = " AND ".join([f"{col} = ?" for col in key_cols])
    conn.execute(
        f"UPDATE {table} SET image_count = image_count - 1 WHERE {update_where}",
        key_vals,
    )
    conn.execute(
        f"DELETE FROM {table} WHERE {update_where} AND image_count <= 0",
        key_vals,
    )


def bump_creator(conn: sqlite3.Connection, creator: str) -> None:
    if not creator:
        return
    _bump_counter(conn, "stat_creators", ("creator",), (creator,))


def dec_creator(conn: sqlite3.Connection, creator: str) -> None:
    if not creator:
        return
    _dec_counter(conn, "stat_creators", ("creator",), (creator,))


def bump_software(conn: sqlite3.Connection, software: str) -> None:
    if not software:
        return
    _bump_counter(conn, "stat_software", ("software",), (software,))


def dec_software(conn: sqlite3.Connection, software: str) -> None:
    if not software:
        return
    _dec_counter(conn, "stat_software", ("software",), (software,))


def bump_creator_software(conn: sqlite3.Connection, creator_id: int, software: str) -> None:
    if not software:
        return
    cid = int(creator_id or 0)
    if cid <= 0:
        return
    _bump_counter(conn, "stat_creator_software", ("creator_id", "software"), (cid, software))


def dec_creator_software(conn: sqlite3.Connection, creator_id: int, software: str) -> None:
    if not software:
        return
    cid = int(creator_id or 0)
    if cid <= 0:
        return
    _dec_counter(conn, "stat_creator_software", ("creator_id", "software"), (cid, software))


def bump_day(conn: sqlite3.Connection, ymd: str) -> None:
    if not ymd:
        return
    _bump_counter(conn, "stat_day_counts", ("ymd",), (ymd,))


def bump_creator_day(conn: sqlite3.Connection, creator_id: int, ymd: str) -> None:
    if not ymd:
        return
    cid = int(creator_id or 0)
    if cid <= 0:
        return
    _bump_counter(conn, "stat_creator_day_counts", ("creator_id", "ymd"), (cid, ymd))


def dec_day(conn: sqlite3.Connection, ymd: str) -> None:
    if not ymd:
        return
    _dec_counter(conn, "stat_day_counts", ("ymd",), (ymd,))


def dec_creator_day(conn: sqlite3.Connection, creator_id: int, ymd: str) -> None:
    if not ymd:
        return
    cid = int(creator_id or 0)
    if cid <= 0:
        return
    _dec_counter(conn, "stat_creator_day_counts", ("creator_id", "ymd"), (cid, ymd))


def bump_month(conn: sqlite3.Connection, ym: str) -> None:
    if not ym:
        return
    _bump_counter(conn, "stat_month_counts", ("ym",), (ym,))


def bump_creator_month(conn: sqlite3.Connection, creator_id: int, ym: str) -> None:
    if not ym:
        return
    cid = int(creator_id or 0)
    if cid <= 0:
        return
    _bump_counter(conn, "stat_creator_month_counts", ("creator_id", "ym"), (cid, ym))


def dec_month(conn: sqlite3.Connection, ym: str) -> None:
    if not ym:
        return
    _dec_counter(conn, "stat_month_counts", ("ym",), (ym,))


def dec_creator_month(conn: sqlite3.Connection, creator_id: int, ym: str) -> None:
    if not ym:
        return
    cid = int(creator_id or 0)
    if cid <= 0:
        return
    _dec_counter(conn, "stat_creator_month_counts", ("creator_id", "ym"), (cid, ym))


def bump_year(conn: sqlite3.Connection, year: str) -> None:
    if not year:
        return
    _bump_counter(conn, "stat_year_counts", ("year",), (year,))


def bump_creator_year(conn: sqlite3.Connection, creator_id: int, year: str) -> None:
    if not year:
        return
    cid = int(creator_id or 0)
    if cid <= 0:
        return
    _bump_counter(conn, "stat_creator_year_counts", ("creator_id", "year"), (cid, year))


def dec_year(conn: sqlite3.Connection, year: str) -> None:
    if not year:
        return
    _dec_counter(conn, "stat_year_counts", ("year",), (year,))


def dec_creator_year(conn: sqlite3.Connection, creator_id: int, year: str) -> None:
    if not year:
        return
    cid = int(creator_id or 0)
    if cid <= 0:
        return
    _dec_counter(conn, "stat_creator_year_counts", ("creator_id", "year"), (cid, year))


def bump_tag(conn: sqlite3.Connection, tag: str, category: int | None) -> None:
    if not tag:
        return
    _bump_counter(conn, "stat_tag_counts", ("tag_canonical",), (tag,), category=category)


def dec_tag(conn: sqlite3.Connection, tag: str) -> None:
    if not tag:
        return
    _dec_counter(conn, "stat_tag_counts", ("tag_canonical",), (tag,))
