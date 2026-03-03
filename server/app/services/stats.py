from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Iterable


def rebuild_all(conn: sqlite3.Connection) -> None:
    """Rebuild stat_* cache tables from current DB state.

    This is an admin-only heavy operation (full scans). Normal UI must NOT rely
    on full scans; it should query stat_* tables instead.
    """
    rebuild_creators(conn)
    rebuild_software(conn)
    rebuild_day_counts(conn)
    rebuild_month_counts(conn)
    rebuild_year_counts(conn)
    rebuild_tag_counts(conn)


def rebuild_creators(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM stat_creators")
    conn.execute(
        """
        INSERT INTO stat_creators(creator, image_count)
        SELECT users.username AS creator, COUNT(*) AS image_count
        FROM images
        JOIN users ON users.id = images.uploader_user_id
        GROUP BY users.username
        """
    )


def rebuild_software(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM stat_software")
    conn.execute(
        """
        INSERT INTO stat_software(software, image_count)
        SELECT software, COUNT(*) AS image_count
        FROM images
        WHERE software IS NOT NULL AND TRIM(software) <> ''
        GROUP BY software
        """
    )


def rebuild_day_counts(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM stat_day_counts")
    conn.execute(
        """
        INSERT INTO stat_day_counts(ymd, image_count)
        SELECT SUBSTR(file_mtime_utc,1,10) AS ymd, COUNT(*) AS image_count
        FROM images
        WHERE file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 10
        GROUP BY SUBSTR(file_mtime_utc,1,10)
        """
    )

def rebuild_month_counts(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM stat_month_counts")
    conn.execute(
        """
        INSERT INTO stat_month_counts(ym, image_count)
        SELECT SUBSTR(file_mtime_utc,1,7) AS ym, COUNT(*) AS image_count
        FROM images
        WHERE file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 7
        GROUP BY SUBSTR(file_mtime_utc,1,7)
        """
    )


def rebuild_year_counts(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM stat_year_counts")
    conn.execute(
        """
        INSERT INTO stat_year_counts(year, image_count)
        SELECT SUBSTR(file_mtime_utc,1,4) AS year, COUNT(*) AS image_count
        FROM images
        WHERE file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 4
        GROUP BY SUBSTR(file_mtime_utc,1,4)
        """
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


def bump_creator(conn: sqlite3.Connection, creator: str) -> None:
    if not creator:
        return
    conn.execute(
        """
        INSERT INTO stat_creators(creator, image_count)
        VALUES (?, 1)
        ON CONFLICT(creator) DO UPDATE SET image_count = image_count + 1
        """,
        (creator,),
    )


def dec_creator(conn: sqlite3.Connection, creator: str) -> None:
    if not creator:
        return
    conn.execute(
        "UPDATE stat_creators SET image_count = image_count - 1 WHERE creator = ?",
        (creator,),
    )
    conn.execute(
        "DELETE FROM stat_creators WHERE creator = ? AND image_count <= 0",
        (creator,),
    )

def bump_software(conn: sqlite3.Connection, software: str) -> None:
    if not software:
        return
    conn.execute(
        """
        INSERT INTO stat_software(software, image_count)
        VALUES (?, 1)
        ON CONFLICT(software) DO UPDATE SET image_count = image_count + 1
        """,
        (software,),
    )


def dec_software(conn: sqlite3.Connection, software: str) -> None:
    if not software:
        return
    conn.execute(
        "UPDATE stat_software SET image_count = image_count - 1 WHERE software = ?",
        (software,),
    )
    conn.execute(
        "DELETE FROM stat_software WHERE software = ? AND image_count <= 0",
        (software,),
    )

def bump_day(conn: sqlite3.Connection, ymd: str) -> None:
    if not ymd:
        return
    conn.execute(
        """
        INSERT INTO stat_day_counts(ymd, image_count)
        VALUES (?, 1)
        ON CONFLICT(ymd) DO UPDATE SET image_count = image_count + 1
        """,
        (ymd,),
    )


def dec_day(conn: sqlite3.Connection, ymd: str) -> None:
    if not ymd:
        return
    conn.execute(
        "UPDATE stat_day_counts SET image_count = image_count - 1 WHERE ymd = ?",
        (ymd,),
    )
    conn.execute(
        "DELETE FROM stat_day_counts WHERE ymd = ? AND image_count <= 0",
        (ymd,),
    )

def bump_month(conn: sqlite3.Connection, ym: str) -> None:
    if not ym:
        return
    conn.execute(
        """
        INSERT INTO stat_month_counts(ym, image_count)
        VALUES (?, 1)
        ON CONFLICT(ym) DO UPDATE SET image_count = image_count + 1
        """
        ,
        (ym,),
    )


def dec_month(conn: sqlite3.Connection, ym: str) -> None:
    if not ym:
        return
    conn.execute("UPDATE stat_month_counts SET image_count = image_count - 1 WHERE ym = ?", (ym,))
    conn.execute("DELETE FROM stat_month_counts WHERE ym = ? AND image_count <= 0", (ym,))


def bump_year(conn: sqlite3.Connection, year: str) -> None:
    if not year:
        return
    conn.execute(
        """
        INSERT INTO stat_year_counts(year, image_count)
        VALUES (?, 1)
        ON CONFLICT(year) DO UPDATE SET image_count = image_count + 1
        """
        ,
        (year,),
    )


def dec_year(conn: sqlite3.Connection, year: str) -> None:
    if not year:
        return
    conn.execute("UPDATE stat_year_counts SET image_count = image_count - 1 WHERE year = ?", (year,))
    conn.execute("DELETE FROM stat_year_counts WHERE year = ? AND image_count <= 0", (year,))


def bump_tag(conn: sqlite3.Connection, tag: str, category: int | None) -> None:
    if not tag:
        return
    conn.execute(
        """
        INSERT INTO stat_tag_counts(tag_canonical, image_count, category)
        VALUES (?, 1, ?)
        ON CONFLICT(tag_canonical) DO UPDATE SET
          image_count = image_count + 1,
          category = COALESCE(stat_tag_counts.category, excluded.category)
        """,
        (tag, category),
    )


def dec_tag(conn: sqlite3.Connection, tag: str) -> None:
    if not tag:
        return
    conn.execute(
        "UPDATE stat_tag_counts SET image_count = image_count - 1 WHERE tag_canonical = ?",
        (tag,),
    )
    conn.execute(
        "DELETE FROM stat_tag_counts WHERE tag_canonical = ? AND image_count <= 0",
        (tag,),
    )
