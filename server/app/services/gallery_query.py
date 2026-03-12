from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import sqlite3
from typing import Callable


@dataclass(slots=True)
class GalleryFilters:
    creator: str | None
    software: str | None
    tag_list: list[str]
    tag_not_list: list[str]
    date_from: str | None
    date_to: str | None
    dedup_only: int
    bm_any: int
    bm_list_id: int | None
    sort_key: str


def normalize_sort(sort: str | None) -> str:
    sort_key = (sort or "newest").lower()
    if sort_key not in {"newest", "oldest", "favorite"}:
        return "newest"
    return sort_key


def normalize_tag_filters(
    tags: str | None,
    tags_not: str | None,
    *,
    normalize_tag: Callable[[str], str],
) -> tuple[list[str], list[str]]:
    tag_list: list[str] = []
    tag_not_list: list[str] = []
    if tags:
        tag_list = [normalize_tag(t) for t in tags.split(",") if t.strip()]
        tag_list = list(dict.fromkeys(tag_list))
    if tags_not:
        tag_not_list = [normalize_tag(t) for t in tags_not.split(",") if t.strip()]
        tag_not_list = list(dict.fromkeys(tag_not_list))
    if tag_list and tag_not_list:
        inc = set(tag_list)
        tag_not_list = [t for t in tag_not_list if t not in inc]
    return tag_list, tag_not_list


def normalize_gallery_filters(
    *,
    creator: str | None,
    software: str | None,
    tags: str | None,
    tags_not: str | None,
    date_from: str | None,
    date_to: str | None,
    dedup_only: int,
    bm_any: int,
    bm_list_id: int | None,
    fav_only: int,
    sort: str | None,
    normalize_tag: Callable[[str], str],
) -> GalleryFilters:
    tag_list, tag_not_list = normalize_tag_filters(tags, tags_not, normalize_tag=normalize_tag)
    return GalleryFilters(
        creator=creator or None,
        software=software or None,
        tag_list=tag_list,
        tag_not_list=tag_not_list,
        date_from=date_from or None,
        date_to=date_to or None,
        dedup_only=1 if int(dedup_only or 0) else 0,
        bm_any=int(bm_any or fav_only or 0),
        bm_list_id=bm_list_id,
        sort_key=normalize_sort(sort),
    )


def resolve_creator_id(conn: sqlite3.Connection, creator: str | None) -> tuple[bool, int | None]:
    if not creator:
        return True, None
    row = conn.execute("SELECT id FROM users WHERE username=?", (creator,)).fetchone()
    if not row:
        return False, None
    return True, int(row["id"])


def normalize_bookmark_list_id(
    conn: sqlite3.Connection,
    *,
    viewer: dict,
    bm_list_id: int | None,
    can_view_bookmark_list: Callable[[sqlite3.Connection, dict, int], bool],
) -> int | None:
    if bm_list_id is None:
        return None
    try:
        lid = int(bm_list_id)
    except Exception:
        return None
    if lid <= 0 or not can_view_bookmark_list(conn, viewer, lid):
        return None
    return lid


def build_user_bookmark_join(uid: int, bm_list_id: int | None) -> tuple[str, list]:
    join_sql = """
    LEFT JOIN (
      SELECT DISTINCT b.image_id AS image_id, 1 AS bm
      FROM bookmarks b
      JOIN bookmark_lists bl ON bl.id=b.list_id
      WHERE bl.user_id=?
    ) ubm ON ubm.image_id = images.id
    """
    join_params: list = [uid]
    if bm_list_id is not None:
        join_sql += """
        JOIN (
          SELECT DISTINCT image_id AS image_id
          FROM bookmarks
          WHERE list_id=?
        ) lbm ON lbm.image_id = images.id
        """
        join_params.append(int(bm_list_id))
    return join_sql, join_params


def apply_common_filters(
    conn: sqlite3.Connection,
    *,
    filters: GalleryFilters,
    creator_id: int | None,
    where: list[str],
    params: list,
    viewer: dict,
    apply_tag_filters: Callable[[sqlite3.Connection, list[str], list, list[str], list[str], str], None],
    append_visibility_filter: Callable[[list[str], list, dict], None],
) -> None:
    if creator_id is not None:
        where.append("images.uploader_user_id = ?")
        params.append(creator_id)
    if filters.software:
        where.append("images.software = ?")
        params.append(filters.software)
    if filters.date_from:
        where.append("images.file_mtime_utc >= ?")
        params.append(filters.date_from)
    if filters.date_to:
        try:
            dt = datetime.strptime(filters.date_to, "%Y-%m-%d") + timedelta(days=1)
            where.append("images.file_mtime_utc < ?")
            params.append(dt.strftime("%Y-%m-%d"))
        except Exception:
            where.append("substr(images.file_mtime_utc,1,10) <= ?")
            params.append(filters.date_to)
    if filters.dedup_only:
        where.append("images.dedup_flag = 1")
    if filters.bm_any and filters.bm_list_id is None:
        where.append("ubm.bm = 1")
    apply_tag_filters(conn, where, params, filters.tag_list, filters.tag_not_list, "images")
    append_visibility_filter(where, params, viewer)
