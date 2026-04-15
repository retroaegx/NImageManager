from __future__ import annotations

import hashlib
import datetime as dt
import traceback
import importlib
import io
import json
import os
import re
import secrets
import shutil
import sys
import tempfile
import time
import unittest
import warnings
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from urllib.parse import parse_qs, urlparse

from fastapi.testclient import TestClient
from PIL import Image, PngImagePlugin


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PREFIXES = ("server", "shared")


warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=ResourceWarning)


class RuntimeErrorWithContext(RuntimeError):
    pass


@dataclass
class Runtime:
    temp_root: Path | None = None
    main: ModuleType | None = None
    api: ModuleType | None = None
    db: ModuleType | None = None
    stats: ModuleType | None = None
    client_cm: TestClient | None = None
    client: TestClient | None = None
    tokens: dict[str, str] | None = None
    user_ids: dict[str, int] | None = None

    def __enter__(self) -> "Runtime":
        self.tokens = {}
        self.user_ids = {}
        self.temp_root = self._copy_project()
        self._purge_modules()
        sys.path.insert(0, str(self.temp_root))
        os.environ["NAI_IM_UPDATE_CHECK_ENABLED"] = "0"

        self.main = importlib.import_module("server.app.main")
        self.api = importlib.import_module("server.app.api")
        self.db = importlib.import_module("server.app.db")
        self.stats = importlib.import_module("server.app.services.stats")

        # Background workers make tests flaky and are not needed here.
        self.main.api_module.start_background_workers = lambda: None
        self.main.api_module.stop_background_workers = lambda: None
        self.main.start_update_checker = lambda: None
        self.main.stop_update_checker = lambda: None
        self.main.app.user_middleware = [m for m in self.main.app.user_middleware if getattr(m.cls, "__name__", "") != "GZipMiddleware"]
        self.main.app.middleware_stack = self.main.app.build_middleware_stack()

        self.client_cm = TestClient(self.main.app)
        self.client = self.client_cm.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.client_cm is not None:
            self.client_cm.__exit__(exc_type, exc, tb)
        if self.temp_root is not None:
            if str(self.temp_root) in sys.path:
                sys.path.remove(str(self.temp_root))
            shutil.rmtree(self.temp_root, ignore_errors=True)
        self._purge_modules()

    def _copy_project(self) -> Path:
        base = Path(tempfile.mkdtemp(prefix="nim_test_runtime_")) / "NImageManager"
        base.mkdir(parents=True, exist_ok=True)
        shutil.copytree(PROJECT_ROOT / "server", base / "server", ignore=shutil.ignore_patterns("data", "__pycache__", "*.pyc"))
        shutil.copytree(PROJECT_ROOT / "shared", base / "shared", ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        shutil.copy2(PROJECT_ROOT / "VERSION", base / "VERSION")
        (base / "server" / "data").mkdir(parents=True, exist_ok=True)
        return base

    def _purge_modules(self) -> None:
        doomed = []
        for name in list(sys.modules.keys()):
            if name == "server" or name.startswith("server.") or name == "shared" or name.startswith("shared."):
                doomed.append(name)
        for name in doomed:
            sys.modules.pop(name, None)

    def request(self, method: str, path: str, *, user: str | None = None, **kwargs):
        assert self.client is not None
        headers = dict(kwargs.pop("headers", {}) or {})
        if user is not None:
            token = (self.tokens or {}).get(user)
            if not token:
                raise RuntimeErrorWithContext(f"No token for user: {user}")
            headers["Authorization"] = f"Bearer {token}"
        target = path if path.startswith("/api/") else f"/api{path}"
        return self.client.request(method=method, url=target, headers=headers, **kwargs)

    def web_request(self, method: str, path: str, *, user: str | None = None, **kwargs):
        assert self.client is not None
        headers = dict(kwargs.pop("headers", {}) or {})
        if user is not None:
            token = (self.tokens or {}).get(user)
            if not token:
                raise RuntimeErrorWithContext(f"No token for user: {user}")
            headers["Authorization"] = f"Bearer {token}"
        return self.client.request(method=method, url=path, headers=headers, **kwargs)

    def json(self, method: str, path: str, *, user: str | None = None, payload: dict | None = None, **kwargs):
        return self.request(method, path, user=user, json=(payload or {}), **kwargs)

    def setup_master(self, username: str = "master", password: str = "masterpw") -> int:
        resp = self.json("POST", "/auth/setup_master", payload={
            "username": username,
            "password": password,
            "password2": password,
        })
        expect_status(resp, 200)
        data = resp.json()
        self.tokens[username] = data["token"]
        self.user_ids[username] = int(data["user"]["id"])
        return int(data["user"]["id"])

    def login(self, username: str, password: str) -> str:
        resp = self.json("POST", "/auth/login", payload={"username": username, "password": password})
        expect_status(resp, 200)
        data = resp.json()
        self.tokens[username] = data["token"]
        self.user_ids[username] = int(data["user"]["id"])
        return data["token"]

    def create_user(self, *, actor: str, username: str, role: str, password: str | None = None) -> int:
        password = password or f"{username}_pw"
        resp = self.json("POST", "/admin/users", user=actor, payload={"username": username, "role": role})
        expect_status(resp, 200)
        setup_url = resp.json()["setup_url"]
        token = parse_token_from_url(setup_url)
        info = self.request("GET", "/auth/password_tokens/info", params={"token": token})
        expect_status(info, 200)
        consume = self.json("POST", "/auth/password_tokens/consume", payload={
            "token": token,
            "password": password,
            "password2": password,
        })
        expect_status(consume, 200)
        self.login(username, password)
        return int(self.user_ids[username])

    def update_sharing(self, user: str, *, share_works: int | None = None, share_bookmarks: int | None = None) -> dict:
        payload: dict[str, int] = {}
        if share_works is not None:
            payload["share_works"] = int(share_works)
        if share_bookmarks is not None:
            payload["share_bookmarks"] = int(share_bookmarks)
        resp = self.json("POST", "/me/settings", user=user, payload=payload)
        expect_status(resp, 200)
        return resp.json()

    def get_default_bookmark_list_id(self, user: str) -> int:
        resp = self.request("GET", "/bookmarks/lists", user=user)
        expect_status(resp, 200)
        lists = resp.json()["lists"]
        for row in lists:
            if int(row["is_default"] or 0) == 1:
                return int(row["id"])
        raise RuntimeErrorWithContext(f"Default list not found for {user}")

    def create_bookmark_list(self, user: str, name: str) -> int:
        resp = self.json("POST", "/bookmarks/lists", user=user, payload={"name": name})
        expect_status(resp, 200)
        return int(resp.json()["list_id"])

    def add_creator(self, user: str, target_user: str) -> None:
        resp = self.json("POST", "/creators/list", user=user, payload={"user_id": int(self.user_ids[target_user])})
        expect_status(resp, 200)

    def add_bookmark_subscription(self, user: str, target_user: str) -> None:
        resp = self.json("POST", "/bookmarks/subscriptions", user=user, payload={"user_id": int(self.user_ids[target_user])})
        expect_status(resp, 200)

    def refresh_stats(self) -> None:
        assert self.db is not None and self.stats is not None
        conn = self.db.get_conn()
        try:
            self.stats.rebuild_all(conn)
            self.stats.rebuild_creator_day_counts(conn)
            self.stats.rebuild_creator_month_counts(conn)
            self.stats.rebuild_creator_year_counts(conn)
            self.stats.recompute_dedup_flags(conn)
            conn.commit()
        finally:
            conn.close()

    def make_png_bytes(self, *, size: tuple[int, int] = (96, 72), color: tuple[int, int, int] = (64, 128, 192)) -> bytes:
        image = Image.new("RGB", size, color)
        buf = io.BytesIO()
        image.save(buf, format="PNG")
        return buf.getvalue()

    def _make_derivative_bytes(self, *, size: tuple[int, int], color: tuple[int, int, int]) -> bytes:
        image = Image.new("RGB", size, color)
        buf = io.BytesIO()
        image.save(buf, format="WEBP", quality=85)
        return buf.getvalue()

    def seed_image(
        self,
        *,
        owner: str,
        filename: str,
        size: tuple[int, int] = (96, 72),
        color: tuple[int, int, int] = (64, 128, 192),
        software: str = "NovelAI",
        model: str = "nai-test-model",
        tags: list[tuple[str, int | None]] | None = None,
        prompt_positive: str = "masterpiece",
        prompt_negative: str = "bad anatomy",
        prompt_character: str = "",
        potion: bool = True,
        file_mtime_utc: str = "2026-03-12T09:28:27+00:00",
        uploaded_at_utc: str = "2026-03-12 09:28:27",
    ) -> int:
        assert self.db is not None and self.api is not None
        tags = tags or [("tag_default", None)]
        raw = self.make_png_bytes(size=size, color=color)
        sha = hashlib.sha256(raw).hexdigest()
        params_json = json.dumps({"seed": 123456}, ensure_ascii=False)
        potion_bytes = json.dumps({"strength": 1}, ensure_ascii=False).encode("utf-8") if potion else None

        conn = self.db.get_conn()
        try:
            cur = conn.execute(
                """
                INSERT INTO images(
                  public_id, sha256, original_filename, ext, mime, width, height,
                  file_mtime_utc, uploaded_at_utc, uploader_user_id,
                  software, model_name, prompt_positive_raw, prompt_negative_raw,
                  prompt_character_raw, character_entries_json, main_negative_combined_raw,
                  seed, params_json, potion_raw, has_potion, metadata_raw,
                  main_sig_hash, full_meta_hash, dedup_flag, favorite, is_nsfw, reparse_skip
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    secrets.token_hex(8),
                    sha,
                    filename,
                    "png",
                    "image/png",
                    int(size[0]),
                    int(size[1]),
                    file_mtime_utc,
                    uploaded_at_utc,
                    int(self.user_ids[owner]),
                    software,
                    model,
                    prompt_positive,
                    prompt_negative,
                    prompt_character,
                    json.dumps([], ensure_ascii=False),
                    prompt_negative,
                    123456,
                    params_json,
                    potion_bytes,
                    1 if potion else 0,
                    json.dumps({"source": "integration-test"}, ensure_ascii=False),
                    hashlib.sha1((prompt_positive + prompt_negative).encode("utf-8")).hexdigest(),
                    hashlib.sha1((prompt_positive + prompt_negative + filename).encode("utf-8")).hexdigest(),
                    1,
                    0,
                    0,
                    0,
                ),
            )
            image_id = int(cur.lastrowid)
            disk_path = self.api._write_original_to_disk(
                image_id=image_id,
                original_filename=filename,
                ext="png",
                sha256=sha,
                raw=raw,
            )
            conn.execute(
                "INSERT INTO image_files(image_id, disk_path, size, bytes) VALUES (?,?,?,NULL)",
                (image_id, disk_path, len(raw)),
            )
            for idx, (tag, category) in enumerate(tags):
                conn.execute(
                    "INSERT OR IGNORE INTO tags_master(tag, category, post_count, sources) VALUES (?,?,?,?)",
                    (tag, category, None, None),
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO image_tags(
                      image_id, tag_canonical, tag_text, tag_raw, category,
                      emphasis_type, brace_level, numeric_weight, src_mask, seq
                    ) VALUES (?,?,?,?,?,'none',0,0,1,?)
                    """,
                    (image_id, tag, tag, tag, category, idx),
                )
            for kind in ("grid", "overlay"):
                quality = self.api._derivative_quality_for(kind, "webp")
                deriv_raw = self._make_derivative_bytes(size=size, color=color)
                self.api._upsert_derivative_file(
                    conn,
                    image_id=image_id,
                    kind=kind,
                    fmt="webp",
                    width=int(size[0]),
                    height=int(size[1]),
                    quality=int(quality),
                    raw=deriv_raw,
                    created_at_utc=uploaded_at_utc,
                )
            conn.commit()
            return image_id
        finally:
            conn.close()

    def insert_upload_job(self, *, owner: str, filename: str = "staged.zip") -> int:
        assert self.db is not None
        staging_dir = Path(self.db.DATA_DIR) / "upload_test_staging" / f"{owner}_{secrets.token_hex(4)}"
        staging_dir.mkdir(parents=True, exist_ok=True)
        (staging_dir / "dummy.txt").write_text("dummy", encoding="utf-8")
        conn = self.db.get_conn()
        try:
            cur = conn.execute(
                """
                INSERT INTO upload_zip_jobs(user_id, filename, source_kind, staging_dir, bookmark_enabled, bookmark_list_id, total, done, failed, dup, status)
                VALUES (?,?,?,?,0,NULL,1,0,0,0,'collecting')
                """,
                (int(self.user_ids[owner]), filename, "direct", str(staging_dir)),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            conn.close()

    def upload_via_api(self, *, user: str, filename: str, color: tuple[int, int, int] = (10, 90, 180), bookmark_enabled: bool = False, bookmark_list_id: int | None = None) -> int:
        raw = self.make_png_bytes(color=color)
        data = {"bookmark_enabled": "1" if bookmark_enabled else "0"}
        if bookmark_list_id is not None:
            data["bookmark_list_id"] = str(bookmark_list_id)
        resp = self.request(
            "POST",
            "/upload",
            user=user,
            data=data,
            files={"file": (filename, raw, "image/png")},
        )
        expect_status(resp, 200)
        image_id = int(resp.json()["image_id"])
        self.ensure_derivatives(image_id=image_id, color=color)
        self.refresh_stats()
        return image_id

    def ensure_derivatives(self, *, image_id: int, color: tuple[int, int, int] = (10, 90, 180)) -> None:
        assert self.db is not None and self.api is not None
        conn = self.db.get_conn()
        try:
            row = conn.execute("SELECT width, height FROM images WHERE id=?", (int(image_id),)).fetchone()
            if not row:
                raise RuntimeErrorWithContext(f"image not found: {image_id}")
            size = (int(row["width"] or 96), int(row["height"] or 72))
            for kind in ("grid", "overlay"):
                quality = self.api._derivative_quality_for(kind, "webp")
                deriv_raw = self._make_derivative_bytes(size=size, color=color)
                self.api._upsert_derivative_file(
                    conn,
                    image_id=int(image_id),
                    kind=kind,
                    fmt="webp",
                    width=size[0],
                    height=size[1],
                    quality=int(quality),
                    raw=deriv_raw,
                    created_at_utc="2026-03-12 09:28:27",
                )
            conn.commit()
        finally:
            conn.close()

    def get_db_row_count(self, sql: str, params: tuple | list = ()) -> int:
        assert self.db is not None
        conn = self.db.get_conn()
        try:
            row = conn.execute(sql, params).fetchone()
            return int(row[0] if isinstance(row, tuple) else list(row)[0])
        finally:
            conn.close()

    def upload_batch_one(self, *, user: str, filename: str = "batch.png") -> int:
        init = self.json("POST", "/upload_batch/init", user=user, payload={"total": 1})
        expect_status(init, 200)
        job_id = int(init.json()["job_id"])
        raw = self.make_png_bytes(color=(190, 40, 70))
        append = self.request(
            "POST",
            f"/upload_batch/{job_id}/append?seq=1&filename={filename}",
            user=user,
            content=raw,
            headers={"content-type": "application/octet-stream"},
        )
        expect_status(append, 200)
        finish = self.request("POST", f"/upload_batch/{job_id}/finish", user=user)
        expect_status(finish, 200)

        # No background worker in tests. Process the queued items inline.
        assert self.api is not None and self.db is not None
        conn = self.db.get_conn()
        try:
            item_ids = [int(r[0] if isinstance(r, tuple) else r["id"]) for r in conn.execute("SELECT id FROM upload_zip_items WHERE job_id=? ORDER BY id", (job_id,)).fetchall()]
        finally:
            conn.close()
        for item_id in item_ids:
            self.api._process_upload_item_job(item_id, source="test")

        status = self.request("GET", f"/upload_zip/{job_id}", user=user)
        expect_status(status, 200)
        items = status.json().get("items") or []
        if not items:
            raise RuntimeErrorWithContext(f"No upload items for job {job_id}")
        image_id = int(items[0]["image_id"])
        self.ensure_derivatives(image_id=image_id, color=(190, 40, 70))
        self.refresh_stats()
        return image_id


def parse_token_from_url(url: str) -> str:
    parsed = urlparse(url)
    token = parse_qs(parsed.query).get("token", [""])[0]
    if not token:
        raise RuntimeErrorWithContext(f"token missing in URL: {url}")
    return token


def expect_status(resp, code: int) -> None:
    if resp.status_code != code:
        body = None
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        raise AssertionError(f"Expected HTTP {code}, got {resp.status_code}: {body}")


class ComprehensiveIntegrationTests(unittest.TestCase):
    maxDiff = None

    def test_ui_regression_contracts(self):
        with Runtime() as rt:
            web_root = rt.temp_root / "server" / "web"
            admin_js = (web_root / "admin.js").read_text(encoding="utf-8")
            settings_js = (web_root / "settings.js").read_text(encoding="utf-8")
            app_js = (web_root / "app.js").read_text(encoding="utf-8")
            index_html = (web_root / "index.html").read_text(encoding="utf-8")
            settings_html = (web_root / "settings.html").read_text(encoding="utf-8")
            page_i18n = (web_root / "lib" / "page-i18n.js").read_text(encoding="utf-8")
            ja_json = json.loads((web_root / "i18n" / "ja.json").read_text(encoding="utf-8"))
            en_json = json.loads((web_root / "i18n" / "en.json").read_text(encoding="utf-8"))

            self.assertIn("confirmAccountDelete", admin_js)
            self.assertIn("tr(", admin_js)
            self.assertIn("confirmMyAccountDelete", settings_js)
            self.assertIn("ui_language", settings_js)
            self.assertIn('id="uiLanguage"', settings_html)
            self.assertIn("UPLOAD_STATE_LABELS", app_js)
            self.assertIn("uploadStateCode", app_js)
            self.assertIn("done", app_js)
            self.assertIn("page-i18n.js", index_html)
            self.assertRegex(index_html, r">\s*プレビュー\s*<")
            self.assertRegex(index_html, r">\s*アップロード\s*<")
            self.assertIn("initI18n()", page_i18n)
            self.assertEqual(ja_json["source"]["Preview"], "プレビュー")
            self.assertEqual(en_json["source"]["プレビュー"], "Preview")
            self.assertNotIn("プレビュー管理", index_html)
            self.assertNotIn("アップロード管理", index_html)

    def test_auth_and_account_lifecycle(self):
        with Runtime() as rt:
            rt.setup_master()
            me = rt.request("GET", "/me", user="master")
            expect_status(me, 200)
            self.assertEqual(me.json()["role"], "master")

            rt.create_user(actor="master", username="admin1", role="admin", password="admin1pw")
            rt.create_user(actor="master", username="user1", role="user", password="user1pw")

            admin_as_admin = rt.json("POST", "/admin/users", user="admin1", payload={"username": "bad_admin", "role": "admin"})
            self.assertEqual(admin_as_admin.status_code, 403)

            create_by_admin = rt.json("POST", "/admin/users", user="admin1", payload={"username": "user2", "role": "user"})
            expect_status(create_by_admin, 200)
            token = parse_token_from_url(create_by_admin.json()["setup_url"])
            consume = rt.json("POST", "/auth/password_tokens/consume", payload={"token": token, "password": "user2pw", "password2": "user2pw"})
            expect_status(consume, 200)
            rt.login("user2", "user2pw")

            self_delete_master = rt.request("DELETE", "/me", user="master")
            self.assertEqual(self_delete_master.status_code, 400)

            disable_self = rt.json("POST", f"/admin/users/{rt.user_ids['admin1']}", user="admin1", payload={"disabled": 1})
            self.assertEqual(disable_self.status_code, 400)

            delete_other_user = rt.request("DELETE", f"/admin/users/{rt.user_ids['user2']}", user="admin1")
            expect_status(delete_other_user, 200)
            login_deleted = rt.json("POST", "/auth/login", payload={"username": "user2", "password": "user2pw"})
            self.assertEqual(login_deleted.status_code, 401)

    def test_visibility_matrix_gallery_sidebar_and_filters(self):
        with Runtime() as rt:
            rt.setup_master()
            rt.create_user(actor="master", username="alice", role="user")
            rt.create_user(actor="master", username="bob", role="user")
            rt.create_user(actor="master", username="carol", role="user")
            rt.create_user(actor="master", username="dave", role="user")

            rt.update_sharing("bob", share_bookmarks=1)
            rt.update_sharing("carol", share_works=1)
            rt.update_sharing("dave", share_works=1)

            alice_img = rt.seed_image(owner="alice", filename="alice.png", color=(30, 120, 200), tags=[("self_tag", None)])
            carol_img = rt.seed_image(owner="carol", filename="carol.png", color=(220, 80, 60), tags=[("carol_tag", None)], software="NovelAI Carol")
            dave_img = rt.seed_image(owner="dave", filename="dave.png", color=(20, 160, 80), tags=[("dave_tag", None)], software="NovelAI Dave")
            rt.refresh_stats()

            rt.add_creator("alice", "carol")
            rt.add_creator("bob", "carol")
            rt.add_creator("bob", "dave")

            bob_default = rt.get_default_bookmark_list_id("bob")
            set_carol = rt.json("PUT", f"/bookmarks/images/{carol_img}", user="bob", payload={"list_ids": [bob_default]})
            expect_status(set_carol, 200)
            set_dave = rt.json("PUT", f"/bookmarks/images/{dave_img}", user="bob", payload={"list_ids": [bob_default]})
            expect_status(set_dave, 200)

            rt.add_bookmark_subscription("alice", "bob")
            rt.update_sharing("dave", share_works=0)

            gallery = rt.request("GET", "/images", user="alice")
            expect_status(gallery, 200)
            visible_ids = {int(item["id"]) for item in gallery.json()["items"]}
            self.assertIn(alice_img, visible_ids)
            self.assertIn(carol_img, visible_ids)
            self.assertNotIn(dave_img, visible_ids)

            creator_list = rt.request("GET", "/creators/list", user="alice")
            expect_status(creator_list, 200)
            creator_names = {row["creator"] for row in creator_list.json()}
            self.assertIn("alice", creator_names)
            self.assertIn("carol", creator_names)
            self.assertNotIn("dave", creator_names)

            creator_filter = rt.request("GET", "/images", user="alice", params={"creator": "carol"})
            expect_status(creator_filter, 200)
            filtered_ids = {int(item["id"]) for item in creator_filter.json()["items"]}
            self.assertEqual(filtered_ids, {carol_img})

            shared_list = rt.request("GET", "/images", user="alice", params={"bm_list_id": bob_default})
            expect_status(shared_list, 200)
            shared_ids = {int(item["id"]) for item in shared_list.json()["items"]}
            self.assertIn(carol_img, shared_ids)
            self.assertNotIn(dave_img, shared_ids)

            sidebar = rt.request("GET", "/bookmarks/sidebar", user="alice")
            expect_status(sidebar, 200)
            others = sidebar.json()["others"]
            self.assertEqual(len(others), 1)
            self.assertEqual(others[0]["creator"], "bob")
            other_list_ids = {int(row["id"]) for row in others[0]["lists"]}
            self.assertIn(bob_default, other_list_ids)

            rt.update_sharing("bob", share_bookmarks=0)
            sidebar_after = rt.request("GET", "/bookmarks/sidebar", user="alice")
            expect_status(sidebar_after, 200)
            self.assertEqual(sidebar_after.json()["others"], [])

            users_suggest = rt.request("GET", "/users/suggest", user="alice", params={"kind": "creators"})
            expect_status(users_suggest, 200)
            suggestion_names = {row["username"] for row in users_suggest.json()["items"]}
            self.assertIn("carol", suggestion_names)
            self.assertNotIn("dave", suggestion_names)

    def test_visibility_for_detail_thumb_overlay_and_downloads(self):
        with Runtime() as rt:
            rt.setup_master()
            rt.create_user(actor="master", username="viewer", role="user")
            rt.create_user(actor="master", username="owner", role="user")
            rt.create_user(actor="master", username="shared", role="user")
            rt.create_user(actor="master", username="hidden", role="user")

            rt.update_sharing("owner", share_bookmarks=1)
            rt.update_sharing("shared", share_works=1)
            rt.update_sharing("hidden", share_works=1)

            shared_img = rt.seed_image(owner="shared", filename="shared.png", color=(50, 140, 220), potion=True)
            hidden_img = rt.seed_image(owner="hidden", filename="hidden.png", color=(210, 80, 70), potion=True)
            rt.refresh_stats()

            rt.add_creator("owner", "shared")
            rt.add_creator("owner", "hidden")
            owner_default = rt.get_default_bookmark_list_id("owner")
            expect_status(rt.json("PUT", f"/bookmarks/images/{shared_img}", user="owner", payload={"list_ids": [owner_default]}), 200)
            expect_status(rt.json("PUT", f"/bookmarks/images/{hidden_img}", user="owner", payload={"list_ids": [owner_default]}), 200)
            rt.add_bookmark_subscription("viewer", "owner")

            rt.update_sharing("hidden", share_works=0)

            detail = rt.request("GET", f"/images/{shared_img}/detail", user="viewer", params={"bm_list_id": owner_default})
            expect_status(detail, 200)
            payload = detail.json()
            self.assertIn(f"bm_list_id={owner_default}", payload["thumb"])
            self.assertIn(f"bm_list_id={owner_default}", payload["overlay"])
            self.assertIn(f"bm_list_id={owner_default}", payload["view_full"])
            self.assertIn(f"bm_list_id={owner_default}", payload["download_file"])
            self.assertIn(f"bm_list_id={owner_default}", payload["download_meta"])

            batch = rt.json("POST", "/images/details", user="viewer", payload={"ids": [shared_img], "bm_list_id": owner_default})
            expect_status(batch, 200)
            self.assertIn(str(shared_img), batch.json()["items"])

            for suffix in (
                f"/images/{shared_img}/thumb?kind=grid&bm_list_id={owner_default}",
                f"/images/{shared_img}/thumb?kind=overlay&bm_list_id={owner_default}",
                f"/images/{shared_img}/view?bm_list_id={owner_default}",
                f"/images/{shared_img}/file?bm_list_id={owner_default}",
                f"/images/{shared_img}/metadata_json?bm_list_id={owner_default}",
            ):
                resp = rt.request("GET", suffix, user="viewer")
                expect_status(resp, 200)
                self.assertGreater(len(resp.content or b""), 0)

            for suffix in (
                f"/images/{hidden_img}/detail?bm_list_id={owner_default}",
                f"/images/{hidden_img}/thumb?kind=grid&bm_list_id={owner_default}",
                f"/images/{hidden_img}/thumb?kind=overlay&bm_list_id={owner_default}",
                f"/images/{hidden_img}/view?bm_list_id={owner_default}",
                f"/images/{hidden_img}/file?bm_list_id={owner_default}",
                f"/images/{hidden_img}/metadata_json?bm_list_id={owner_default}",
            ):
                resp = rt.request("GET", suffix, user="viewer")
                self.assertEqual(resp.status_code, 404, suffix)

            rt.update_sharing("shared", share_works=0)
            after_off = rt.request("GET", f"/images/{shared_img}/detail", user="viewer", params={"bm_list_id": owner_default})
            self.assertEqual(after_off.status_code, 404)

    def test_bookmark_crud_bulk_operations_and_gallery_queries(self):
        with Runtime() as rt:
            rt.setup_master()
            rt.create_user(actor="master", username="usera", role="user")
            rt.update_sharing("usera", share_works=1)
            img1 = rt.seed_image(owner="usera", filename="one.png", color=(100, 30, 150), tags=[("alpha", None)], software="SoftA")
            img2 = rt.seed_image(owner="usera", filename="two.png", color=(120, 50, 170), tags=[("beta", None)], software="SoftA")
            img3 = rt.seed_image(owner="usera", filename="three.png", color=(140, 70, 190), tags=[("beta", None)], software="SoftB")
            rt.refresh_stats()

            default_id = rt.get_default_bookmark_list_id("usera")
            extra_id = rt.create_bookmark_list("usera", "List A")
            rename = rt.request("PATCH", f"/bookmarks/lists/{extra_id}", user="usera", json={"name": "List B"})
            expect_status(rename, 200)
            delete_default = rt.request("DELETE", f"/bookmarks/lists/{default_id}", user="usera")
            self.assertEqual(delete_default.status_code, 400)

            expect_status(rt.json("PUT", f"/bookmarks/images/{img1}", user="usera", payload={"list_ids": [default_id, extra_id]}), 200)
            expect_status(rt.request("POST", f"/bookmarks/images/{img2}/default", user="usera"), 200)
            expect_status(rt.request("POST", f"/images/{img3}/favorite", user="usera", json={"toggle": True}), 200)

            img_bookmarks = rt.request("GET", f"/bookmarks/images/{img1}", user="usera")
            expect_status(img_bookmarks, 200)
            checked = {int(row["id"]) for row in img_bookmarks.json()["lists"] if int(row["checked"] or 0) == 1}
            self.assertEqual(checked, {default_id, extra_id})

            bulk_status = rt.json("POST", "/bookmarks/bulk/status", user="usera", payload={
                "mode": "query",
                "query": {"software": "SoftA"},
                "exclude_ids": [img2],
            })
            expect_status(bulk_status, 200)
            self.assertEqual(int(bulk_status.json()["selected_count"]), 1)

            bulk_apply = rt.json("POST", "/bookmarks/bulk/apply", user="usera", payload={
                "mode": "query",
                "query": {"software": "SoftA"},
                "exclude_ids": [img2],
                "add_list_ids": [extra_id],
                "remove_list_ids": [default_id],
            })
            expect_status(bulk_apply, 200)

            img1_state = rt.request("GET", f"/bookmarks/images/{img1}", user="usera")
            expect_status(img1_state, 200)
            img1_checked = {int(row["id"]) for row in img1_state.json()["lists"] if int(row["checked"] or 0) == 1}
            self.assertEqual(img1_checked, {extra_id})

            clear = rt.request("POST", f"/bookmarks/images/{img3}/clear", user="usera")
            expect_status(clear, 200)
            cleared = rt.request("GET", f"/bookmarks/images/{img3}", user="usera")
            expect_status(cleared, 200)
            self.assertFalse(any(int(row["checked"] or 0) == 1 for row in cleared.json()["lists"]))

            gallery = rt.request("GET", "/images_scroll", user="usera", params={"software": "SoftA", "limit": 2})
            expect_status(gallery, 200)
            returned_ids = {int(item["id"]) for item in gallery.json()["items"]}
            self.assertEqual(returned_ids, {img1, img2})

            software_stats = rt.request("GET", "/stats/software", user="usera")
            expect_status(software_stats, 200)
            software_names = {row["software"] for row in software_stats.json()}
            self.assertEqual(software_names, {"SoftA", "SoftB"})

    def test_upload_routes_and_admin_status_smoke(self):
        with Runtime() as rt:
            rt.setup_master()
            rt.create_user(actor="master", username="uploader", role="user")
            list_id = rt.get_default_bookmark_list_id("uploader")

            direct_image = rt.upload_via_api(user="uploader", filename="direct.png", bookmark_enabled=True, bookmark_list_id=list_id)
            batch_image = rt.upload_batch_one(user="uploader", filename="batch.png")

            gallery = rt.request("GET", "/images", user="uploader")
            expect_status(gallery, 200)
            ids = {int(item["id"]) for item in gallery.json()["items"]}
            self.assertIn(direct_image, ids)
            self.assertIn(batch_image, ids)

            bm_any = rt.request("GET", "/images", user="uploader", params={"bm_any": 1})
            expect_status(bm_any, 200)
            bm_ids = {int(item["id"]) for item in bm_any.json()["items"]}
            self.assertIn(direct_image, bm_ids)

            admin_status = rt.request("GET", "/admin/status", user="master")
            expect_status(admin_status, 200)
            admin_json = admin_status.json()
            self.assertGreaterEqual(int(admin_json["images"]["total"]), 2)
            self.assertGreaterEqual(int(admin_json["derivatives"]["grid"]), 2)
            self.assertGreaterEqual(int(admin_json["derivatives"]["overlay"]), 2)

    def test_bulk_delete_permissions_and_account_delete_cascade(self):
        with Runtime() as rt:
            rt.setup_master()
            rt.create_user(actor="master", username="admin1", role="admin")
            rt.create_user(actor="master", username="owner1", role="user")
            rt.create_user(actor="master", username="viewer1", role="user")
            rt.create_user(actor="master", username="author1", role="user")

            rt.update_sharing("author1", share_works=1)
            owner_img1 = rt.seed_image(owner="owner1", filename="own1.png", color=(90, 10, 10))
            owner_img2 = rt.seed_image(owner="owner1", filename="own2.png", color=(100, 20, 20))
            author_img = rt.seed_image(owner="author1", filename="author.png", color=(20, 100, 20))
            rt.refresh_stats()

            rt.add_creator("owner1", "author1")
            owner_default = rt.get_default_bookmark_list_id("owner1")
            extra = rt.create_bookmark_list("owner1", "Keep")
            expect_status(rt.json("PUT", f"/bookmarks/images/{author_img}", user="owner1", payload={"list_ids": [owner_default, extra]}), 200)
            expect_status(rt.json("PUT", f"/bookmarks/images/{owner_img1}", user="owner1", payload={"list_ids": [owner_default]}), 200)
            rt.insert_upload_job(owner="owner1")
            rt.update_sharing("owner1", share_bookmarks=1)
            rt.add_bookmark_subscription("viewer1", "owner1")

            forbidden = rt.json("POST", "/images/bulk_delete", user="viewer1", payload={"mode": "ids", "ids": [owner_img1]})
            self.assertEqual(forbidden.status_code, 403)

            own_delete = rt.json("POST", "/images/bulk_delete", user="owner1", payload={"mode": "ids", "ids": [owner_img2]})
            expect_status(own_delete, 200)
            self.assertEqual(int(own_delete.json()["deleted"]), 1)

            after_own_delete = rt.request("GET", "/images", user="owner1")
            expect_status(after_own_delete, 200)
            after_ids = {int(item["id"]) for item in after_own_delete.json()["items"]}
            self.assertIn(owner_img1, after_ids)
            self.assertNotIn(owner_img2, after_ids)

            self_delete = rt.request("DELETE", "/me", user="owner1")
            expect_status(self_delete, 200)

            self.assertEqual(rt.get_db_row_count("SELECT COUNT(*) FROM users WHERE username='owner1'"), 0)
            self.assertEqual(rt.get_db_row_count("SELECT COUNT(*) FROM images WHERE uploader_user_id=?", (rt.user_ids["owner1"],)), 0)
            self.assertEqual(rt.get_db_row_count("SELECT COUNT(*) FROM bookmark_lists WHERE user_id=?", (rt.user_ids["owner1"],)), 0)
            self.assertEqual(rt.get_db_row_count("SELECT COUNT(*) FROM user_creators WHERE user_id=? OR creator_user_id=?", (rt.user_ids["owner1"], rt.user_ids["owner1"])), 0)
            self.assertEqual(rt.get_db_row_count("SELECT COUNT(*) FROM user_bookmark_creators WHERE user_id=? OR creator_user_id=?", (rt.user_ids["owner1"], rt.user_ids["owner1"])), 0)
            self.assertEqual(rt.get_db_row_count("SELECT COUNT(*) FROM upload_zip_jobs WHERE user_id=?", (rt.user_ids["owner1"],)), 0)
            self.assertEqual(rt.get_db_row_count("SELECT COUNT(*) FROM password_tokens WHERE created_by=?", (rt.user_ids["owner1"],)), 0)

            login_deleted = rt.json("POST", "/auth/login", payload={"username": "owner1", "password": "owner1_pw"})
            self.assertEqual(login_deleted.status_code, 401)

            master_delete_admin = rt.request("DELETE", f"/admin/users/{rt.user_ids['admin1']}", user="master")
            expect_status(master_delete_admin, 200)
            admin_login = rt.json("POST", "/auth/login", payload={"username": "admin1", "password": "admin1_pw"})
            self.assertEqual(admin_login.status_code, 401)

    def test_metadata_extract_usage_flags_from_nested_scopes(self):
        with Runtime() as rt:
            from server.app.services.metadata_extract import extract_novelai_metadata

            payload = {
                "params": {
                    "reference_image_multiple": ["ref-image"],
                    "director_reference_strengths": [1, 1],
                    "sampler": "k_euler_ancestral",
                }
            }
            image = Image.new("RGB", (64, 64), (40, 120, 200))
            pnginfo = PngImagePlugin.PngInfo()
            pnginfo.add_text("Comment", json.dumps(payload, ensure_ascii=False))

            target = Path(rt.temp_root) / "nested_usage.png"
            image.save(target, format="PNG", pnginfo=pnginfo)

            meta = extract_novelai_metadata(target)
            self.assertTrue(meta.uses_potion)
            self.assertTrue(meta.uses_precise_reference)
            self.assertEqual(meta.sampler, "k_euler_ancestral")

    def test_reparse_updates_usage_flags_from_nested_scopes(self):
        with Runtime() as rt:
            rt.setup_master()
            rt.create_user(actor="master", username="uploader", role="user")

            payload = {
                "params": {
                    "reference_image_multiple": ["ref-image"],
                    "director_reference_strengths": [1, 1],
                    "sampler": "k_euler_ancestral",
                }
            }
            image = Image.new("RGB", (64, 64), (70, 160, 220))
            pnginfo = PngImagePlugin.PngInfo()
            pnginfo.add_text("Comment", json.dumps(payload, ensure_ascii=False))
            buf = io.BytesIO()
            image.save(buf, format="PNG", pnginfo=pnginfo)

            upload = rt.request(
                "POST",
                "/upload",
                user="uploader",
                data={"bookmark_enabled": "0"},
                files={"file": ("nested_usage.png", buf.getvalue(), "image/png")},
            )
            expect_status(upload, 200)
            image_id = int(upload.json()["image_id"])

            conn = rt.db.get_conn()
            try:
                conn.execute(
                    "UPDATE images SET uses_potion=0, uses_precise_reference=0, sampler=NULL WHERE id=?",
                    (image_id,),
                )
                conn.commit()
            finally:
                conn.close()

            reparsed = rt.json("POST", "/admin/reparse_one", user="master", payload={"image_id": image_id})
            expect_status(reparsed, 200)
            self.assertTrue(bool(reparsed.json()["ok"]))

            conn = rt.db.get_conn()
            try:
                row = conn.execute(
                    "SELECT uses_potion, uses_precise_reference, sampler FROM images WHERE id=?",
                    (image_id,),
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(int(row["uses_potion"] or 0), 1)
                self.assertEqual(int(row["uses_precise_reference"] or 0), 1)
                self.assertEqual(row["sampler"], "k_euler_ancestral")
            finally:
                conn.close()

    def test_usage_detection_falls_back_to_params_json(self):
        from server.app.services.metadata_extract import detect_generation_usage_from_storage

        params_json = json.dumps(
            {
                "reference_image_multiple": ["ref-image"],
                "director_reference_strengths": [1, 1],
                "sampler": "k_euler_ancestral",
            },
            ensure_ascii=False,
        )

        uses_potion, uses_precise_reference, sampler = detect_generation_usage_from_storage(params_json)
        self.assertTrue(uses_potion)
        self.assertTrue(uses_precise_reference)
        self.assertEqual(sampler, "k_euler_ancestral")

    def test_detail_usage_fields_fall_back_to_params_json_when_db_is_stale(self):
        with Runtime() as rt:
            rt.setup_master()
            rt.create_user(actor="master", username="uploader", role="user")

            payload = {
                "params": {
                    "reference_image_multiple": ["ref-image"],
                    "director_reference_strengths": [1, 1],
                    "sampler": "k_euler_ancestral",
                }
            }
            image = Image.new("RGB", (64, 64), (90, 150, 230))
            pnginfo = PngImagePlugin.PngInfo()
            pnginfo.add_text("Comment", json.dumps(payload, ensure_ascii=False))
            buf = io.BytesIO()
            image.save(buf, format="PNG", pnginfo=pnginfo)

            upload = rt.request(
                "POST",
                "/upload",
                user="uploader",
                data={"bookmark_enabled": "0"},
                files={"file": ("stale_usage.png", buf.getvalue(), "image/png")},
            )
            expect_status(upload, 200)
            image_id = int(upload.json()["image_id"])

            conn = rt.db.get_conn()
            try:
                conn.execute(
                    "UPDATE images SET has_potion=0, uses_potion=0, uses_precise_reference=0, sampler=NULL WHERE id=?",
                    (image_id,),
                )
                conn.commit()
            finally:
                conn.close()

            detail = rt.json("GET", f"/images/{image_id}/detail", user="uploader")
            expect_status(detail, 200)
            payload = detail.json()
            self.assertTrue(bool(payload["has_potion"]))
            self.assertTrue(bool(payload["uses_potion"]))
            self.assertTrue(bool(payload["uses_precise_reference"]))
            self.assertEqual(payload["sampler"], "k_euler_ancestral")

            meta_json = rt.request("GET", f"/images/{image_id}/metadata_json", user="uploader")
            expect_status(meta_json, 200)
            meta_payload = meta_json.json()
            self.assertTrue(bool(meta_payload["generation_usage"]["uses_potion"]))
            self.assertTrue(bool(meta_payload["generation_usage"]["uses_precise_reference"]))
            self.assertEqual(meta_payload["generation_usage"]["sampler"], "k_euler_ancestral")


TEST_DESCRIPTIONS = {
    "test_ui_regression_contracts": "UI文言・確認ダイアログ・タブ名の静的回帰確認",
    "test_auth_and_account_lifecycle": "認証、権限、アカウント作成/削除、自己削除の統合確認",
    "test_visibility_matrix_gallery_sidebar_and_filters": "一覧、creatorフィルタ、共有ブックマーク経由、sidebar、候補一覧の可視性確認",
    "test_visibility_for_detail_thumb_overlay_and_downloads": "detail、thumb(grid/overlay)、view/file/metadata の可視性確認",
    "test_bookmark_crud_bulk_operations_and_gallery_queries": "ブックマークCRUD、favorite、bulk apply、一覧問い合わせの確認",
    "test_upload_routes_and_admin_status_smoke": "upload、upload_batch、管理ステータス反映の確認",
    "test_bulk_delete_permissions_and_account_delete_cascade": "bulk delete権限、自己削除、管理削除、削除連鎖の確認",
    "test_metadata_extract_usage_flags_from_nested_scopes": "入れ子 params の参照利用フラグと sampler 抽出確認",
    "test_reparse_updates_usage_flags_from_nested_scopes": "再解析で入れ子 params の参照利用フラグと sampler を更新できるか確認",
    "test_usage_detection_falls_back_to_params_json": "params_json から参照利用フラグと sampler を拾えるか確認",
    "test_detail_usage_fields_fall_back_to_params_json_when_db_is_stale": "DB列が古くても params_json を使って詳細表示の参照利用フラグと sampler を返せるか確認",
}


class TeeStream:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for s in self.streams:
            s.write(data)
            s.flush()

    def flush(self):
        for s in self.streams:
            s.flush()


class TrackingTextResult(unittest.TextTestResult):
    def __init__(self, stream, descriptions, verbosity):
        super().__init__(stream, descriptions, verbosity)
        self.successes = []

    def addSuccess(self, test):
        self.successes.append(test)
        super().addSuccess(test)


def _case_name(test):
    return f"{test.__class__.__name__}.{test._testMethodName}"


def _test_record(test, status, detail=None):
    method = test._testMethodName
    return {
        "case": _case_name(test),
        "method": method,
        "description": TEST_DESCRIPTIONS.get(method, ""),
        "status": status,
        "detail": detail or "",
    }


def build_report(result: TrackingTextResult, started_at: str, duration_sec: float) -> dict:
    records = []
    for test in result.successes:
        records.append(_test_record(test, "passed"))
    for test, tb in result.failures:
        records.append(_test_record(test, "failed", tb))
    for test, tb in result.errors:
        records.append(_test_record(test, "error", tb))
    for test, reason in getattr(result, 'skipped', []):
        records.append(_test_record(test, "skipped", reason))

    records.sort(key=lambda x: x["case"])
    summary = {
        "started_at": started_at,
        "duration_seconds": round(duration_sec, 3),
        "total": result.testsRun,
        "passed": len(result.successes),
        "failed": len(result.failures),
        "errors": len(result.errors),
        "skipped": len(getattr(result, 'skipped', [])),
        "successful": result.wasSuccessful(),
    }
    return {"summary": summary, "tests": records}


def write_markdown_report(report: dict, path: Path) -> None:
    s = report["summary"]
    lines = [
        "# NIM Comprehensive Test Report",
        "",
        f"- started_at: {s['started_at']}",
        f"- duration_seconds: {s['duration_seconds']}",
        f"- total: {s['total']}",
        f"- passed: {s['passed']}",
        f"- failed: {s['failed']}",
        f"- errors: {s['errors']}",
        f"- skipped: {s['skipped']}",
        f"- successful: {str(s['successful']).lower()}",
        "",
        "## Per test",
        "",
    ]
    for test in report["tests"]:
        lines.append(f"### {test['case']}")
        lines.append(f"- status: {test['status']}")
        if test['description']:
            lines.append(f"- description: {test['description']}")
        if test['detail']:
            lines.append("")
            lines.append("```text")
            lines.append(test['detail'].rstrip())
            lines.append("```")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def run_suite() -> int:
    output_dir = PROJECT_ROOT / "tests" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    console_log = output_dir / "console.log"
    json_report = output_dir / "test_report.json"
    md_report = output_dir / "test_report.md"

    started = dt.datetime.now().astimezone()
    started_at = started.isoformat(timespec="seconds")

    with console_log.open("w", encoding="utf-8") as log_fp:
        stream = TeeStream(sys.stdout, log_fp)
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(ComprehensiveIntegrationTests)
        runner = unittest.TextTestRunner(
            stream=stream,
            verbosity=2,
            resultclass=TrackingTextResult,
        )
        started_perf = time.perf_counter()
        result = runner.run(suite)
        duration_sec = time.perf_counter() - started_perf

        report = build_report(result, started_at, duration_sec)
        json_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        write_markdown_report(report, md_report)

        print("")
        print(f"[NIM TEST] console log   : {console_log}")
        print(f"[NIM TEST] json report   : {json_report}")
        print(f"[NIM TEST] markdown report: {md_report}")

    return 0 if report["summary"]["successful"] else 1



def test_alias_canonicalization_disabled():
    from server.app.services.tag_parser import normalize_tag

    assert normalize_tag("dynamic pose") == "dynamic_pose"


def test_prompt_join_plain_prefers_text_only():
    prompt_js = (Path(__file__).resolve().parents[1] / "server" / "web" / "lib" / "prompt.js").read_text(encoding="utf-8")
    assert 't?.text || ""' in prompt_js
    assert 't?.canonical || t?.text' not in prompt_js


if __name__ == "__main__":
    raise SystemExit(run_suite())
