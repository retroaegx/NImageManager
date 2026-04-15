# Comprehensive test program

この zip は `NImageManager` プロジェクト直下に展開して使います。

## 実行後に確認する場所

実行結果は必ず次へ保存されます。

- `tests/output/bootstrap.log`
- `tests/output/console.log`
- `tests/output/test_report.json`
- `tests/output/test_report.md`

`bat` をダブルクリックしても、確認先は上の4つです。

## 判定

- `tests/output/test_report.json` の `summary.successful = true` なら合格
- `false` なら不合格
- 失敗箇所は `tests/output/test_report.md` と `tests/output/console.log` に出ます

## Windows

`tests\run_tests.bat`

または

```powershell
powershell -ExecutionPolicy Bypass -File .\tests\run_tests.ps1
```

## macOS / Linux

```bash
./tests/run_tests.sh
```

## 直接実行

既存の `.venv` を自分で使う場合:

```bash
python -m pip install -r tests/requirements-test.txt
python tests/run_comprehensive_tests.py
```

## 追加メモ

- 依存は `requirements.txt` に加えて `httpx` を使用します。
- テストは FastAPI の `TestClient` でアプリを起動して API / 可視性 / 削除連鎖 / 静的 UI 文言を通しで検証します。
- プロジェクト直下で実行してください。
