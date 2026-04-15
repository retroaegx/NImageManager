# Comprehensive Test Program

Extract this package into the root of the `NImageManager` project and run it there.

## Files to check after execution

Results are always written to:

- `tests/output/bootstrap.log`
- `tests/output/console.log`
- `tests/output/test_report.json`
- `tests/output/test_report.md`

Even when you start the batch file by double-clicking it, the output location is still the four files above.

## Pass / fail

- Pass when `tests/output/test_report.json` contains `summary.successful = true`
- Fail when it is `false`
- Failure details are written to `tests/output/test_report.md` and `tests/output/console.log`

## Windows

`tests\run_tests.bat`

or

```powershell
powershell -ExecutionPolicy Bypass -File .\tests\run_tests.ps1
```

## macOS / Linux

```bash
./tests/run_tests.sh
```

## Run directly

If you want to use an existing `.venv` manually:

```bash
python -m pip install -r tests/requirements-test.txt
python tests/run_comprehensive_tests.py
```

## Notes

- In addition to `requirements.txt`, the test environment uses `httpx`.
- The test suite starts the app with FastAPI `TestClient` and validates APIs, visibility rules, delete cascades, and the web UI contract.
- Run it from the project root.
