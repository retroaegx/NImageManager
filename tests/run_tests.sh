#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p ./tests/output
./.venv/bin/python -m pip install --upgrade pip > ./tests/output/bootstrap.log 2>&1 || true
if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
./.venv/bin/python -m pip install --upgrade pip > ./tests/output/bootstrap.log 2>&1
./.venv/bin/python -m pip install -r ./tests/requirements-test.txt >> ./tests/output/bootstrap.log 2>&1
./.venv/bin/python ./tests/run_comprehensive_tests.py "$@"
rc=$?
echo
echo "Bootstrap log : ./tests/output/bootstrap.log"
echo "Console log   : ./tests/output/console.log"
echo "JSON report   : ./tests/output/test_report.json"
echo "MD report     : ./tests/output/test_report.md"
echo "Exit code     : $rc"
exit $rc
