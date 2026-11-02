#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
if [ "$#" -eq 0 ]; then
    set -- tests.sqlite_test tests.test_sqlite_node tests.test_sqlite_reload
fi
IS_TEST=1 /home/abhinav/any/python3_14_venv/bin/python3 -m unittest "$@"
