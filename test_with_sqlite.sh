#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
if [ "$#" -eq 0 ]; then
    # Separate processes: importing blaster patches gevent process-wide.
    IS_TEST=1 /home/abhinav/any/python3_14_venv/bin/python3 -m unittest tests.sqlite_test tests.test_sqlite_node tests.test_sqlite_reload
    IS_TEST=1 /home/abhinav/any/python3_14_venv/bin/python3 -m unittest tests.sqlite_modes_test
else
    IS_TEST=1 /home/abhinav/any/python3_14_venv/bin/python3 -m unittest "$@"
fi
