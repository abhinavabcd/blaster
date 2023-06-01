# default env variables
from os import environ
import logging

IS_PROD = environ.get("IS_PROD") == "1"
IS_STAGING = IS_PROD and environ.get("IS_STAGING") == "1"
IS_DEV = 1 if not (IS_PROD or IS_STAGING) else 0
IS_TEST = IS_DEV and environ.get("IS_TEST") == "1"

# CRITICAL-50 ERROR-40  WARNING-30  INFO-20  DEBUG-10  NOTSET-0
APP_NAME = environ.get("APP_NAME")
APP_VERSION = environ.get("APP_VERSION")
# Logging

LOG_LEVEL = logging.DEBUG if IS_DEV else (logging.INFO if IS_STAGING else logging.WARN)
DEBUG_PRINT_LEVEL = IS_DEV and int(environ.get("DEBUG_PRINT_LEVEL") or 0)

LOG_APP_NAME = environ.get("LOG_APP_NAME") or APP_NAME or ""
LOG_APP_VERSION = environ.get("LOG_APP_VERSION") or APP_VERSION or ""
CONSOLE_LOG_RAW_JSON = bool(environ.get("CONSOLE_LOG_RAW_JSON"))
