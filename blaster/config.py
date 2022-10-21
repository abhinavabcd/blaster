
from os import environ
import sys, os
import json
import logging
from .utils import events
import inspect


# default env variables
IS_PROD = environ.get("IS_PROD") == "1"
IS_STAGING = IS_PROD and environ.get("IS_STAGING") == "1"
IS_DEV = 1 if not (IS_PROD or IS_STAGING) else 0

# CRITICAL-50 ERROR-40  WARNING-30  INFO-20  DEBUG-10  NOTSET-0
LOG_LEVEL = logging.DEBUG if IS_DEV else (logging.INFO if IS_STAGING else logging.WARN)
DEBUG_LEVEL = int(environ.get("DEBUG_LEVEL") or 1)  # higher implies more debug information


_this_ = sys.modules[__name__]

class Config:
    _config = None
    event_prefix = None
    frozen_keys = None
    def __init__(self, event_prefix=None):
        self.frozen_keys = {k:v for k,v in vars(_this_).items() if not k.startswith("_")}
        self._config = dict(self.frozen_keys)
        self.event_prefix = event_prefix

    def load(self, *paths):
        import yaml
        for path in paths or ["./"]:
            path = os.path.join(
                os.path.dirname(inspect.stack()[1][1]), # caller file, called once usually, so no performance impact on app
                path
            )
            config_files = []
            if(os.path.isfile(path)):
                config_files = [path]
            else:
                config_files.append(os.path.join(path, "app.yaml"))
                if(config.IS_DEV):
                    config_files.append(os.path.join(path, "dev.yaml"))
                elif(config.IS_PROD):
                    config_files.append(os.path.join(path, "prod.yaml"))
                    config_files.append(os.path.join(path, "prod.secrets.yaml"))
                    if(config.IS_STAGING):
                        config_files.append(os.path.join(path, "staging.yaml"))
                        config_files.append(os.path.join(path, "staging.secrets.yaml"))
            for f in config_files:
                if(not os.path.isfile(f)):
                    continue
                print("Loading config file: ", f)
                for k, v in yaml.safe_load(open(f).read()).items():
                    setattr(self, k, v)

    def __setattr__(self, key, val):
        if(self._config == None):
            super().__setattr__(key, val)
            return
        if(self.frozen_keys and key in self.frozen_keys and key in self._config):
            # don't set frozen keys
            return
        self._config[key] = val
        # broadcast
        self.event_prefix and val != None and events.broadcast_event(self.event_prefix + key, val)

    def __getattr__(self, key):
        if(key not in self._config):
            if(key.startswith("__")):
                return getattr(_this_, key, None)
            elif(not self._config.get("BLASTER_FORK_ID")): # None or 0
                caller_frame = inspect.stack()[1]
                print("MISSING CONFIG Key#: {} {}:{}".format(key, caller_frame[1], caller_frame[2]))
            return None
        return self._config[key]

# hack: customized config module,
# that doesn't crash for missing config variables, 
# they will be just None.
config = Config(event_prefix="CONFIG_")

# more variables from env
if(gcloud_credential_file := environ.get("GOOGLE_APPLICATION_CREDENTIALS")):
    try:
        config.GCLOUD_CREDENTIALS = json.loads(open(gcloud_credential_file).read())
    except Exception as ex: 
        print(ex)