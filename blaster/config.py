from os import environ
import sys, os
import json
import logging
import inspect
## NOTE: don't import anything from blaster library,
## they may depend on config and won't load correctly.
## keep it isolated as much as possible

# default env variables
IS_PROD = environ.get("IS_PROD") == "1"
IS_STAGING = IS_PROD and environ.get("IS_STAGING") == "1"
IS_DEV = 1 if not (IS_PROD or IS_STAGING) else 0

# CRITICAL-50 ERROR-40  WARNING-30  INFO-20  DEBUG-10  NOTSET-0
LOG_LEVEL = logging.DEBUG if IS_DEV else (logging.INFO if IS_STAGING else logging.WARN)


_this_ = sys.modules[__name__]

class Config:
    _config = None
    frozen_keys = None
    def __init__(self):
        self.frozen_keys = {k:v for k,v in vars(_this_).items() if not k.startswith("_")}
        self._config = dict(self.frozen_keys)
    
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

    def __getattr__(self, key):
        if(key not in self._config):
            if(key.startswith("__")):
                return getattr(_this_, key, None)
            elif(not self._config.get("BLASTER_FORK_ID")): # None or 0
                caller_frame = inspect.stack()[1]
                if(not caller_frame[1].startswith("<frozen")): # <frozen importlib._bootstrap>
                    print("MISSING CONFIG Key#: {} {}:{}".format(key, caller_frame[1], caller_frame[2]))
            return None
        return self._config[key]

# hack: customized config module,
# that doesn't crash for missing config variables, 
# they will be just None.
config = Config()

# more variables from env
if(gcloud_credential_file:= environ.get("GOOGLE_APPLICATION_CREDENTIALS")):
    try:
        config.GCLOUD_CREDENTIALS = json.loads(open(gcloud_credential_file).read())
    except Exception as ex: 
        print(ex)

# BLASTER SPECIFIC CONFIGS, that can be overridden
config.BLASTER_HTTP_TIMEOUT_WARN_THRESHOLD = 5000

# MONGO ORM SPECIFIC CONFIGS
config.MONGO_WARN_THRESHOLD_MANY_RESULTS_FETCHED = 2000
config.MONGO_WARN_MAX_QUERY_TIME_SECONDS = 3