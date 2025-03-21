from os import environ
import sys
import os
import json
import inspect
# NOTE: don't import anything from blaster library,
# they may depend on config and won't load correctly.
# keep it isolated as much as possible
from . import env

_this_ = sys.modules[__name__]


class Config:
    _config = None
    frozen_keys = None

    def __init__(self):
        self.frozen_keys = {k: v for k, v in vars(env).items() if not k.startswith("_")}
        _config = {}
        for k, v in os.environ.items():
            if(v.startswith("{") or v.startswith("[")):
                v = json.loads(v)
            _config[k] = v
        _config.update(self.frozen_keys)  # these cannot be overridden
        self._config = _config

    def load(self, *paths):
        import yaml
        for path in paths or ["./"]:
            path = os.path.join(
                os.path.dirname(inspect.stack()[1][1]),  # caller file, called once usually, so no performance impact on app
                path
            ) if not path.startswith("/") else path
            config_files = []
            if(os.path.isfile(path)):
                config_files = [path]
            else:
                config_files.append(os.path.join(path, "app.yaml"))
                if(self.IS_DEV):  # DEV/LOCAL ENVIRONMENT
                    config_files.append(os.path.join(path, "dev.yaml"))
                    if(self.IS_TEST):
                        config_files.append(os.path.join(path, "test.yaml"))
                        if(self.IS_TEST_LOCAL):
                            config_files.append(os.path.join(path, "test_local.yaml"))
                elif(self.IS_PROD):  # PROD ENVIRONMENT
                    config_files.append(os.path.join(path, "prod.yaml"))
                    config_files.append(os.path.join(path, "prod.secrets.yaml"))
                    if(self.IS_STAGING):
                        config_files.append(os.path.join(path, "staging.yaml"))
                        config_files.append(os.path.join(path, "staging.secrets.yaml"))
                        if(self.IS_STAGING_DEV):
                            config_files.append(os.path.join(path, "staging.dev.yaml"))
                            config_files.append(os.path.join(path, "staging.dev.secrets.yaml"))
            for f in config_files:
                if(
                    not os.path.isfile(f)
                    or (_config := yaml.safe_load(open(f).read())) is None  # empty file
                ):
                    continue
                print("Loading config file: ", f)
                for k, v in _config.items():
                    setattr(self, k, v)

    def __setattr__(self, key, val):
        if(self._config is None):
            super().__setattr__(key, val)
            return
        if(self.frozen_keys and self.frozen_keys.get(key) is not None):
            # don't set frozen keys if already set
            return
        self._config[key] = val

    def __getattr__(self, key):
        if(key not in self._config):
            if(key.startswith("__")):
                return getattr(_this_, key, None)  # for internal attributes
            elif(not self._config.get("BLASTER_FORK_ID")):  # PRINT THESE ONLY ONCE
                caller_frame = inspect.stack()[1]
                if(not caller_frame[1].startswith("<frozen")):  # <frozen importlib._bootstrap>
                    print("MISSING CONFIG Key#: {} {}:{}".format(key, caller_frame[1], caller_frame[2]))
            return None
        return self._config[key]


# hack: customized config module,
# that doesn't crash for missing config variables,
# they will be just None.
config = Config()

# more variables from env
if(gcloud_credential_file := environ.get("GOOGLE_APPLICATION_CREDENTIALS")):
    try:
        config.GCLOUD_CREDENTIALS = json.loads(open(gcloud_credential_file).read())
    except Exception as ex:
        print(ex)

# BLASTER SPECIFIC CONFIGS, that can be overridden
config.BLASTER_HTTP_TOOK_LONG_WARN_THRESHOLD = 5000

# MONGO ORM SPECIFIC CONFIGS
config.MONGO_WARN_MAX_RESULTS_RATE = 1000  # can scan at a max of 1000 / sec
config.MONGO_MAX_RESULTS_AT_HIGH_SCAN_RATE = 30000  # cannot scan more than this at high scan rate
# Logging basics
