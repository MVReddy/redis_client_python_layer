import ConfigParser
import os
import logging
from mass_common import mio_utils

curr_env = None
settings_dict = {}


class EnvironmentType:
    DEV = 'dev'
    QA = 'qa'
    STAGE = 'stg'
    PROD = 'prod'
    LOCAL = 'local'


class EnvSettings:

    def __init__(self):
        self.LOG_ROOT = None
        self.LOG_NAME = None
        self.LOG_LEVEL = None
        self.REDIS_HOST = None
        self.REDIS_PORT = None
        self.REDIS_PASSWORD = None
        self.REDIS_DB = None


def load_settings(env=None):
    global settings_dict, curr_env
    mass_redis_settings = EnvSettings()

    mass_redis_settings.cpg = ConfigParser.SafeConfigParser()
    if env is None or env == EnvironmentType.LOCAL:
        mass_redis_settings.cpg.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), r'config.txt'))
    else:
        mass_redis_settings.cpg.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), r'config_%s.txt' % env))

    mass_redis_settings.ENV = mass_redis_settings.cpg.get('general', 'env')

    # Set only once so that this maintains a reference to the environment the app is running in
    # Don't want to do this on subsequent loads since when user requests a provider in a different environment
    if curr_env is None:
        curr_env = mass_redis_settings.ENV

    mass_redis_settings.LOG_ROOT = mass_redis_settings.cpg.get('general', 'log_root')
    mass_redis_settings.LOG_NAME = 'generic_redis_cache.log'
    mass_redis_settings.LOG_LEVEL = mass_redis_settings.cpg.get('general', 'log_level')

    mio_utils.init_log(mass_redis_settings.LOG_NAME, mass_redis_settings.LOG_ROOT)
    level = logging.getLevelName(mass_redis_settings.LOG_LEVEL)
    mio_utils.set_log_level(level, mass_redis_settings.LOG_NAME)

    logger = logging.getLogger(mass_redis_settings.LOG_NAME)

    # Redis Settings
    mass_redis_settings.REDIS_HOST = mass_redis_settings.cpg.get('general', 'redis_server')
    mass_redis_settings.REDIS_PORT = mass_redis_settings.cpg.get('general', 'redis_port')
    mass_redis_settings.REDIS_PASSWORD = mass_redis_settings.cpg.get('general', 'redis_password')
    mass_redis_settings.REDIS_DB = mass_redis_settings.cpg.get('general', 'redis_db')

    # For debugging purposes, log settings data
    logger.info('log_root: %s, log_name:%s, log_level:%s' % (mass_redis_settings.LOG_ROOT, mass_redis_settings.LOG_NAME,
                                                             mass_redis_settings.LOG_LEVEL))

    settings_dict[mass_redis_settings.ENV] = mass_redis_settings


def get_settings(env=None):
    if env is None:
        env = curr_env

    if env not in settings_dict:
        load_settings(env)
    return settings_dict[env]

# LOAD SETTINGS
load_settings()
