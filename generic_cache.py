"""
A simple redis-cache interface for storing Python objects.
Created on May 11, 2016

@author: Venkata Mulam
"""
from functools import wraps
import json
import hashlib
import redis
from redis.exceptions import ConnectionError
import logging
import time
import time as timer
try:
    import cPickle as pickle
except:
    import pickle
from settings import get_settings

DEFAULT_EXPIRY = 60 * 60 * 24
env_settings = get_settings()
REDIS_HOST = env_settings.REDIS_HOST
REDIS_PORT = env_settings.REDIS_PORT
REDIS_PASSWORD = env_settings.REDIS_PASSWORD
REDIS_DB = env_settings.REDIS_DB


# create the cache key for storage
def cache_create_key(namespace, ignore_args, func_name, *args, **kwargs):
    import hashlib
    serialise = []
    if ignore_args:
        serialise.append(str(args[0]))
    else:
        for arg in args:
            serialise.append(str(arg))
        for key, arg in kwargs.items():
            serialise.append(str(key))
            serialise.append(str(arg))
    key = hashlib.md5("".join(serialise)).hexdigest()
    if namespace:
        key = ':'.join([namespace, func_name, key])
    return key


class MyCache(redis.client.Redis):
    """
    Generic redis Cache class which uses the internal redis-py cliet 
    Provides genaric implementation of redis cache using Python client
    """
    def __init__(self, *args, **kwargs):
        # Save them for re connection purposes
        self.args = args
        self.kwargs = kwargs

        # conn_retries is the number of times that reconnect will try to connect
        if 'conn_retries' in kwargs:
            self.conn_retries = kwargs.pop('conn_retries')
        else:
            self.conn_retries = 1

        # max_sleep is the amount of time between reconnection attempts by safe_reconnect
        if 'max_sleep' in kwargs:
            self.max_sleep = kwargs.pop('max_sleep')
        else:
            self.max_sleep = 30

        if 'log' in kwargs:
            self._log = kwargs.pop('log')
        else:
            self._log = logging.getLogger(env_settings.LOG_NAME)

        if 'prefix' in kwargs:
            self.prefix = kwargs.pop('prefix')
        else:
            self.prefix = None

        if 'hashkeys' in kwargs:
            self.prefix = kwargs.pop('hashkeys')
        else:
            self.hashkeys = True

        self.host = kwargs.get('host', REDIS_HOST)
        self.port = kwargs.get('port', REDIS_PORT)
        self.db = kwargs.get('db', REDIS_DB)
        self.password = kwargs.get('password', REDIS_PASSWORD)

        self.connection = self.connect()

    def ping(self):
        """
        Utility function to check whether redis connection is alive or not
        :return boolean value True if redis connection is alive else False
        """
        try:
            self.connection.ping()
            self._log.info("Able to ping Redis Server")
            return True
        except ConnectionError as e:
            self._log.info("Unable to ping Redis Server: %s" % e)
            return False

    def connect(self, *args, **kwargs):
        """
        We cannot assume that connection will succeed, as such we use a ping()
        method in the redis client library to validate ability to contact redis.
        :return: redis.StrictRedis Connection Object
        """
        try:
            connection = redis.StrictRedis(host=self.host, port=self.port, db=self.db, password=self.password)
            connection.ping()
            self._log.info("Successfully connected to redis with: %s, %s" % (self.host, self.port))
            self.connection = connection
            return connection
        except redis.ConnectionError as e:
            self._log.error("Failed to create connection to redis with: %s, %s" % (self.host, self.port))
            self._log.error("Please check the redis connection. ERROR: %s" % e)

    def reconnect(self, conn_retries=None):
        """ Connects to Redis with a exponential waiting (3**n) """
        if conn_retries is None:
            conn_retries = self.conn_retries

        count = 0
        if self._log:
            self._log.info('Connecting to Redis.')
        while count < conn_retries:
            # super(redis.client.Redis, self).__init__(*self.args, **self.kwargs)
            connection = redis.StrictRedis(host=self.host, port=self.port, db=self.db, password=self.password)

            if connection.ping():
                self._log.info('Connected to Redis!')
                return True
            else:
                sl = min(3 ** count, self.max_sleep)
                self._log.info('Connecting failed, retrying in {0} seconds'.format(sl))
                time.sleep(sl)
                count += 1
        return False

    def safe_reconnect(self):
        """ Connects to Redis with a exponential waiting (3**n), wont return until successfully connected"""
        count = 0
        if self._log:
            self._log.info('Connecting to Redis.')
        while True:
            # super(redis.client.Redis, self).__init__(*self.args, **self.kwargs)
            connection = redis.StrictRedis(host=self.host, port=self.port, db=self.db, password=self.password)

            if connection.ping():
                self._log.info('Connected to Redis!')
                return True
            else:
                sl = min(3 ** count, self.max_sleep)
                self._log.info('Connecting failed, retrying in {0} seconds'.format(sl))
                time.sleep(sl)
                count += 1

    def make_key(self, key):
        if self.prefix:
            return "{0}:{1}".format(self.prefix, key)

        return key

    def namespace_key(self, namespace):
        return namespace + ':*'

    def get(self, key):
        """
        Method returns value of a given key from the cache else None.
        :param key: key to look up in Redis
        :return: Value of the given key if it exists in cache else None
        """
        key = to_unicode(key)
        if key:  # No need to validate membership, which is an O(1) operation, but seems we can do without.
            start = timer.time()
            try:
                value = self.connection.get(self.make_key(key))
                if not value:  # expired key
                    self._log.info('Key - %s Not found' % key)
                    return

                self._log.info("Cache took {0} to retrieve data from the Redis".format(float(timer.time() - start)))
                return pickle.loads(value)
            except (ConnectionError, AttributeError) as e:
                msg = "Error while getting key - %s" % key
                self._log.error('{} \nERROR: {}'.format(msg, str(e)))

    def mget(self, keys):
        """
        Method returns a dict of key/values for found keys.
        :param keys: List of keys to look up in Redis
        :return: dict of found key/values
        """
        if keys:
            cache_keys = [self.make_key(to_unicode(key)) for key in keys]
            try:
                values = self.connection.mget(cache_keys)
                return {k: pickle.loads(v) for (k, v) in zip(keys, values)}
            except (ConnectionError, AttributeError) as e:
                self._log.error('Error while getting multiple keys. \nERROR: {}'.format(str(e)))

    def keys(self):
        """
        :return: Returns all keys in the cache as a list
        """
        start = timer.time()
        try:
            keys = self.connection.keys()
            self._log.info("Cache took {0} to retrieve data from the Redis Server".format(float(timer.time() - start)))
            return keys
        except (ConnectionError, AttributeError) as e:
            self._log.error('Error while getting all keys in the cache. \nERROR: {}'.format(str(e)))

    def get_keys_json(self, keys):
        """
        Method returns a dict of key/values for found keys with each value
        parsed from JSON format.
        :param keys: List of keys to look up in Redis
        :return: dict of found key/values with values parsed from JSON format
        """
        start = timer.time()
        d = self.mget(keys)
        if d:
            self._log.info("Cache took {0} to retrieve data from the Redis Server".format(float(timer.time() - start)))
            for key, value in d.items():
                d[key] = json.loads(value if value else None)
            return d

    def set(self, key, value, expire=DEFAULT_EXPIRY):
        """
        Method stores a value in the cache with expiration
        :param key: key by which to reference datum being stored in Redis
        :param value: actual value being stored under this key
        :param expire: time-to-live (ttl) for this datum
        """
        key = to_unicode(key)
        value = pickle.dumps(value)

        if expire is None:
            expire = self.expire
        try:
            self.connection.set(self.make_key(key), value, expire)
            self._log.info("Successfully set %s: %s" % (key, value))
        except (ConnectionError, AttributeError) as e:
            self._log.error('Error While setting key, values \nERROR: {}'.format(str(e)))

    def delete(self, key):
        """
        Method removes (invalidates) an item from the cache.
        :param key: key to remove from Redis
        """
        key = to_unicode(key)
        try:
            self.connection.delete(self.make_key(key))
            self._log.info("Successfully deleted key: %s" % key)
        except (ConnectionError, AttributeError) as e:
            self._log.error('Error while deleting key -{} \nERROR: {}'.format(key, str(e)))

    def delete_all(self):
        """
        Method removes (invalidates) all items from the cache.
        """
        try:
            keys = self.connection.keys()
            if keys:
                self.connection.delete(*keys)
                self._log.info("Successfully deleted keys: %s" % keys)
                return

            self._log.info("No keys found in cache")
        except (ConnectionError, AttributeError) as e:
            self._log.error('Error while deleting/flushing all keys. \nERROR: {}'.format(str(e)))

    def delete_namespace(self, space):
        """
        Method removes (invalidates) all items in a given namespace from the cache.
        :param key: key to remove from Redis
        """
        namespace = self.namespace_key(space)
        try:
            keys = list(self.connection.keys(namespace))
            if keys:
                self.connection.delete(keys)
                self._log.info("Successfully deleted namespace: %s" % namespace)
                return

            self._log.info("No keys found in cache with namespace: %s" % namespace)
        except (ConnectionError, AttributeError) as e:
            self._log.error('Error while deleting namespace. \nERROR: {}'.format(str(e)))

    def store_json(self, key, value, expire=None):
        try:
            self.connection.set(key, json.dumps(value), expire)
        except (ConnectionError, AttributeError) as e:
            self._log.error('Error while storing as  JSON. \nERROR: {}'.format(str(e)))

    def store_pickle(self, key, value, expire=None):
        try:
            self.connection.set(key, pickle.dumps(value), expire)
        except (ConnectionError, AttributeError) as e:
            self._log.error('Error while doing pickling. \nERROR: {}'.format(str(e)))

    def get_json(self, key):
        try:
            return json.loads(self.connection.get(key))
        except (ConnectionError, AttributeError) as e:
            self._log.error('Error while converting to JSON. \nERROR: {}'.format(str(e)))

    def get_pickle(self, key):
        try:
            return pickle.loads(self.connection.get(key))
        except (ConnectionError, AttributeError) as e:
            self._log.error('Error while doing un-pickling. \nERROR: {}'.format(str(e)))

    def __contains__(self, key):
        return key in self.connection.keys()

    def get_hash(self, args):
        if self.hashkeys:
            key = hashlib.md5(args).hexdigest()
        else:
            key = pickle.dumps(args)
        return key


def cache_it(namespace=None, expire=DEFAULT_EXPIRY, cache=None, ignore_args=False, use_json=False, view=False):
    """
    Arguments and function result must be pickleable.
    :param expire: period after which an entry in cache is considered expired
    :param cache: SimpleCache object, if created separately
    :return: decorated function
    """
    cache_ = cache    # Since python 2.x doesn't have the nonlocal keyword, we need to do this
    expire_ = expire  # Same here.

    def decorator(function):
        cache, expire = cache_, expire_
        if cache is None:
            cache = MIOCache(namespace=namespace)

        @wraps(function)
        def func(*args, **kwargs):
            # Handle cases where caching is down or otherwise not available.
            if cache.connection is None or cache.ping() is False:
                result = function(*args, **kwargs)
                return result

            # serializer = json if use_json else pickle
            fetcher = cache.get_json if use_json else cache.get_pickle
            # This way, you need to make sure all args must be json or pcikle serializable.
            storer = cache.store_json if use_json else cache.store_pickle

            cache_key = cache_create_key(namespace, ignore_args, func.__name__, *args, **kwargs)

            try:
                start = timer.time()
                result = fetcher(cache_key)
                msg = "Cache took {0} to retireve data from the Redis Server".format(float(timer.time() - start))
                cache._log.info(msg)
                return result

            except Exception as e:
                # Add some sort of cache miss handing here.
                pass
            except:
                cache._log.info("Unknown redis-cache error. Please check your Redis free space.")

            try:
                result = function(*args, **kwargs)
            except Exception as e:
                result = e.result
            else:
                try:
                    start = timer.time()
                    storer(cache_key, result, expire)
                    msg = "Cache took {0} to retireve data from the Redis Server".format(float(timer.time() - start))
                    cache._log.info(msg)
                except redis.ConnectionError as e:
                    logging.exception(e)

            return result
        return func
    return decorator


def cache_it_json(expire=DEFAULT_EXPIRY, cache=None, namespace=None, view=True):
    """
    Arguments and function result must be able to convert to JSON.
    :param limit: maximum number of keys to maintain in the set
    :param expire: period after which an entry in cache is considered expired
    :param cache: SimpleCache object, if created separately
    :return: decorated function
    """
    return cache_it(expire=expire, use_json=True, cache=cache, namespace=None)


def to_unicode(obj, encoding='utf-8'):
    if isinstance(obj, basestring):
        if not isinstance(obj, unicode):
            obj = unicode(obj, encoding)
    return obj

