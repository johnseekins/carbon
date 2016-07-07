"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import os
from os.path import exists, dirname, join, sep
from carbon.util import PluginRegistrar
from carbon import log


class TimeSeriesDatabase(object):
  "Abstract base class for Carbon database backends."
  __metaclass__ = PluginRegistrar
  plugins = {}

  def write(self, metric, datapoints):
    "Persist datapoints in the database for metric."
    raise NotImplemented()

  def exists(self, metric):
    "Return True if the given metric path exists, False otherwise."
    raise NotImplemented()

  def create(self, metric, retentions, xfilesfactor, aggregation_method):
    "Create an entry in the database for metric using options."
    raise NotImplemented()

  def getMetadata(self, metric, key):
    "Lookup metric metadata."
    raise NotImplemented()

  def setMetadata(self, metric, key, value):
    "Modify metric metadata."
    raise NotImplemented()

  def getFilesystemPath(self, metric):
    "Return filesystem path for metric, defaults to None."
    pass


try:
  import whisper
except ImportError:
  pass
else:
  class WhisperDatabase(TimeSeriesDatabase):
    plugin_name = 'whisper'

    def __init__(self, settings):
      self.data_dir = settings.LOCAL_DATA_DIR
      self.sparse_create = settings.WHISPER_SPARSE_CREATE
      self.fallocate_create = settings.WHISPER_FALLOCATE_CREATE
      if settings.WHISPER_AUTOFLUSH:
        log.msg("Enabling Whisper autoflush")
        whisper.AUTOFLUSH = True

      if settings.WHISPER_FALLOCATE_CREATE:
        if whisper.CAN_FALLOCATE:
          log.msg("Enabling Whisper fallocate support")
        else:
          log.err("WHISPER_FALLOCATE_CREATE is enabled but linking failed.")

      if settings.WHISPER_LOCK_WRITES:
        if whisper.CAN_LOCK:
          log.msg("Enabling Whisper file locking")
          whisper.LOCK = True
        else:
          log.err("WHISPER_LOCK_WRITES is enabled but import of fcntl module failed.")

      if settings.WHISPER_FADVISE_RANDOM:
        try:
          if whisper.CAN_FADVISE:
            log.msg("Enabling Whisper fadvise_random support")
            whisper.FADVISE_RANDOM = True
          else:
            log.err("WHISPER_FADVISE_RANDOM is enabled but import of ftools module failed.")
        except AttributeError:
          log.err("WHISPER_FADVISE_RANDOM is enabled but skipped because it is not compatible with the version of Whisper.")

    def write(self, metric, datapoints):
      path = self.getFilesystemPath(metric)
      whisper.update_many(path, datapoints)

    def exists(self, metric):
      return exists(self.getFilesystemPath(metric))

    def create(self, metric, retentions, xfilesfactor, aggregation_method):
      path = self.getFilesystemPath(metric)
      directory = dirname(path)
      try:
        if not exists(directory):
          os.makedirs(directory)
      except OSError, e:
        log.err("%s" % e)

      whisper.create(path, retentions, xfilesfactor, aggregation_method,
                     self.sparse_create, self.fallocate_create)

    def getMetadata(self, metric, key):
      if key != 'aggregationMethod':
        raise ValueError("Unsupported metadata key \"%s\"" % key)

      wsp_path = self.getFilesystemPath(metric)
      return whisper.info(wsp_path)['aggregationMethod']

    def setMetadata(self, metric, key, value):
      if key != 'aggregationMethod':
        raise ValueError("Unsupported metadata key \"%s\"" % key)

      wsp_path = self.getFilesystemPath(metric)
      return whisper.setAggregationMethod(wsp_path, value)

    def getFilesystemPath(self, metric):
      metric_path = metric.replace('.', sep).lstrip(sep) + '.wsp'
      return join(self.data_dir, metric_path)


try:
  from carbon.hbase import HBaseDB, load_schemas, create_tables
  from random import randrange
except ImportError:
  pass
else:
  class HBaseDatabase(TimeSeriesDatabase):
    plugin_name = 'hbase'

    def __init__(self, settings):
      thrift_host = settings.get('HBASE_THRIFT_HOST', 'localhost')
      thrift_port = settings.get('HBASE_THRIFT_PORT', 9090)
      transport_type = settings.get('HBASE_TRANSPORT_TYPE', 'buffered')
      batch_size = settings.get('HBASE_BATCH_SIZE', 100)
      """
      We add a few random minutes so thrift servers don't get slammed
      with re-connection attempts
      """
      reset_interval = settings.get('HBASE_RESET_INTERVAL', 3600) + randrange(120)
      connection_retries = settings.get('HBASE_CONNECTION_RETRIES', 3)
      protocol = settings.get('HBASE_PROTOCOL', 'binary')
      compat_level = str(settings.get('HBASE_COMPAT_LEVEL', 0.94))
      memcache_hosts = settings.get('MEMCACHE_SERVERS', [])
      if memcache_hosts and not isinstance(memcache_hosts, list):
        memcache_hosts = [m.strip() for m in memcache_hosts.split(',') if m]

      schema_path = join(settings["CONF_DIR"], "storage-schemas.conf")
      agg_path = join(settings["CONF_DIR"], "storage-aggregation.conf")
      compression = settings.get("HBASE_STORAGE_COMPRESSION", None)
      metric_schema, self.storage_schemas = load_schemas(schema_path)

      create_tables(metric_schema, compression, thrift_host, thrift_port,
                    transport_type, protocol, compat_level)
      """
        We'll send batches aggressively
      We batch to reduce writes, but we still want "real time" data.
      (Relative, obvs...)
      This means we have to write fairly frequently (send_freq) so
      more fine-grained data (<1 minute, etc) still shows rapidly.
      The trade-off is that we're doing more smaller writes this way.
      """
      send_freq = 60
      for s in self.storage_schemas:
        cur_min = min([t.getTuple()[0] for t in s.archives])
        if cur_min < send_freq:
          send_freq = cur_min

      settingsdict = {'host': thrift_host, 'port': thrift_port,
                      'ttype': transport_type, 'batch': batch_size,
                      'reset_int': reset_interval, 'retries': connection_retries,
                      'protocol': protocol, 'compat': compat_level,
                      'send_freq': send_freq, 'm_schema': metric_schema,
                      'memcache': memcache_hosts}
      self.h_db = HBaseDB(settingsdict)

    def create(self, metric, retentions, xfilesfactor, aggregation_method):
      self.h_db.create(metric, retentions, aggregation_method)

    def getMetaData(self, metric, key):
      if key != 'aggregationMethod':
        raise ValueError("Unsupported metadata key \"%s\"" % key)

      return self.h_db.get_metric(metric)

    def write(self, metric, points):
      reten_config = []
      for schema in self.storage_schemas:
        if schema.matches(metric):
          reten_config = [archive.getTuple() for archive in schema.archives]
          break
      if not reten_config:
        raise Exception("Couldn't find a retention config for %s" % metric)
      self.h_db.update_many(metric, points, reten_config)

    def exists(self, metric):
      return self.h_db.exists(metric)
