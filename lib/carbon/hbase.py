from time import time, sleep
import os
from ConfigParser import ConfigParser
import re
import json
from carbon.exceptions import CarbonConfigException
from carbon import log
import happybase
import whisper
"""
We manage a namespace table (NS) and a group of data tables.

The NS table is organized to mimic a tree structure, with a ROOT node
containing links to its children.
Nodes are either a BRANCH node which contains multiple child columns
prefixed with c_, or a LEAF node containing a single NODE column
so it has data

ROOT
   - c_branch1 -> branch1
   - c_leaf1 -> leaf1

branch1
   - c_leaf2 -> branch1.leaf2

leaf1
    - NODE -> bool
    - AGG_METHOD -> str
    - AGG -> json

The data tables are 2-hour-segmented lists of values for each metric, e.g.
<metric>:<2 hour period> => (t:<normalized timestamp>:<actual timstamp> => <value>, etc.)

Each table represents one "segment" of the retention policy e.g.
With retention definition:
[carbon]
pattern = ^carbon\.
retentions = 60s:3d,5min:30d

We would have two tables:
1) graphite_60.60_4320.300_8640
2) graphite_300.60_4320.300_8640

Each table expires data automatically when it passes the retention boundary
of that table. So...
1) 259200 seconds
2) 2851200 seconds
"""
META_CF_NAME = 'm'
META_NODE = "%s:NODE" % META_CF_NAME
META_AGG = "%s:AGG" % META_CF_NAME
META_AGG_METHOD = "%s:AGG_METHOD" % META_CF_NAME
DATA_CF_NAME = 'd'
META_SUFFIX = "META"
TABLE_PREFIX = 'graphite'


class HBaseDB(object):
  __slots__ = ('thrift_host', 'thrift_port', 'transport_type', 'batch_size',
               'reset_interval', 'connection_retries', 'protocol',
               'compat_level', 'send_freq', 'schemas', 'data_tables', 'data_batches',
               'send_time', 'reset_time', 'reset_interval', 'client', 'meta_table')

  def __init__(self, settingsdict):
    self.thrift_host = settingsdict['host']
    self.thrift_port = settingsdict['port']
    self.transport_type = settingsdict['ttype']
    self.batch_size = settingsdict['batch']
    self.reset_interval = settingsdict['reset_int']
    self.connection_retries = settingsdict['retries']
    self.protocol = settingsdict['protocol']
    self.compat_level = settingsdict['compat']
    self.send_freq = settingsdict['send_freq']
    self.schemas = settingsdict['m_schema']

    # variables that get defined elsewhere
    self.data_tables = {}
    self.data_batches = {}
    self.send_time = 0
    self.reset_time = 0
    self.client = None
    self.meta_table = None

    # use the reset function only for consistent connection creation
    self.__reset_conn()

  def create(self, metric, retention_config, agg_method):
    """create the "tree" portion of the metric

    Keyword arguments:
    metric -- the name of the metric to process
    retention_config -- time ranges for this metrics "roll up"
    agg_method -- function used to "roll up" this metric
    """
    values = {META_NODE: 'True', META_AGG: json.dumps(retention_config),
              META_AGG_METHOD: agg_method}
    metric_parts = metric.split('.')
    metric_key = ""
    prior_key = "ROOT"
    metric_prefix = "%s:c_" % (META_CF_NAME)
    for part in metric_parts:
      # if parent is empty, special case for root
      if metric_key == "":
        prior_key = "ROOT"
        metric_key = part
      else:
        prior_key = metric_key
        metric_key = "%s.%s" % (metric_key, part)
      metric_name = "%s%s" % (metric_prefix, part)
      if metric_name == metric_prefix:
        continue
      try:
        self.meta_table.put(prior_key, {metric_name: metric_key})
      except Exception, e:
        raise Exception(str(e))
    # Write the actual value
    try:
      self.meta_table.put(metric, values)
    except Exception, e:
      raise Exception(str(e))

  def update_many(self, metric, points, reten_config):
    """
    Update many datapoints.

    Keyword arguments:
    metric -- the name of the metric to process
    points  -- Is a list of (timestamp,value) points
    reten_config -- the retention policy for this metric
    """
    # Base name for each table
    reten_str = ".".join("%s_%s" % tup for tup in reten_config)
    for point in points:
      timestamp = int(point[0])
      value = float(point[1])
      """
        What row will this go in
      We put two hours of data in one row to make sure our rows are nice and big
      Theoretically, we could have much larger rows than this...but two hours
      seems a reasonable compromise between dense metrics (1 second or so retention)
      and sparser metrics (15 minutes).
      We determine the row by "flooring" the timestamp of this particular point
      """
      row = int(timestamp / 7200) * 7200
      rowkey = "%s:%d" % (metric, row)
      # Write to every table at once
      for r in reten_config:
        step = int(r[0])
        tname = "%d.%s" % (step, reten_str)
        """
          Normalize the timestamp into a bucket
        We do this so more granular timestamps can be set in groups that match
        the retention of the current table they're in.
        This way, we get "coarser" retentions without any effort
        """
        norm_time = int(timestamp / step) * step
        colkey = "%s:%d:%d" % (DATA_CF_NAME, norm_time, timestamp)
        try:
          self.data_batches[tname].put(rowkey, {colkey: str(value)})
        except Exception, e:
          log.err("Couldn't write to %s because %s" % (tname, e))
    # Here we make sure data is getting flushed
    cur_time = time()
    if cur_time - self.reset_time > self.reset_interval:
      self.__refresh_conn()
    elif cur_time - self.send_time > self.send_freq:
      for t in self.data_batches.values():
        t.send()
      self.send_time = time()

  def exists(self, metric):
    """
    Does a metric exist?

    Keyword arguments:
    metric -- the name of the metric to process
    """
    try:
      res = self.get_row(metric, column=[META_NODE])
      metric_exists = bool(res[META_NODE])
    except Exception:
      metric_exists = False
    return metric_exists

  def get_row(self, row, column=None):
    """
    return the data from a row in the meta table

    Keyword arguments:
    row -- the row (metric name) to return data for
    column -- return only data from a particular column
    """
    if time() - self.reset_time > self.reset_interval:
      self.__refresh_conn()
    try:
      if column:
        res = self.meta_table.row(row, column)
      else:
        res = self.meta_table.row(row)
    except Exception:
      self.__refresh_conn()
      if column:
        res = self.meta_table.row(row, column)
      else:
        res = self.meta_table.row(row)
    return res

  def __make_conn(self):
    """
    We want to be able to explicitly attempt the open
    (so we can retry if it fails), so we'll create the
    connection object inside this wrapper script.
    """
    try:
      del self.client
    except Exception:
      pass
    self.client = happybase.Connection(host=self.thrift_host,
                                       port=self.thrift_port,
                                       table_prefix=TABLE_PREFIX,
                                       transport=self.transport_type,
                                       protocol=self.protocol,
                                       compat=self.compat_level,
                                       autoconnect=False)
    self.client.open()
    sleep(0.25)
    try:
      len(self.client.tables())
      return True, None
    except Exception, e:
      return False, e

  def __reset_conn(self):
    for conn in xrange(self.connection_retries):
      res, e = self.__make_conn()
      if res:
        break
    else:
      log.err('Cannot get connection to HBase because %s' % e)
      exit(2)
    self.meta_table = self.client.table(META_SUFFIX)
    self.data_tables = {}
    self.data_batches = {}
    for r in self.schemas:
      t, r_secs = r
      # Just in case a table wasn't created
      try:
        self.data_tables[t] = self.client.table(t)
        self.data_batches[t] = self.data_tables[t].batch(batch_size=self.batch_size)
      except Exception:
        log.err("Missing table %s" % t)
        pass
    t = time()
    self.reset_time = t
    self.send_time = t

  def __refresh_conn(self, wait_time=5):
    # flush data batches
    for t in self.data_batches.values():
      t.send()

    self.client.close()
    # try and refresh for wait_time seconds
    give_up_time = time() + wait_time
    while time() < give_up_time:
      try:
        log.msg('Retrying connection to %s' % self.thrift_host)
        sleep(1)
        self.client.open()
      except Exception:
        pass
      else:
        log.msg('Connection resumed...')
        cur_time = time()
        self.reset_time = cur_time
        self.send_time = cur_time
        break
    # While -> else Pretty cool, python. Pretty cool.
    else:
      self.__reset_conn()


class OrderedConfigParser(ConfigParser):
  """Hacky workaround to ensure sections are always returned in the order
   they are defined in. Note that this does *not* make any guarantees about
   the order of options within a section or the order in which sections get
   written back to disk on write()."""
  _ordered_sections = []

  def read(self, path):
    # Verifies a file exists *and* is readable
    if not os.access(path, os.R_OK):
        raise CarbonConfigException("Error: Missing config file or wrong perms on %s" % path)

    result = ConfigParser.read(self, path)
    sections = []
    for line in open(path):
      line = line.strip()

      if line.startswith('[') and line.endswith(']'):
        sections.append( line[1:-1] )

    self._ordered_sections = sections

    return result

  def sections(self):
    return list( self._ordered_sections ) # return a copy for safety


class Schema(object):
  def matches(self, metric):
    return bool(self.test(metric))


class DefaultSchema(Schema):
  __slots__ = ('name', 'archives')

  def __init__(self, name, archives):
    self.name = name
    self.archives = archives

  def test(self, metric):
    return True


class PatternSchema(Schema):
  __slots__ = ('name', 'pattern', 'regex', 'archives')

  def __init__(self, name, pattern, archives):
    self.name = name
    self.pattern = pattern
    self.regex = re.compile(pattern)
    self.archives = archives

  def test(self, metric):
    return self.regex.search(metric)


class Archive(object):
  __slots__ = ('secondsPerPoint', 'points')

  def __init__(self, secondsPerPoint, points):
    self.secondsPerPoint = int(secondsPerPoint)
    self.points = int(points)

  def __str__(self):
    return "Archive = (Seconds per point: %d, Datapoints to save: %d)" % (self.secondsPerPoint,
                                                                          self.points)

  def getTuple(self):
    return (self.secondsPerPoint, self.points)

  @staticmethod
  def fromString(retentionDef):
    (secondsPerPoint, points) = whisper.parseRetentionDef(retentionDef)
    return Archive(secondsPerPoint, points)

# default retention for unclassified data (7 days of minutely data)
defaultArchive = Archive(60, 60 * 24 * 7)
defaultSchema = DefaultSchema('default', [defaultArchive])
defaultAggregation = DefaultSchema('default', (None, None))


def load_schemas(schema_path, agg_path):
  """
  Load storage schemas

  Keyword arguments:
  schema_path -- the filesystem path to the storage-schems.conf file
  agg_path -- the filesystem path to the storage-aggregation.conf file
  """
  if not os.access(schema_path, os.R_OK):
    raise CarbonConfigException("Error: Missing config file or wrong perms on %s" % schema_path)
  config = ConfigParser()
  config.read(schema_path)
  sections = []
  for line in open(path):
    line = line.strip()
    if line.startswith('[') and line.endswith(']'):
      sections.append(line[1:-1])
  """
  Make some actual schema lists
  """
  schemaList = []
  for section in sections:
    options = dict(config.items(section))
    pattern = options.get('pattern')
    retentions = options['retentions'].split(',')
    archives = [Archive.fromString(s) for s in retentions]
    if pattern:
      mySchema = PatternSchema(section, pattern, archives)
    else:
      log.err("Section missing 'pattern': %s" % section)
      continue
    archiveList = [a.getTuple() for a in archives]
    try:
      whisper.validateArchiveList(archiveList)
      schemaList.append(mySchema)
    except whisper.InvalidConfiguration, e:
      log.msg("Invalid schemas found in %s: %s" % (section, e))
  schemaList.append(defaultSchema)

  tables = []
  for r in schemaList:
    full_reten = [t.getTuple() for t in r.archives]
    reten_str = ".".join("%s_%s" % tup for tup in full_reten)
    r_secs = 0
    for f in full_reten:
      r_secs += f[0] * f[1]
      tables.append(("%s.%s" % (f[0], reten_str), r_secs))

  aggList = []
  config = OrderedConfigParser()

  try:
    config.read(agg_path)
  except (IOError, CarbonConfigException):
    log.msg("%s not found or wrong perms, ignoring." % agg_path)

  for section in config.sections():
    options = dict(config.items(section))
    pattern = options.get('pattern')

    xFilesFactor = options.get('xfilesfactor')
    aggregationMethod = options.get('aggregationmethod')

    try:
      if xFilesFactor is not None:
        xFilesFactor = float(xFilesFactor)
        assert 0 <= xFilesFactor <= 1
      if aggregationMethod is not None:
        assert aggregationMethod in whisper.aggregationMethods
    except ValueError:
      log.msg("Invalid schemas found in %s." % section)
      continue

    archives = (xFilesFactor, aggregationMethod)

    if pattern:
      mySchema = PatternSchema(section, pattern, archives)
    else:
      log.err("Section missing 'pattern': %s" % section)
      continue
    aggList.append(mySchema)
  aggList.append(defaultAggregation)

  return tables, schemaList, aggList


def create_tables(data_tables, compress=None, host='localhost',
                  port=9090, transport='buffered', protocol='binary',
                  compat_level='0.94'):
  """
  Build all the HBase tables we'll need

  Keyword arguments:
  schemas -- storage-schemas.conf path
  host -- a host running thrift
  port -- the port the thrift instance is open on
  transport -- The type of thrift transport (buffered, framed, etc.) to use
  protocol -- The thrift protocol (binary, compact, etc.) to use
  compat_level -- What version of HBase should we limit ourselves to?
  """
  client = happybase.Connection(host=host, port=int(port),
                                table_prefix=TABLE_PREFIX,
                                transport=transport,
                                compat=compat_level,
                                protocol=protocol)
  sleep(0.25)
  try:
    tables = client.tables()
  except Exception, e:
    raise Exception(e)

  tabledict = {'compression': compress,
               'block_cache_enabled': True,
               'bloom_filter_type': "ROW",
               'max_versions': 1}
  meta_families = {META_CF_NAME: dict(tabledict)}
  data_families = {DATA_CF_NAME: dict(tabledict)}

  if META_SUFFIX not in tables:
    client.create_table(META_SUFFIX, meta_families)

  for r in data_tables:
    table_name, r_secs = r
    if table_name not in tables:
      data_families[DATA_CF_NAME]['time_to_live'] = r_secs
      try:
        client.create_table(table_name, data_families)
      except Exception, e:
        raise Exception(e)

  client.close()
