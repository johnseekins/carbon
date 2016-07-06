#!/usr/bin/env python
import argparse
import sys
import os
from time import time
import happybase


def _get_nodes(query):
  """
  Since find_nodes returns a generator,
  we need to get *all* results from each search
  or the recursion below will eat our generators
  for lunch.
  """
  search_db = hbase.HBaseFinder()
  metric_list = []
  branch_list = []
  column_list = []
  for node in list(search_db.find_nodes(query)):
    try:
      path = node.path
    except Exception:
      continue
    if isinstance(node, LeafNode):
      metric_list.append(path)
    else:
      branch_list.append(path)
      column_path = path.split('.')
      if len(column_path) > 1:
        head = '.'.join(column_path[:-1])
        column = column_path[-1]
        column_list.append((head, column))
      else:
        column_list.append(('ROOT', column_path[0]))
      new_node = FindQuery(path, 0, time())
      mlst, blst, clst = _get_nodes(new_node)
      metric_list.extend(mlst)
      branch_list.extend(blst)
      column_list.extend(clst)

  return metric_list, branch_list, column_list


def cli_opts():
  parser = argparse.ArgumentParser(
      description="Delete a single metric or a group of metrics from HBase")
  parser.add_argument('--metric', '-m',
                      action='store', dest='metric', required=True,
                      help='The metric (or pattern) to search for')
  parser.add_argument('--dry-run', '-d',
                      action='store_true', dest='dry_run', default=False,
                      help="Don't actually delete anything")
  parser.add_argument('-l', '--lib-dir',
                      help='The carbon lib directory',
                      action='store', dest='lib_dir',
                      default='/opt/graphite/lib')
  parser.add_argument('-w', '--web-dir',
                      help='The graphite webapp directory',
                      action='store', dest='web_dir',
                      default='/opt/graphite/webapp/')
  return parser.parse_args()

if __name__ == '__main__':
  opts = cli_opts()
  sys.path.append(opts.lib_dir)
  from carbon.conf import settings, Settings
  os.environ['DJANGO_SETTINGS_MODULE'] = 'graphite.settings'
  sys.path.append(opts.web_dir)
  from graphite.local_settings import CONF_DIR as gConfDir
  from graphite.finders import hbase
  from graphite import finders
  from graphite.storage import FindQuery
  from graphite.node import LeafNode

  settings = Settings()
  settings.readFrom("%s/carbon.conf" % gConfDir, 'cache')
  settings['CONF_DIR'] = gConfDir

  print("Searching for %s" % opts.metric)
  print("Larger patterns can take time...")
  metric_query = FindQuery(opts.metric, 0, time())
  metric_list, branch_list, column_list = _get_nodes(metric_query)

  if opts.dry_run:
    print("Would have deleted the following metrics:")
    print(metric_list)
    print("Would have deleted the following branches:")
    print(branch_list)
    print("Would have deleted the following columns:")
    print(column_list)
    print("Exiting without deleting")
    exit(1)

  client = happybase.Connection(host=settings['HBASE_THRIFT_HOST'],
                                port=int(settings['HBASE_THRIFT_PORT']),
                                table_prefix='graphite',
                                transport=settings['HBASE_TRANSPORT_TYPE'],
                                compat=str(settings['HBASE_COMPAT_LEVEL']),
                                protocol=settings['HBASE_PROTOCOL'])
  batch = client.table('META').batch()

  for metric in metric_list:
    print('Deleting metric %s' % metric)
    batch.delete(metric)

  for branch in branch_list:
    print('Deleting branch %s' % branch)
    batch.delete(branch)

  for column in column_list:
    print("Deleting column m:c_%s from row %s" % (column[1], column[0]))
    batch.delete(column[0], columns=['m:c_%s' % column[1]])

  batch.send()
