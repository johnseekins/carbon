#!/usr/bin/env python
import argparse
import sys
import os
from time import time
import happybase

patterns = ['{', '}', '[', ']', '*', '?']


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

    settings = Settings()
    settings.readFrom("%s/carbon.conf" % gConfDir, 'cache')
    settings['CONF_DIR'] = gConfDir

    parts = opts.metric.split('.')
    if len(parts) > 2 or any(p in opts.metric for p in patterns):
        print("metric name must be an explicit root node!")
        exit(1)

    if opts.dry_run:
        print("Would delete %s and ROOT column m:c_%s" % (opts.metric, opts.metric))
        exit(1)

    client = happybase.Connection(host=settings['HBASE_THRIFT_HOST'],
                                  port=int(settings['HBASE_THRIFT_PORT']),
                                  table_prefix='graphite',
                                  transport=settings['HBASE_TRANSPORT_TYPE'],
                                  compat=str(settings['HBASE_COMPAT_LEVEL']),
                                  protocol=settings['HBASE_PROTOCOL'])
    tbl = client.table('META')
    tbl.delete(opts.metric)
    tbl.delete('ROOT', columns=['m:c_%s' % opts.metric])

