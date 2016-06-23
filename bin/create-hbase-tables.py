#!/usr/bin/env python
import argparse
import sys
import os


def cli_opts():
    parser = argparse.ArgumentParser("Create HBase tables for Graphite")
    parser.add_argument('-l', '--lib-dir',
                        help='The carbon lib directory',
                        action='store', dest='lib_dir',
                        default='/opt/graphite/lib')
    parser.add_argument('-w', '--web-dir',
                        help='The graphite webapp directory',
                        action='store', dest='web_dir',
                        default='/opt/graphite/webapp/')
    return parser.parse_args()

opts = cli_opts()
sys.path.append(opts.lib_dir)
from carbon.conf import settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'graphite.settings'
sys.path.append(opts.web_dir)
from graphite.local_settings import CONF_DIR as gConfDir
settings['CONF_DIR'] = gConfDir
from carbon import hbase

try:
    hbase.create_tables(os.path.join(settings['CONF_DIR'], 'storage-schemas.conf'),
                        os.path.join(settings['CONF_DIR'], 'whitelist.conf'),
                        host=settings['HBASE_THRIFT_HOST'],
                        port=config['HBASE_THRIFT_PORT'],
                        table_prefix=config['HBASE_TABLE_PREFIX'],
                        transport=config['HBASE_TRANSPORT_TYPE'],
                        protocol=config['HBASE_PROTOCOL'])
except Exception:
    print("Failed to read config options...trying defaults")
    hbase.create_tables(os.path.join(settings['CONF_DIR'], 'storage-schemas.conf'),
                    os.path.join(settings['CONF_DIR'], 'whitelist.conf'))
