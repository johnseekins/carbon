#!/usr/bin/env python
import os
import sys
import argparse
from time import time


def cli_opts():
    parser = argparse.ArgumentParser("Read data from distributed backend")
    parser.add_argument('-l', '--lib-dir',
                        help='The carbon lib directory',
                        action='store', dest='lib_dir',
                        default='/opt/graphite/lib/')
    parser.add_argument('-w', '--web-dir',
                        help='The graphite webapp directory',
                        action='store', dest='web_dir',
                        default='/opt/graphite/webapp/')
    parser.add_argument("-q", '--query',
                        help="The actual query to search for",
                        required=True, action='store')
    parser.add_argument("-s", "--start", default=int(time()) - 3600,
                        help="Start time")
    parser.add_argument("-e", "--end", default=int(time()),
                        help="End time")
    return parser.parse_args()

opts = cli_opts()

sys.path.append(opts.lib_dir)
from carbon.conf import settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'graphite.settings'
sys.path.append(opts.web_dir)
from graphite.local_settings import CONF_DIR as gConfDir
settings['CONF_DIR'] = gConfDir
from graphite.storage import HBaseFinder

search = HBaseFinder()
print("Will search for %s" % opts.query)
for o in search.find(opts.query):
    print(o.real_metric)
    if o.isLeaf():
        res = o.fetch(int(opts.start), int(opts.end), int(time()))
        print(res)
    else:
        print("Branch!")

