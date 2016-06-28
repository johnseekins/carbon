#!/usr/bin/env python
import argparse
import sys
import os
from time import time


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
    for node in list(search_db.find_nodes(query)):
        if isinstance(node, LeafNode):
            metric_list.append(node.path)
        else:
            branch_list.append(node.path)
            new_node = FindQuery(node.path, 0, time())
            mlst, blst = _get_nodes(new_node)
            metric_list.extend(mlst)
            branch_list.extend(blst)
    return metric_list, branch_list


def metric_delete(metric, dry_run=False):
    db = HBaseDB()
    if any(ex in metric for ex in patterns):
        print("Searching for %s" % metric)
        print("Larger patterns can take time...")
        metric_query = FindQuery(metric, 0, time())
        metric_list, branch_list = _get_nodes(metric_query)
    else:
        metric_list = [metric]
        branch_list = [metric]

    if dry_run:
        print("Would have deleted the following metrics:")
        print(metric_list)
        print("Would have deleted the following branches:")
        print(branch_list)
        print("Exiting without deleting")
        exit(1)

    for metric in metric_list:
        print('Deleting %s from data table' % metric)
        db.delete(metric)

    batch = db.meta_table.batch(batch_size=db.batch_size)

    for branch in branch_list:
        print('Deleting branch %s' % branch)
        batch.delete(branch)
    batch.send()


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
    from carbon.conf import settings
    from carbon.hbase import HBaseDB
    os.environ['DJANGO_SETTINGS_MODULE'] = 'graphite.settings'
    sys.path.append(opts.web_dir)
    from graphite.local_settings import CONF_DIR as gConfDir
    from graphite.finders import hbase, match_patterns
    from graphite.storage import FindQuery
    from graphite.node import LeafNode
    settings['CONF_DIR'] = gConfDir
    metric_delete(opts.metric, opts.dry_run)
