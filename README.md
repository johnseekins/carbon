# Carbon

## Overview

Carbon is one of three components within the Graphite project:

1. [Graphite-Web](https://github.com/graphite-project/graphite-web), a Django-based web application that renders graphs and dashboards
2. The Carbon metric processing daemons
3. HBase Storage Engine

## Writing Data

![Writing Data](https://github.com/johnseekins/carbon/blob/hbase/WritingData.jpg "Writing Data")
![Writing Meta Data](https://github.com/johnseekins/carbon/blob/hbase/WritingMetaData.jpg "Writing Meta Data")

Carbon is responsible for receiving metrics over the network, caching them in memory for "hot queries" from the Graphite-Web application, and persisting them to HBase.

## Performance

This carbon instance also utilizes memcache (http://www.memcached.org/) to store existing meta information about metrics to reduce read requests (gets) to HBase. This requires a working memcache instance and the following config in carbon.conf:
```
# Memcache settings (for reducing reads in the cluster)
#
MEMCACHE_HOSTS = localhost:11211,
```

## Installation, Configuration and Usage

Please refer to the instructions at [readthedocs](http://graphite.readthedocs.org/).

## License

Carbon is licensed under version 2.0 of the Apache License. See the [LICENSE](https://github.com/graphite-project/carbon/blob/master/LICENSE) file for details.
