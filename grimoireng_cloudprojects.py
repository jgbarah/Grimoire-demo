#! /usr/bin/python
# -*- coding: utf-8 -*-

## Copyright (C) 2015 Bitergia
##
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##
## Example:
## python grimoireng_cloudprojects.py --user root --port 3307 --esauth readwrite XXX --verbose --delete

import grimoireng_data
import argparse
from collections import OrderedDict
import logging

description = """
Produce data for NG dashboards for some cloud projects.

It uses information in CVSAnalY, Bicho/Gerrit, and SortingHat databases
to produce the JSON files needed by a GrimoireNG dashboard.

"""

def parse_args ():
    """
    Parse command line arguments

    """

    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("--user", required = True,
                        help = "User to access the databases")
    parser.add_argument("--passwd", default = "",
                        help = "Password to access the databases " + \
                        "(default: no password)")
    parser.add_argument("--host",  default = "127.0.0.1",
                        help = "Host where the databases reside" + \
                        "(default: 127.0.0.1)")
    parser.add_argument("--port",  type = int, default = 3306,
                        help = "Port to access the databases" + \
                        "(default: 3306, standard MySQL port)")
    parser.add_argument("--output", default = "",
                        help = "Output directory")
    parser.add_argument("--deleteold", action = 'store_true',
                        help = "Delete old contents in output system")
    parser.add_argument("--verbose", action = 'store_true',
                        help = "Be verbose")
    parser.add_argument("--debug", action = 'store_true',
                        help = "Show debugging information")
    parser.add_argument("--since",
                        help = "Consider only commits since specified date " + \
                        "(YYYY-MM-DD format)")
    parser.add_argument("--esauth", nargs=2, default = None,
                        help = "Authentication to access ElasticSearch" + \
                        "(eg: user password)")
    
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    
    args = parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO)

    logging.info("Starting...")
    allbranches = True
    dateformat = "utime"
    # elasticsearch = None 
    elasticsearch = [
        "https://789ba13a7edced40de95ef091ac591d3.us-east-1.aws.found.io:9243",
        "scm"
        ]
    dashboards = OrderedDict ([
        ("ElasticSearch", {"scmdb": "quan_cvsanaly_elastic_6863",
                           "scrdb": None,
                           "shdb": "quan_sortinghat_elastic_6863",
                           "prjdb": None,
                           "port": 3307}),
        ("Kubernetes", {"scmdb": "lcanas_cvsanaly_kubernetes_oscon2015",
                        "scrdb": None,
                        "shdb": "lcanas_sortinghat_kubernetes_oscon2015",
                        "prjdb": None,
                        "port": 3307}),
        ("Docker", {"scmdb": "quan_cvsanaly_docker_6753",
                    "scrdb": None,
                    "shdb": "quan_sortinghat_docker_6753",
                    "prjdb": None,
                    "port": 3307}),
        ("Puppet", {"scmdb": "puppet_2015q3_cvsanaly",
                    "scrdb": None,
                    "shdb": "puppet_2015q3_sortinghat",
                    "prjdb": None,
                    "port": 3307}),
        ("Midokura", {"scmdb": "cp_cvsanaly_midokura",
                      "scrdb": None,
                      "shdb": "cp_sortinghat_midokura",
                      "prjdb": None,
                      "port": 3307})
    ])

    for dashboard, params in dashboards.iteritems():
        logging.info("** Producing data for " + dashboard)
        if "port" in params:
            port = params["port"]
        else:
            port = args.port
        if elasticsearch:
            elasticsearch[1] = dashboard.lower() + "-scm"
        grimoireng_data.process_all (
            user = args.user, passwd = args.passwd,
            host = args.host, port = port,
            scmdb = params["scmdb"],
            scrdb = params["scrdb"],
            shdb = params["shdb"],
            prjdb = params["prjdb"],
            allbranches = allbranches,
            since = args.since,
            output = args.output,
            elasticsearch = elasticsearch,
            esauth = args.esauth,
            dateformat = dateformat,
            dashboard = dashboard,
            deleteold = args.deleteold,
            verbose = args.verbose,
            debug = args.debug
        )
