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
## Examples:
## python grimoireng_cloudprojects.py --user root --port 3307 \
##   --esauth readwrite XXX --verbose --delete
## python grimoireng_cloudprojects.py --user root --esauth readwrite XXX \
##  --verbose --delete --dashboards FirefoxOS --config config.py

import grimoireng_data
import argparse
import logging
import sys
import imp

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
    parser.add_argument("--user", default = "root",
                        help = "User to access the databases (default root)")
    parser.add_argument("--passwd", default = "",
                        help = "Password to access the databases " + \
                        "(default: no password)")
    parser.add_argument("--host",  default = "127.0.0.1",
                        help = "Host where the databases reside" + \
                        "(default: 127.0.0.1)")
    parser.add_argument("--port",  type = int, default = 3306,
                        help = "Port to access the databases" + \
                        "(default: 3306, standard MySQL port)")
    parser.add_argument("--dashboards", default = "all",
                        nargs='+',
                        help = "Dashboard to generate, according to the " + \
                        "configuration, 'all' to generate all dashboards (default)")
    parser.add_argument("--list_dashboards",  action = 'store_true',
                        help = "List dashboards in configuration (do nothing else)")
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
    parser.add_argument("--batchsize",  type = int, default = 10000,
                        help = "Size of batches for uploading data" + \
                        "(default: 10,000 items)")
    parser.add_argument("--config", default = "grimoireng_config.py",
                        help = "Configuration file")

    args = parser.parse_args()
    return args


if __name__ == "__main__":

    args = parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO)

    logging.info("Reading config from " + args.config)
    # importlib.import_module(args.config) as config
    config = imp.load_source("config", args.config)

    logging.info("Starting...")
    allbranches = True
    dateformat = "utime"
    elasticsearch = config.elasticsearch
    dashboards = config.dashboards

    # List dashbords, if asked to do so
    if args.list_dashboards:
        for dashboard in dashboards:
            print dashboard
        sys.exit()
    # Which dashboards should we produce?
    dashboards_produce = []
    if "all" in args.dashboards:
        dashboards_produce = dashboards.iteritems()
    else:
        for dashboard in args.dashboards:
            if dashboard in dashboards:
                dashboards_produce.append([dashboard, dashboards[dashboard]])
            else:
                logging.info("No configuration for " + dashboard \
                             + ", not producing it")
    # Go and produce them!!
    for dashboard, params in dashboards_produce:
        logging.info("** Producing data for " + dashboard)
        if "port" in params:
            port = params["port"]
        else:
            port = args.port
        if params["scmdb"]:
            if elasticsearch:
                elasticsearch[1] = dashboard.lower() + "-scm"
            grimoireng_data.process_all (
                user = args.user, passwd = args.passwd,
                host = args.host, port = port,
                scmdb = params["scmdb"],
                shdb = params["shdb"],
                prjdb = params["prjdb"],
                allbranches = allbranches,
                since = args.since,
                output = args.output,
                elasticsearch = elasticsearch,
                esauth = args.esauth,
                dateformat = dateformat,
                dashboard = dashboard,
                batchsize = args.batchsize,
                deleteold = args.deleteold,
                verbose = args.verbose,
                debug = args.debug
            )
        if params["scrdb"]:
            if elasticsearch:
                elasticsearch[1] = dashboard.lower() + "-scr"
            grimoireng_data.process_all (
                user = args.user, passwd = args.passwd,
                host = args.host, port = port,
                scrdb = params["scrdb"],
                shdb = params["shdb"],
                prjdb = params["prjdb"],
                since = args.since,
                output = args.output,
                elasticsearch = elasticsearch,
                esauth = args.esauth,
                dateformat = dateformat,
                dashboard = dashboard,
                batchsize = args.batchsize,
                deleteold = args.deleteold,
                verbose = args.verbose,
                debug = args.debug
            )
