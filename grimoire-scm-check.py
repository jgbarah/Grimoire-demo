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
## Check some properties of a SCM database.
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

import MySQLdb
import logging
import pandas
from collections import OrderedDict
from os.path import join
from datetime import datetime
from jsonpickle import encode, set_encoder_options

## Variables to access the databases

# OpenStack with Sorting Hat in atari
user = "root"
password = ""
host = "127.0.0.1"
port = 3308
scm_db="dic_cvsanaly_openstack_4114_sh"
sh_db="amartin_sortinghat_openstack"

# OpenStack with Sorting Hat in jgb's laptop
# user = "jgb"
# password = "XXX"
# host = "127.0.0.1"
# port = 3306
# scm_db="dic_cvsanaly_openstack_4114_sh"
# sh_db="amartin_sortinghat_openstack"

# OpenShift with Sorting Hat in spectrum
# user = "root"
# password = ""
# host = "127.0.0.1"
# port = 3307
# scm_db="amartin_cvsanaly_openshift_ext"
# sh_db="amartin_sortinghat_openshift_ext"

def connect():
    """Connects to the databases."""
    try:
        db = MySQLdb.connect(user = user, passwd = password,
                             host = host, port = port,
                             use_unicode = True)
        return db, db.cursor()
    except:
        logging.error("Database connection error")
        raise
        
def execute_query(connector, query):
    """Execute a query."""
    
    query = query.format(scm_db = scm_db, sh_db = sh_db)
#    print query
    results = int (connector.execute(query))
    if results > 0:
        return connector.fetchall()
    else:
        return []

def check_scmlog_join ():
    """Check that joining people:uidentities don't alter the number of commits."""
    
    # Get non-repeated commits in scmlog
    query_scmlog = """SELECT scmlog.rev,
  scmlog.author_id
FROM {scm_db}.scmlog
GROUP BY scmlog.rev
"""
    scmlog = execute_query(cursor, query_scmlog)

    # Get non-repeated commits in scmlog, joining people_uidentities
    query_scmlog_pu = """SELECT scmlog.rev AS rev,
  scmlog.author_id
FROM {scm_db}.scmlog
JOIN {scm_db}.people_uidentities
  ON people_uidentities.people_id = scmlog.author_id
GROUP BY scmlog.rev
"""
    scmlog_pu = execute_query(cursor, query_scmlog_pu)

    # Print number of commits calculated in both ways
    print
    print "Commits (scmlog): ", len(scmlog)
    print "Commits (scmlog joining people_uidentities): ", len(scmlog_pu)
    print
    if  len(scmlog) != len(scmlog_pu):
        # Different nuber of commits, print differences
        scmlog_df = pandas.DataFrame(list(scmlog), columns=["rev", "author"])
        scmlog_pu_df = pandas.DataFrame(list(scmlog_pu), columns=["rev", "author"])
        merged_df = pandas.merge (scmlog_df, scmlog_pu_df,
                              on = "rev",
                              how = "outer")
        diff_df = merged_df[merged_df.isnull().any(axis=1)]
        print "Diff"
        print diff_df
        print
        print "Error! Number of commits not equal when joining people_uidentities."
        print "See above for offending commits"
        return False
    else:
        print "Passed!"
        return True

def check_project_repos_repeated ():
    """Check for repeated entries in project_repositories.
    """

    query_pr_repeated = """SELECT *
FROM (
   SELECT *, COUNT(repository_name) AS number
   FROM {scm_db}.project_repositories
   WHERE data_source = "scm"
   GROUP BY repository_name
   ) pr
WHERE pr.number > 1
"""

    pr_repeated = execute_query(cursor, query_pr_repeated)

    # Print number of repeated entries in project_repositories
    print
    print "Repeated entries (project_repositories): ", len(pr_repeated)
    if  len(pr_repeated) > 0:
        pr_repeated_df = pandas.DataFrame(list(pr_repeated),
                                          columns=["id", "type", "uri", "number"])
        print pr_repeated_df
        print
        print "Error! Repeated entries in project_repositories" 
        print "See above for list of repeated entries"
        return False
    else:
        print "Passed!"
        return True

## Starting the game...
db, cursor = connect()

check_project_repos_repeated ()
check_scmlog_join ()

