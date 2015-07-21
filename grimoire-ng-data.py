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
## grimoire-ng-data.py --user jgb --passwd XXX --scmdb dic_cvsanaly_openstack_4114_sh --shdb amartin_sortinghat_openstack --output openstack

import argparse
import MySQLdb
import logging
import pandas
from collections import OrderedDict
import codecs
import json
from os.path import join
from datetime import datetime
from jsonpickle import encode, set_encoder_options

description = """
Produce JSON files needed by a GrimoireNG dashboard.

It uses information in CVSAnalY and SortingHat databases to produce the JSON
files needed by a GrimoireNG dashboard.
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
    parser.add_argument("--scmdb", required = True,
                        help = "SCM (git) database")
    parser.add_argument("--shdb", required = True,
                        help = "SortingHat database")
    parser.add_argument("--output", default = "",
                        help = "Output directory")
    parser.add_argument("--verbose", action = 'store_true',
                        help = "Be verbose")
    parser.add_argument("--allbranches", action = 'store_true',
                        help = "Produce data with commits in all branches " + \
                        "(default, only master")
    parser.add_argument("--since",
                        help = "Consider only commits since specified date " + \
                        "(YYYY-MM-DD format)")
    
    args = parser.parse_args()
    return args


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
        
    if isinstance(obj, datetime):
#        serial = obj.isoformat()
        serial = (obj - datetime(1970,1,1)).total_seconds()
        return serial
    raise TypeError ("Type not serializable")

def json_dumps(data, compact = True):
    if compact:
        return json.dumps(data, sort_keys=False,
                          separators=(',',':'),
                          default=json_serial)
    else:
        return json.dumps(data, sort_keys=False, 
                          indent=4, separators=(',', ': '),
                          default=json_serial)

def produce_json (filename, data, compact = True):
    """Produce JSON content (data) into a file (filename).

    Parameters
    ----------

    filename: string
       Name of file to write the content.
    data: any
       Content to write in JSON format. It has to be ready to pack using
       jsonpickle.encode.

    """

    json_dict = OrderedDict()
    json_dict['names'] = list(data.columns.values)
    json_dict['values'] = data.values.tolist()

    data_json = json_dumps(json_dict, compact)
    with codecs.open(filename, "w", "utf-8") as file:
        file.write(data_json)


def create_report (report_files, destdir):
    """Create report, by producing a collection of JSON files

    Parameters
    ----------

    report_files: dictionary
       Keys are the names of JSON files to produce, values are the
       data to include in those JSON files.
    destdir: str
       Name of the destination directory to write all JSON files

    """

    for file in report_files:
        print "Producing file: ", join (destdir, file)
        produce_json (join (destdir, file), report_files[file])

class Database:
    """To work with a database (likely including several schemas).
    """
    
    def _connect(self):
        """Connect to the MySQL database.
        """
        
        try:
            db = MySQLdb.connect(user = self.user, passwd = self.passwd,
                                 host = self.host, port = self.port,
                                 use_unicode = True)
            return db, db.cursor()
        except:
            logging.error("Database connection error")
            raise

    def execute(self, query, show = False):
        """Execute an SQL query with the corresponding database.

        The query can be "templated" with {scm_db} and {sh_db}.
        """
        
        sql = query.format(scm_db = self.scmdb,
                           sh_db = self.shdb)
        if show:
            print sql
        results = int (self.cursor.execute(sql))
        cont = 0
        if results > 0:
            result1 = self.cursor.fetchall()
            return result1
        else:
            return []

    def __init__ (self, user, passwd, host, port, scmdb, shdb):
        self.user = user
        self.passwd = passwd
        self.host = host
        self.port = port
        self.scmdb = scmdb
        self.shdb = shdb
        self.db, self.cursor = self._connect()

def query_persons (allbranches, since, verbose):
    """ Execute query to select persons."""
    sql = """SELECT uidentities.uuid AS uuid,
  profiles.name AS name,
  profiles.is_bot AS bot
FROM {scm_db}.scmlog
  JOIN {scm_db}.people_uidentities
    ON people_uidentities.people_id = scmlog.author_id
  JOIN {sh_db}.uidentities
    ON uidentities.uuid = people_uidentities.uuid
  LEFT JOIN {sh_db}.profiles
    ON uidentities.uuid = profiles.uuid
  JOIN {scm_db}.actions
    ON scmlog.id = actions.commit_id
"""
    if not allbranches:
        sql = sql + """JOIN {scm_db}.branches
    ON branches.id = actions.branch_id 
WHERE branches.name IN ("master")
"""
        if since:
            sql = sql + 'AND scmlog.author_date >= "' + since + '" '
    else:
        sql = sql + 'WHERE scmlog.author_date >= "' + since + '" '
    sql = sql + "GROUP BY uidentities.uuid"
    persons = db.execute(sql, verbose)
    return persons

def query_commits (allbranches, verbose):
    """Execute query to select commits."""

    sql = """SELECT scmlog.id AS id, 
  scmlog.date AS date,
  uidentities.uuid AS person_id,
  profiles.name AS name,
  enrollments.organization_id AS org_id,
  scmlog.repository_id AS repo_id,
  LEFT(LTRIM(scmlog.message), 30) as message,
  scmlog.rev as hash,
  (((scmlog.author_date_tz DIV 3600) + 36) % 24) - 12 AS tz,
  scmlog.author_date_tz AS tz_orig,
  organizations.name AS org_name
FROM {scm_db}.scmlog
  JOIN {scm_db}.people_uidentities
    ON people_uidentities.people_id = scmlog.author_id
  JOIN {sh_db}.uidentities
    ON uidentities.uuid = people_uidentities.uuid
  LEFT JOIN {sh_db}.enrollments
    ON uidentities.uuid = enrollments.uuid
  LEFT JOIN {sh_db}.organizations
    ON enrollments.organization_id = organizations.id
  LEFT JOIN {sh_db}.profiles
    ON uidentities.uuid = profiles.uuid
  JOIN {scm_db}.actions
    ON scmlog.id = actions.commit_id
  JOIN {scm_db}.branches
    ON branches.id = actions.branch_id
"""
    if allbranches:
        sql = sql + "WHERE "
    else:
        sql = sql + 'WHERE branches.name IN ("master") AND '
    sql = sql + \
"""        (enrollments.start IS NULL OR
    (scmlog.date > enrollments.start AND scmlog.date < enrollments.end))
GROUP BY scmlog.rev
"""
    commits = db.execute(sql, verbose)
    return commits

# Query to select organizations
query_orgs = """SELECT enrollments.organization_id AS org_id,
  {sh_db}.organizations.name AS org_name
FROM {scm_db}.scmlog
  JOIN {scm_db}.people_uidentities
    ON people_uidentities.people_id = scmlog.author_id
  JOIN {sh_db}.uidentities
    ON uidentities.uuid = people_uidentities.uuid
  JOIN {sh_db}.enrollments
    ON uidentities.uuid = enrollments.uuid
  JOIN {sh_db}.organizations
    ON enrollments.organization_id = organizations.id
  JOIN {scm_db}.actions
    ON scmlog.id = actions.commit_id
  JOIN {scm_db}.branches
    ON branches.id = actions.branch_id 
WHERE enrollments.start IS NULL OR
  (scmlog.date > enrollments.start AND scmlog.date < enrollments.end)
GROUP BY org_id"""

#  ORDER BY repo_id should not be needed, but there are some double
#  entries in project_repositories table, at least in OpenStack, which
#  cause dupped entries for repositories.
query_repos = """SELECT repositories.id AS repo_id,
  repositories.name AS repo_name,
  projects.project_id AS project_id,
  projects.id AS project
FROM {scm_db}.repositories
LEFT JOIN {scm_db}.project_repositories
  ON repositories.uri = project_repositories.repository_name
    AND repositories.type = "git"
LEFT JOIN {scm_db}.projects
  ON projects.project_id = project_repositories.project_id
GROUP BY repo_id ORDER BY repo_id"""

# Query to select repositories when there are no projects tables
query_repos_noprojects = """SELECT repositories.id AS repo_id,
  repositories.name AS repo_name,
  0 AS project_id,
  "No project" AS project
FROM {scm_db}.repositories
ORDER BY repo_id"""


if __name__ == "__main__":

    import _mysql_exceptions
    
    args = parse_args()

    if args.allbranches:
        print "Analyzing all git branches."
    else:
        print "Analyzing only git master branch."
    if args.since:
        print "Analyzing since: " + args.since + "."
    else:
        print "Analyzing since the first commit."
    db = Database (user = args.user, passwd = args.passwd,
                   host = args.host, port = args.port,
                   scmdb = args.scmdb, shdb = args.shdb)

    print "Querying for persons"
    persons = query_persons (args.allbranches, args.since, args.verbose)
    print "Querying for organizations"
    orgs = db.execute(query_orgs, args.verbose)
    print "Querying for commits"
    commits = query_commits (args.allbranches, args.verbose)
    # Produce repos data, with or without projects, depending
    # on the availability of the projects table
    print "Querying for repositories"
    try:
        check = db.execute("SELECT 1 FROM {scm_db}.projects LIMIT 1", args.verbose)
        projects_exist = True
    except _mysql_exceptions.ProgrammingError, e:
        if e[0] == 1146:
            projects_exist = False
        else:
            raise
    if projects_exist:
        repos = db.execute(query_repos, args.verbose)
    else:
        print "No projects tables, using just one project"
        repos = db.execute(query_repos_noprojects, args.verbose)

    # Produce commits data
    print "Commits: ", len(commits)
    commits_df = pandas.DataFrame(list(commits), columns=["id", "date", "author_uuid", "name", "org_id", "repo_id", "message", "hash", "tz", "tz_orig", "org_name"])
    commits_df["org_id"].fillna(0, inplace=True)
    commits_df["org_name"].fillna("Unknown", inplace=True)
    commits_df["name"] = commits_df["name"] + " (" + commits_df["org_name"] + ")"

    print "Organizations: ", len(orgs)
    orgs_df = pandas.DataFrame([(0,"Unknown"),] + list(orgs),
                               columns=["org_id", "org_name"])
    
    print "Repositories: ", len(repos)
    repos_df = pandas.DataFrame(list(repos),
                                columns=["repo_id", "repo_name",
                                         "project_id", "project_name"])
    repos_df["project_id"].fillna(0, inplace=True)
    repos_df["project_name"].fillna("No project", inplace=True)
    
    # Capitalizing could be a good idea, but not by default.
    # repos_df["repo_name"] = repos_df["repo_name"].str.capitalize()
    # repos_df["project_name"] = repos_df["project_name"].str.capitalize()

    print "Persons: ", len(persons)
    persons_df = pandas.DataFrame(list(persons), columns=["uuid", "name", "bot"])
    persons_df["id"] = persons_df.index

    # Produce packed (minimal) commits dataframe
    commits_pkd_df = pandas.merge (commits_df, persons_df,
                                   left_on="author_uuid", right_on="uuid",
                                   how="left")
    commits_pkd_df = commits_pkd_df[["id_x", "date", "id_y", "org_id",
                                     "repo_id", "tz"]]
    commits_pkd_df.columns = ["id", "date", "author", "org", "repo", "tz"]

    # Produce messages and hashes dataframe for commits
    commits_messages_df = commits_df[["id", "message", "hash"]]
    
    prefix = join (args.output, "scm-")
    report = OrderedDict()

    report[prefix + 'commits.json'] = commits_pkd_df
    report[prefix + 'messages.json'] = commits_messages_df
    report[prefix + 'orgs.json'] = orgs_df
    report[prefix + 'repos.json'] = repos_df
    report[prefix + 'persons.json'] = persons_df
    create_report (report_files = report, destdir = './')

