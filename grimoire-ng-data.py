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
## grimoire-ng-data.py --user jgb --passwd XXX --scmdb dic_cvsanaly_openstack_4114_sh --shdb amartin_sortinghat_openstack --prjdb amartin_projects_openstack_sh --output openstack

## grimoire-ng-data.py --user root --port 3308 --scmdb amartin_cvsanaly_openstack_sh --shdb amartin_sortinghat_openstack_sh --prjdb amartin_projects_openstack_sh --allbranches --elasticsearch http://localhost:9200 scm --since 2015-10-01

import argparse
import MySQLdb
import logging
import pandas
from collections import OrderedDict
import codecs
import json
from os.path import join
import datetime
from jsonpickle import encode, set_encoder_options
import urllib2

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
    parser.add_argument("--scmdb", required = False,
                        help = "SCM (git) database")
    parser.add_argument("--scrdb", required = False,
                        help = "SCR (Gerrit) database")
    parser.add_argument("--shdb", required = True,
                        help = "SortingHat database")
    parser.add_argument("--prjdb", required = False,
                        help = "Projects database (if not specified, same as SCM database)")
    parser.add_argument("--output", default = "",
                        help = "Output directory")
    parser.add_argument("--dateformat",
                        help = "Date format ('utime' or 'iso')")
    parser.add_argument("--verbose", action = 'store_true',
                        help = "Be verbose")
    parser.add_argument("--allbranches", action = 'store_true',
                        help = "Produce data with commits in all branches " + \
                        "(default, only master)")
    parser.add_argument("--since",
                        help = "Consider only commits since specified date " + \
                        "(YYYY-MM-DD format)")
    parser.add_argument("--elasticsearch", nargs=2,
                        help = "Url of elasticsearch, and index to use " + \
                        "(eg: http://localhost:9200 project)")
    
    args = parser.parse_args()
    return args


def json_serial(obj, dateformat = "iso"):
    """JSON serializer for objects not serializable by default json code.

    Currently serializes everything "as by default", except for
    datetime, which is serialized as datetime.isoformat(), if
    dateformat is "iso" (default), or as Unix time (number of microseconds
    since January 1st 1970), if dateformat is "utime"

    :param object: object to serialize
    :param dateformat: format to produce for datetime ("iso" or "utime")
    :type dateformat: str or unicode
    :returns: serialized string
    :rtype: str or unicode

    """

    if isinstance(obj, datetime.datetime):
        if dateformat == "iso":
            serial = obj.isoformat()
        elif dateformat =="utime":
            serial = int((obj - datetime.datetime(1970,1,1)).total_seconds())
        else:
            raise ValueError ("Unknown format for datetime: " + str(dateformat))
        return serial
    elif isinstance(obj, datetime.timedelta):
        return int(obj.total_seconds())
    raise TypeError ("Type not serializable: " + type(obj).__name__)

def json_serial_iso (obj):
    """JSON serializer, using datetime.isoformat() for datetime objects.

    """

    return json_serial (obj, dateformat = "iso")

def json_serial_utime (obj):
    """JSON serializer, using Unix time for datetime objects.

    """

    return json_serial (obj, dateformat = "utime")

    
def json_dumps(data, compact = True, dateformat = "iso"):
    """Dumps data to a JSON string.

    :param data: data to serialize
    :param compact: use compact output
    :type compact: bool
    :param dateformat: format for datetime objects ("iso" or "utime")
    :type dateformat: str or unicode
    :returns: serialized string
    :rtype: str or unicode

    """

    if dateformat == "iso":
        serializer = json_serial_iso
    elif dateformat == "utime":
        serializer = json_serial_utime
    else:
        raise ValueError ("Unknown format for datetime: " + str(dateformat))
    if compact:
        return json.dumps(data, sort_keys=False,
                          separators=(',',':'),
                          default=serializer)
    else:
        return json.dumps(data, sort_keys=False, 
                          indent=4, separators=(',', ': '),
                          default=serializer)

def write_file_json (filename, data, compact = True, dateformat = "utime"):
    """Write JSON content (a dataframe) into a file (filename).

    :param filename: Name of file to write the content.
    :type filename: str or unicode
    :param data: dataframe  to serialize
    :type data: dataframe
    :param compact: use compact output
    :type compact: bool
    :param dateformat: format for datetime objects ("iso" or "utime")
    :type dateformat: str or unicode

    """

    json_dict = OrderedDict()
    json_dict['names'] = list(data.columns.values)
    json_dict['values'] = data.values.tolist()
    data_json = json_dumps(json_dict, compact, dateformat = dateformat)
    with codecs.open(filename, "w", "utf-8") as file:
        file.write(data_json)


def create_report (report_files, destdir, dateformat = "utime"):
    """Create report, by producing a collection of JSON files

    :param report_files: Keys are the names of JSON files to produce,
                         values are the data to include in those JSON files.
    :type report_files: dict
    :param destdir: Name of the destination directory to write all JSON files
    :type destdir: str or unicode

    """

    for file in report_files:
        print "Producing file: ", join (destdir, file)
        write_file_json (join (destdir, file), report_files[file],
                         dateformat = dateformat)


es_scm_mapping_repo = """
  {"repo":
    {"properties":
      {"project_id":{"type":"long"},
       "project_name":{"type":"string",
                       "index":"not_analyzed"},
       "repo_id":{"type":"long"},
       "repo_name":{"type":"string",
                    "index":"not_analyzed"}
      }
    }
  }
"""

es_scm_mapping_commits = """
  {"commit":
    {"properties":
      {"id":{"type":"long"},
       "author_date":{"type":"date",
                      "format":"dateOptionalTime"},
       "commit_date":{"type":"date",
                      "format":"dateOptionalTime"},
       "message":{"type":"string"},
       "hash":{"type":"string",
               "index":"not_analyzed"},
       "tz":{"type":"long"},
       "author_uuid":{"type":"string"},
       "author_name":{"type":"string",
                      "index":"not_analyzed"},
       "bot":{"type":"long"},
       "org_id":{"type":"long"},
       "org_name":{"type":"string",
                   "index":"not_analyzed"},
       "repo_id":{"type":"long"},
       "repo_name":{"type":"string",
                    "index":"not_analyzed"},
       "project_id":{"type":"long"},
       "project_name":{"type":"string",
                       "index":"not_analyzed"}
      }
    }
  }
"""

es_scr_mapping = """
  {"review":
    {"properties":
      {"id":{"type":"long"},
       "review":{"type":"string",
                  "index":"not_analyzed"},
       "summary":{"type":"string"},
       "project":{"type":"string",
                  "index":"not_analyzed"},
       "submitter":{"type":"long"},
       "status":{"type":"string",
                 "index":"not_analyzed"},
       "opened":{"type":"date",
                 "format":"dateOptionalTime"},
       "closed":{"type":"date",
                 "format":"dateOptionalTime"},
       "timeopen":{"type":"long"},
       "patchsets":{"type":"long"}
      }
    }
  }
"""

def http_put (url, body):
    """Perform HTTP PUT on url.

    :param url: url
    :type url: str
    :param body: body of the HTTP request (content to upload)
    :type body: str
    :returns body of the HTTP response
    :rtype str

    """

    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(url, data=body)
    request.get_method = lambda: 'PUT'
    response = opener.open(request)
    return response.read()

def es_put_bulk (url, index, type, data, id):
    """Use HTTP PUT, via bulk API, to upload documents to Elasticsearch.

    Uploads a dataframe, assuming each row is a document, to the specified
    index and type.

    :param url: elasticsearch url
    :type url: str
    :param index: index name
    :type index: str
    :param type: type name
    :type type: str
    :param data: dataframe to upload to elasticsearch
    :type data: pandas.dataframe 
    :param id: dataframe field to use as document id
    :type id: str

    """

    index_line = '{{ "index" : {{ "_index" : "{index}", "_type" : "{type}", "_id" : "{id}" }} }}'

    batch_pos = 0
    batch_count = 1
    batch = ""
    for i, row in data.iterrows():
        batch_item = index_line.format (index = index,
                                        type = type,
                                        id = row[id]) + '\n'
        batch_item = batch_item + json_dumps(OrderedDict(row),
                                             compact = True,
                                             dateformat = "iso") + '\n'
        batch = batch + batch_item
        batch_pos = batch_pos + 1
        if batch_pos % 10000 == 0:
            print "PUT to " + url + " " + index + "/" + type \
                + " (batch no: " + str(batch_count) \
                + ", " + str(batch_pos) + " items)."
            http_put (url + "/_bulk", batch)
            batch_pos = 0
            batch_count = batch_count + 1
            batch = ""
    if batch_pos > 0:
        print "PUT to " + url + " " + index + "/" + type \
            + " (batch no: " + str(batch_count) \
            + ", " + str(batch_pos) + " items)."
        http_put (url + "/_bulk", batch)

def upload_elasticsearch_scm (url, index, data, repos):
    """Upload data (dataframe) to elasticsearch in url.

    :param url: elasticsearch url
    :type url: str
    :param index: index name
    :type index: str
    :param data: commits dataframe
    :type data: pandas.dataframe
    :param repos: repositories dataframe
    :type repos: pandas.dataframe
    
    """
    
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    # Delete index, just in case
    request = urllib2.Request(url + "/" + index)
    request.get_method = lambda: 'DELETE'
    try:
        response = opener.open(request)
        print "Elasticsearch: index deleted."
        print response.read()
    except urllib2.HTTPError as e:
        if e.code == 404:
            print "Elasticsearch: index didn't exist."
    # Create index
    response = http_put (url + "/" + index, "")
    print response
    # Create mappings
    response = http_put (url + "/" + index + "/_mapping/repo", es_scm_mapping_repo)
    print response
    response = http_put (url + "/" + index + "/_mapping/commit", es_scm_mapping_commit)
    print response
    # Upload data to indices
    es_put_bulk (url, index, 'repo', repos, 'repo_id')
    es_put_bulk (url, index, 'commit', data, 'id')

def upload_elasticsearch_scr (url, index, data):
    """Upload reviews data (dataframe) to elasticsearch in url.

    :param url: elasticsearch url
    :type url: str
    :param index: index name
    :type index: str
    :param data: reviews dataframe
    :type data: pandas.dataframe
    
    """
    
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    # Delete index, just in case
    request = urllib2.Request(url + "/" + index)
    request.get_method = lambda: 'DELETE'
    try:
        response = opener.open(request)
        print "Elasticsearch: index deleted."
        print response.read()
    except urllib2.HTTPError as e:
        if e.code == 404:
            print "Elasticsearch: index didn't exist."
    # Create index
    response = http_put (url + "/" + index, "")
    print response
    # Create mappings
    print url + "/" + index
    response = http_put (url + "/" + index + "/_mapping/review", es_scr_mapping)
    print response
    # Upload data to indices
    es_put_bulk (url, index, 'review', data, 'id')

class Database:
    """To work with a database (likely including several schemas).
    """
    
    def __init__ (self, user, passwd, host, port, maindb, shdb, prjdb):
        """Init state.

        :param user: user for accessing the MySQL database
        :type user: str or unicode
        :param passwd: password for the user accessing the MySQL database
        :type passwd: str or unicode
        :param host: hostname of the MySQL host
        :type passwd: str or unicode
        :param port: port to access MySQL
        :type port: int
        :param maindb: main database schema name (scm, src, etc.)
        :type db: str or unicode
        :param shdb: SortingHat database schema name
        :type shdb: str or unicode
        :param prjdb: projects database schema name
        :type prjdb: str or unicode
        
        """
        
        self.user = user
        self.passwd = passwd
        self.host = host
        self.port = port
        self.maindb = maindb
        self.shdb = shdb
        self.prjdb = prjdb
        self.db, self.cursor = self._connect()

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

        The query can be "templated" with {main_db} and {sh_db}.
        """

        sql = query.format(main_db = self.maindb,
                           sh_db = self.shdb,
                           prj_db = self.prjdb)
        if show:
            print sql
        results = int (self.cursor.execute(sql))
        cont = 0
        if results > 0:
            result1 = self.cursor.fetchall()
            return result1
        else:
            return []

def query_reviews (db, verbose):
    """ Execute query to select reviews.

    """

#     sql = """SELECT i.issue AS gerrit_issue,
#   i.summary AS summary,
#   t.url AS gerrit_tracker, 
#   i.submitted_by as changeset_submitter,
#   count(distinct(ch.old_value)) AS num_patchsets,
#   i.status AS current_status,
#   t2.opening_date AS opening_date, 
#   t1.closed_date AS closed_date
# FROM {main_db}.issues i
#   JOIN {main_db}.people_uidentities p ON i.submitted_by = p.people_id
#   JOIN {main_db}.trackers t ON i.tracker_id=t.id
#   JOIN {main_db}.changes ch ON ch.issue_id = i.id
#   JOIN (SELECT i.id AS issue_id,
#      ch.changed_on AS closed_date
#    FROM {main_db}.issues i
#    LEFT JOIN {main_db}.changes ch ON ch.issue_id = i.id AND field='status'
#      AND (new_value='ABANDONED' OR new_value='MERGED')
#    ) t1 ON i.id=t1.issue_id
#   JOIN (SELECT ch.issue_id,
#      ch.changed_on AS opening_date
#    FROM {main_db}.changes ch
#    WHERE ch.field='status' AND ch.new_value='UPLOADED' AND ch.old_value=1
#    ) t2 ON i.id=t2.issue_id
# GROUP BY i.issue
# """
    sql = """SELECT i.id AS id,
  i.issue AS review,
  i.summary AS summary,
  t.url AS project, 
  i.submitted_by as submitter,
  i.status AS status
FROM {main_db}.issues i
  JOIN {main_db}.trackers t ON i.tracker_id=t.id
GROUP BY i.issue
"""
    reviews = db.execute(sql, verbose)
    return reviews

def query_reviews_opened (db, verbose):

    sql = """SELECT issue_id AS id,
  MAX(changed_on) AS opened
FROM {main_db}.changes
WHERE (field='status' AND new_value='UPLOADED' AND old_value=1) OR 
  (field='Upload')
GROUP BY issue_id
"""
    # A stricter WHERE, but which misses some reviews:
    # WHERE ((field='status' AND new_value='UPLOADED') OR 
    #  (field='Upload')) AND old_value=1
    
    opened = db.execute(sql, verbose)
    return opened

def query_reviews_closed (db, verbose):

    sql = """SELECT issue_id AS id,
  MAX(changed_on) AS closed
FROM {main_db}.changes
WHERE field='status' AND (new_value='ABANDONED' OR new_value='MERGED')
GROUP BY issue_id
"""
    closed = db.execute(sql, verbose)
    return closed

def query_reviews_patchsets (db, verbose):

    sql = """SELECT issue_id AS id,
  COUNT(DISTINCT(changes.old_value)) AS patchsets
FROM {main_db}.changes
GROUP BY issue_id
"""
    patchsets = db.execute(sql, verbose)
    return patchsets

def query_review_retrieval (db, verbose):
    """ Execute query to find out the newest time for data retrieval.

    """

    sql = """SELECT MAX(retrieved_on)
FROM {main_db}.trackers
"""
    
    date = db.execute(sql, verbose)
    return date[0][0]

def query_persons_scr (db, verbose):
    """ Execute query to select persons."""

    sql = """SELECT uidentities.uuid AS uuid,
  profiles.name AS name,
  profiles.is_bot AS bot
FROM {main_db}.issues
  JOIN {main_db}.people_uidentities
    ON people_uidentities.people_id = issues.submitted_by
  JOIN {sh_db}.uidentities
    ON uidentities.uuid = people_uidentities.uuid
  LEFT JOIN {sh_db}.profiles
    ON uidentities.uuid = profiles.uuid
  GROUP BY uidentities.uuid
"""

    persons = db.execute(sql, verbose)
    return persons

def analyze_scr (db, output, elasticsearch,
                 dateformat, verbose):
    """Analyze SCR database.

    """

    print "Starting SCR analysis"
    retrieval_date = query_review_retrieval (db, verbose)
    print "Retrieval date:", retrieval_date
    print "Querying for persons."
    persons = query_persons_scr (db, verbose)
    print "Querying for reviews."
    reviews = query_reviews (db, verbose)
    reviews_df = pandas.DataFrame(list(reviews),
                                  columns = ["id", "review", "summary", "project",
                                             "submitter", "status"])
    print "reviews_df:", len(reviews_df.index)
    print "Querying for opened time for reviews."
    opened = query_reviews_opened (db, verbose)
    opened_df = pandas.DataFrame(list(opened),
                                 columns = ["id", "opened"])
    print "opened_df:", len(opened_df.index)
    print "Querying for closed time for reviews."
    closed = query_reviews_closed (db, verbose)
    closed_df = pandas.DataFrame(list(closed),
                                 columns = ["id", "closed"])
    print "closed_df:", len(closed_df.index)
    print "Querying for number of patchsets for reviews."
    patchsets = query_reviews_patchsets (db, verbose)
    patchsets_df = pandas.DataFrame(list(patchsets),
                                 columns = ["id", "patchsets"])
    print "patchsets_df:", len(patchsets_df.index)
    print "Merging into extended reviews dataframe."
    times_df = pandas.merge (opened_df, closed_df, on="id", how="left")
    print "times_df:", len(times_df.index)
    times_df["closed"].fillna(retrieval_date, inplace=True)
    times_df["timeopen"] = times_df["closed"] - times_df["opened"]
    print "times_df:", len(times_df.index)
    extended_df = pandas.merge (reviews_df, times_df, on="id", how="left")
    print "extended_df:", len(extended_df.index)
    extended_df = pandas.merge (extended_df, patchsets_df, on="id", how="left")
    print "extended_df:", len(extended_df.index)
    print extended_df[extended_df.isnull().any(axis=1)]

    if output:
        print "Producing JSON files in directory: " + output
        prefix = join (output, "scr-")
        report = OrderedDict()
        report[prefix + 'reviews.json'] = extended_df
        create_report (report_files = report, destdir = './',
                       dateformat = dateformat)

    if elasticsearch:
        (esurl, esindex) = elasticsearch
        print "Feeding data to elasticsearch at: " + esurl + "/" + esindex
        upload_elasticsearch_scr (url = esurl,
                                  index = esindex,
                                  data = extended_df)

def query_persons (db, allbranches, since, verbose):
    """ Execute query to select persons."""

    sql = """SELECT uidentities.uuid AS uuid,
  profiles.name AS name,
  profiles.is_bot AS bot
FROM {main_db}.scmlog
  JOIN {main_db}.people_uidentities
    ON people_uidentities.people_id = scmlog.author_id
  JOIN {sh_db}.uidentities
    ON uidentities.uuid = people_uidentities.uuid
  LEFT JOIN {sh_db}.profiles
    ON uidentities.uuid = profiles.uuid
  JOIN {main_db}.actions
    ON scmlog.id = actions.commit_id
"""
    where = False
    if not allbranches:
        sql = sql + """JOIN {main_db}.branches
    ON branches.id = actions.branch_id 
WHERE branches.name IN ("master")
"""
        where = True
    if since:
        if where:
            sql = sql + 'AND scmlog.author_date >= "' + since + '" '
        else:
            sql = sql + 'WHERE scmlog.author_date >= "' + since + '" '
    sql = sql + "GROUP BY uidentities.uuid"
    persons = db.execute(sql, verbose)
    return persons

def query_commits (allbranches, since, verbose):
    """Execute query to select commits."""

    sql = """SELECT scmlog.id AS id, 
  scmlog.author_date AS date,
  scmlog.date AS commit_date,
  uidentities.uuid AS person_id,
  profiles.name AS name,
  enrollments.organization_id AS org_id,
  scmlog.repository_id AS repo_id,
  LEFT(LTRIM(scmlog.message), 30) as message,
  scmlog.rev as hash,
  (((scmlog.author_date_tz DIV 3600) + 36) % 24) - 12 AS tz,
  scmlog.author_date_tz AS tz_orig,
  organizations.name AS org_name
FROM {main_db}.scmlog
  JOIN {main_db}.people_uidentities
    ON people_uidentities.people_id = scmlog.author_id
  JOIN {sh_db}.uidentities
    ON uidentities.uuid = people_uidentities.uuid
  LEFT JOIN {sh_db}.enrollments
    ON uidentities.uuid = enrollments.uuid
  LEFT JOIN {sh_db}.organizations
    ON enrollments.organization_id = organizations.id
  LEFT JOIN {sh_db}.profiles
    ON uidentities.uuid = profiles.uuid
  JOIN {main_db}.actions
    ON scmlog.id = actions.commit_id
  JOIN {main_db}.branches
    ON branches.id = actions.branch_id
"""
    if allbranches:
        sql = sql + "WHERE "
    else:
        sql = sql + 'WHERE branches.name IN ("master") AND '
    if since:
        sql = sql + 'scmlog.author_date >= "' + since + '" AND '
    sql = sql + \
"""        (enrollments.start IS NULL OR
    (scmlog.date > enrollments.start AND scmlog.date < enrollments.end))
GROUP BY scmlog.rev ORDER BY scmlog.author_date
"""
    commits = db.execute(sql, verbose)
    return commits

# Query to select organizations
query_orgs = """SELECT enrollments.organization_id AS org_id,
  {sh_db}.organizations.name AS org_name
FROM {main_db}.scmlog
  JOIN {main_db}.people_uidentities
    ON people_uidentities.people_id = scmlog.author_id
  JOIN {sh_db}.uidentities
    ON uidentities.uuid = people_uidentities.uuid
  JOIN {sh_db}.enrollments
    ON uidentities.uuid = enrollments.uuid
  JOIN {sh_db}.organizations
    ON enrollments.organization_id = organizations.id
  JOIN {main_db}.actions
    ON scmlog.id = actions.commit_id
  JOIN {main_db}.branches
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
FROM {main_db}.repositories
LEFT JOIN {prj_db}.project_repositories
  ON repositories.uri = project_repositories.repository_name
    AND repositories.type = "git"
LEFT JOIN {prj_db}.projects
  ON projects.project_id = project_repositories.project_id
GROUP BY repo_id ORDER BY repo_id"""

# Query to select repositories when there are no projects tables
query_repos_noprojects = """SELECT repositories.id AS repo_id,
  repositories.name AS repo_name,
  0 AS project_id,
  "No project" AS project
FROM {main_db}.repositories
ORDER BY repo_id"""

def analyze_scm (db, allbranches, since, output, elasticsearch,
                 dateformat, verbose):
    """Analyze SCM database.

    """
    
    if allbranches:
        print "Analyzing comits in git master branch, landed in all branches."
    else:
        print "Analyzing only commits in git master branch, landed in master branch."
    if since:
        print "Analyzing since: " + since + "."
    else:
        print "Analyzing since the first commit."

    print "Querying for persons"
    persons = query_persons (db, allbranches, since, verbose)
    print "Querying for organizations"
    orgs = db.execute(query_orgs, verbose)
    print "Querying for commits"
    commits = query_commits (allbranches, since, verbose)
    # Produce repos data, with or without projects, depending
    # on the availability of the projects table
    print "Querying for repositories"
    try:
        check = db.execute("SELECT 1 FROM {prj_db}.projects LIMIT 1", verbose)
        projects_exist = True
    except _mysql_exceptions.ProgrammingError, e:
        if e[0] == 1146:
            projects_exist = False
        else:
            raise
    if projects_exist:
        print "Projects tables found, producing projects information."
        repos = db.execute(query_repos, verbose)
    else:
        print "No projects tables, producing just one project"
        repos = db.execute(query_repos_noprojects, verbose)

    # Produce commits data
    print "Commits: ", len(commits)
    commits_df = pandas.DataFrame(list(commits), columns=["id", "date", "commit_date", "author_uuid", "name", "org_id", "repo_id", "message", "hash", "tz", "tz_orig", "org_name"])
    commits_df["org_id"].fillna(0, inplace=True)
    # None (NaN) is treated as float, making all the column float, convert to int
    commits_df["org_id"] = commits_df["org_id"].astype("int")
    commits_df["org_name"].fillna("Unknown", inplace=True)
    commits_df["author_name"] = commits_df["name"]
    commits_df["name"] = commits_df["name"] + " (" + commits_df["org_name"] + ")"

    print "Organizations: ", len(orgs)
    orgs_df = pandas.DataFrame([(0,"Unknown"),] + list(orgs),
                               columns=["org_id", "org_name"])
    
    print "Repositories: ", len(repos)
    repos_df = pandas.DataFrame(list(repos),
                                columns=["repo_id", "repo_name",
                                         "project_id", "project_name"])
    repos_df["project_id"].fillna(0, inplace=True)
    # None (NaN) is treated as float, making all the column float, convert to int
    repos_df["project_id"] = repos_df["project_id"].astype("int")
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

    if output:
        print "Producing JSON files in directory: " + output
        prefix = join (output, "scm-")
        report = OrderedDict()
        report[prefix + 'commits.json'] = commits_pkd_df
        pandas.set_option('display.max_rows', len(commits_pkd_df))
        report[prefix + 'messages.json'] = commits_messages_df
        report[prefix + 'orgs.json'] = orgs_df
        report[prefix + 'repos.json'] = repos_df
        report[prefix + 'persons.json'] = persons_df
        create_report (report_files = report, destdir = './',
                       dateformat = dateformat)

    if elasticsearch:
        (esurl, esindex) = elasticsearch
        # Produce comprehensive commits dataframe
        commits_comp_df = pandas.merge (commits_df, repos_df,
                                        left_on="repo_id", right_on="repo_id",
                                        how="left")
        commits_comp_df = pandas.merge (commits_comp_df, persons_df,
                                        left_on="author_uuid", right_on="uuid",
                                        how="left")
        commits_comp_df = \
            commits_comp_df [['id_x', 'date', 'commit_date', 'message', 'hash', 'tz',
                              'author_uuid', 'author_name', 'bot',
                              'org_id', 'org_name', 'repo_id', 'repo_name',
                              'project_id', 'project_name']]
        commits_comp_df.columns = [['id', 'author_date', 'commit_date',
                                    'message', 'hash', 'tz',
                                    'author_uuid', 'author_name', 'bot', 
                                    'org_id', 'org_name', 'repo_id', 'repo_name',
                                    'project_id', 'project_name']]
        print "Feeding data to elasticsearch at: " + esurl + "/" + esindex
        upload_elasticsearch_scm (url = esurl,
                                  index = esindex,
                                  data = commits_comp_df,
                                  repos = repos_df)

if __name__ == "__main__":

    import _mysql_exceptions
    
    args = parse_args()

    if args.prjdb:
        print "Using projects database, as specified."
        prjdb = args.prjdb
    else:
        print "No projects database specified, using SCM database instead."
        prjdb = args.scmdb
    if args.dateformat:
        dateformat = args.dateformat
    else:
        dateformat = "utime"

    if args.scmdb:
        print "SCM database specified, analyzing it."
        db = Database (user = args.user, passwd = args.passwd,
                       host = args.host, port = args.port,
                       maindb = args.scmdb, shdb = args.shdb,
                       prjdb = prjdb)
        analyze_scm(db = db,
                    allbranches = args.allbranches,
                    since = args.since,
                    output = args.output,
                    elasticsearch = args.elasticsearch,
                    dateformat = dateformat,
                    verbose = args.verbose,
                    )
    if args.scrdb:
        print "SCR database specified, analyzing it."
        db = Database (user = args.user, passwd = args.passwd,
                       host = args.host, port = args.port,
                       maindb = args.scrdb, shdb = args.shdb,
                       prjdb = prjdb)
        analyze_scr(db = db,
                    output = args.output,
                    elasticsearch = args.elasticsearch,
                    dateformat = dateformat,
                    verbose = args.verbose
                    )



