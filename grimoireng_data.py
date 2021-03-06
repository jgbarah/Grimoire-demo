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

## python grimoire-ng-data.py --user root --port 3308 --scmdb amartin_cvsanaly_openstack_sh --scrdb amartin_bicho_gerrit_openstack_sh --shdb amartin_sortinghat_openstack_sh --prjdb amartin_projects_openstack_sh --elasticsearch http://localhost:9200 openstack --verbose

## python grimoire-ng-data.py --user root --port 3308 --scmdb amartin_cvsanaly_openstack_sh --scrdb amartin_bicho_gerrit_openstack_sh --shdb amartin_sortinghat_openstack_sh --prjdb amartin_projects_openstack_sh --elasticsearch https://789ba13a7edced40de95ef091ac591d3.us-east-1.aws.found.io:9243 openstack --esauth readwrite XXX --verbose

## python grimoire-ng-data.py --user root --port 3307 --scmdb quan_cvsanaly_docker_6753 --allbranches --shdb quan_sortinghat_docker_6753 --elasticsearch https://789ba13a7edced40de95ef091ac591d3.us-east-1.aws.found.io:9243 docker-scm --esauth readwrite XXX --verbose --delete --dashboard Docker

import argparse
import MySQLdb
import _mysql_exceptions
import logging
import pandas
from collections import OrderedDict
import codecs
import json
from os.path import join
import datetime
import urllib2
import base64

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
    parser.add_argument("--scmdb", required = False, default = None,
                        help = "SCM (git) database")
    parser.add_argument("--scrdb", required = False, default = None,
                        help = "SCR (Gerrit) database")
    parser.add_argument("--shdb", required = True, default = None,
                        help = "SortingHat database")
    parser.add_argument("--prjdb", required = False, default = None,
                        help = "Projects database (if not specified, same as SCM database)")
    parser.add_argument("--output", default = "",
                        help = "Output directory")
    parser.add_argument("--dateformat", default = "utime",
                        help = "Date format ('utime' or 'iso')")
    parser.add_argument("--deleteold", action = 'store_true',
                        help = "Delete old contents in output system")
    parser.add_argument("--verbose", action = 'store_true',
                        help = "Be verbose")
    parser.add_argument("--debug", action = 'store_true',
                        help = "Show debugging information")
    parser.add_argument("--allbranches", action = 'store_true',
                        help = "Produce data with commits in all branches " + \
                        "(default, only master)")
    parser.add_argument("--since",
                        help = "Consider only commits since specified date " + \
                        "(YYYY-MM-DD format)")
    parser.add_argument("--elasticsearch", nargs=2,
                        help = "Url of elasticsearch, and index to use " + \
                        "(eg: http://localhost:9200 project)")
    parser.add_argument("--esauth", nargs=2, default = None,
                        help = "Authentication to access ElasticSearch" + \
                        "(eg: user password)")
    parser.add_argument("--batchsize",  type = int, default = 10000,
                        help = "Size of batches for uploading data" + \
                        "(default: 10,000 items)")
    parser.add_argument("--dashboard", required = False, default = "Dashboard",
                        help = "Dashboard name (default: 'Dashboard'")

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
        logging.info("Producing file: ", join (destdir, file))
        write_file_json (join (destdir, file), report_files[file],
                         dateformat = dateformat)

## Mappings for ElasticSearch
##

scm_mapping_repo = """
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

scm_mapping_commit = """
  {"commit":
    {"properties":
      {"id":{"type":"long"},
       "author_date":{"type":"date",
                      "format":"dateOptionalTime"},
       "commit_date":{"type":"date",
                      "format":"dateOptionalTime"},
       "utc_author":{"type":"date",
                     "format":"dateOptionalTime"},
       "utc_commit":{"type":"date",
                     "format":"dateOptionalTime"},
       "message":{"type":"string"},
       "hash":{"type":"string",
               "index":"not_analyzed"},
       "tz":{"type":"long"},
       "author_uuid":{"type":"string",
                      "index":"not_analyzed"},
       "author_name":{"type":"string",
                      "index":"not_analyzed"},
       "bot":{"type":"long"},
       "added":{"type":"long"},
       "deleted":{"type":"long"},
       "org_id":{"type":"long"},
       "org_name":{"type":"string",
                   "index":"not_analyzed"},
       "repo_id":{"type":"long"},
       "repo_name":{"type":"string",
                    "index":"not_analyzed"},
       "project_id":{"type":"long"},
       "project_name":{"type":"string",
                       "index":"not_analyzed"},
       "branch_name":{"type":"string",
                       "index":"not_analyzed"},
       "dashboard":{"type":"string",
                    "index":"not_analyzed"}
      }
    }
  }
"""

scr_mapping_review = """
  {"review":
    {"properties":
      {"id":{"type":"long"},
       "review":{"type":"string",
                  "index":"not_analyzed"},
       "summary":{"type":"string"},
       "submitter":{"type":"long"},
       "status":{"type":"string",
                 "index":"not_analyzed"},
       "uuid":{"type":"string",
               "index":"not_analyzed"},
       "branch":{"type":"string",
                 "index":"not_analyzed"},
       "url":{"type":"string",
              "index":"not_analyzed"},
       "githash":{"type":"string",
                  "index":"not_analyzed"},
       "project":{"type":"string",
                  "index":"not_analyzed"},
       "opened":{"type":"date",
                 "format":"dateOptionalTime"},
       "closed":{"type":"date",
                 "format":"dateOptionalTime"},
       "timeopen":{"type":"long"},
       "patchsets":{"type":"long"},
       "name":{"type":"string",
               "index":"not_analyzed"},
       "bot":{"type":"long"},
       "dashboard":{"type":"string",
                    "index":"not_analyzed"}
      }
    }
  }
"""

scr_mapping_event = """
  {"event":
    {"properties":
      {"id":{"type":"long"},
       "review":{"type":"string",
                  "index":"not_analyzed"},
       "uuid":{"type":"string",
               "index":"not_analyzed"},
       "field":{"type":"string",
                "index":"not_analyzed"},
       "patchset":{"type":"integer"},
       "tag":{"type":"string",
              "index":"not_analyzed"},
       "event_date":{"type":"date",
                     "format":"dateOptionalTime"},
       "name":{"type":"string",
               "index":"not_analyzed"},
       "bot":{"type":"long"},
       "opened":{"type":"date",
                 "format":"dateOptionalTime"},
       "closed":{"type":"date",
                 "format":"dateOptionalTime"},
       "branch":{"type":"string",
                 "index":"not_analyzed"},
       "project":{"type":"string",
                  "index":"not_analyzed"},
       "patchsets":{"type":"long"},
       "status":{"type":"string",
                 "index":"not_analyzed"},
       "timeopen":{"type":"long"},
       "dashboard":{"type":"string",
                    "index":"not_analyzed"}
      }
    }
  }
"""

def http_delete (url, auth):
    """Perform HTTP DELETE on url.

    :param url: url
    :type url: str
    :param auth: authentication data (list with user and password)
    :type auth: list of str
    :returns body of the HTTP response
    :rtype str

    """

    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(url)
    if auth:
        user = auth[0]
        passwd = auth[1]
        base64string = base64.encodestring(
            '%s:%s' % (user, passwd))[:-1]
        authheader =  "Basic %s" % base64string
        request.add_header("Authorization", authheader)
    request.get_method = lambda: 'DELETE'
    try:
        response = opener.open(request)
        result = response.read()
        logging.debug(result)
    except urllib2.HTTPError as e:
        if e.code == 404:
            logging.info("ElasticSearch: resource to DELETE didn't exist")
        else:
            logging.info("ElasticSearch: error DELETEing: " + str(e.code))
        logging.info(e.read())
        result = ""
    return result

def http_put (url, body, auth):
    """Perform HTTP PUT on url.

    :param url: url
    :type url: str
    :param body: body of the HTTP request (content to upload)
    :type body: str
    :param auth: authentication data (list with user and password)
    :type auth: list of str
    :returns body of the HTTP response
    :rtype str

    """

    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(url, data=body)
    if auth:
        user = auth[0]
        passwd = auth[1]
        base64string = base64.encodestring(
            '%s:%s' % (user, passwd))[:-1]
        authheader =  "Basic %s" % base64string
        request.add_header("Authorization", authheader)
    request.get_method = lambda: 'PUT'
    try:
        response = opener.open(request)
        result = response.read()
        logging.debug(result)
        return result
    except urllib2.HTTPError as e:
        logging.info("ElasticSearch: error PUTing: " + str(e.code))
        logging.info(e.read())
        raise


def es_put_bulk (url, index, type, data, id, mapping = None, auth = None,
                 batchsize = 10000):
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
    :param mapping: mapping JSON for index
    :type mapping: str
    :param auth: authentication data (list with user and password)
    :type auth: list of str
    :param batchsize: size of batches to upload (in number of items)
    :type batchsize: int

    """

    if mapping:
        logging.debug("Creating mappings for index/type " + index + "/" + type)
        response = http_put (url + "/" + index + "/_mapping/" + type,
                             mapping, auth = auth)
        logging.debug(response)
    index_line = '{{ "index" : {{ "_index" : "{index}", "_type" : "{type}", "_id" : "{id}" }} }}'
    # Upload data using the bulk API
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
        if batch_pos % batchsize == 0:
            logging.info("PUT to " + url + " " + index + "/" + type \
                + " (batch no: " + str(batch_count) \
                         + ", " + str(batch_pos) + " items, " \
                         + str(len(batch)) + " bytes).")
            http_put (url + "/_bulk", batch, auth = auth)
            batch_pos = 0
            batch_count = batch_count + 1
            batch = ""
    if batch_pos > 0:
        logging.info("PUT to " + url + " " + index + "/" + type \
            + " (batch no: " + str(batch_count) \
            + ", " + str(batch_pos) + " items).")
        http_put (url + "/_bulk", batch, auth = auth)


def upload_elasticsearch (url, index, data, deleteold, auth, batchsize):
    """Upload reviews data (dataframe) to elasticsearch in url.

    The data to upload is a dictionary, with ElasticSearch types as keys,
    and dataframes to upload for each of those types as vaules. For example:
      {'reviews': reviews_df, 'commits': commits_df}

    :param url: elasticsearch url
    :type url: str
    :param index: index name
    :type index: str
    :param data: dictionary with dataframes to upload
    :type data: dictionary (keys: type, values: pandas.dataframe)
    :param deleteold: whether old content (index) should be deleted
    :type dedleteold: bool
    :param auth: authentication data (list with user and password)
    :type auth: list of str
    :param batchsize: size of batches to upload (in number of items)
    :type batchsize: int

    """

    if deleteold:
        logging.info("ElasticSearch: deleting index.")
        response = http_delete (url = url + "/" + index, auth = auth)
        logging.debug(response)
    # Create index
    logging.info("ElasticSearch: creating index " + index)
    try:
        response = http_put (url + "/" + index, "", auth = auth)
        logging.debug("Elasticsearch index creation, response: " + response)
    except urllib2.HTTPError as e:
        logging.info("ElasticSearch: error creating index: " + str(e.code))
    # Upload data to indices
    for type, to_upload in data.iteritems():
        es_put_bulk (url = url, index = index, type = type,
                     data = to_upload['df'], id = to_upload['id'],
                     mapping = to_upload['mapping'],
                     auth = auth, batchsize = batchsize)

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

    def execute(self, query):
        """Execute an SQL query with the corresponding database.

        The query can be "templated" with {main_db} and {sh_db}.
        """

        sql = query.format(main_db = self.maindb,
                           sh_db = self.shdb,
                           prj_db = self.prjdb)
        logging.debug(sql)
        result_length = int (self.cursor.execute(sql))
        fields = [i[0] for i in self.cursor.description]
        if result_length > 0:
            results = self.cursor.fetchall()
        else:
            results = []
        return (results, fields)

    def execute_df(self, query, name = "", other = []):
        """Execute an SQL query with the corresponding database, return dataframe.

        The query can be "templated" with {main_db} and {sh_db}.

        The "other" parameter is a list with rows to be added to the dataframe,
        in addition to those obtained from the database. Each element in
        that list is a tuple with the fields (in the same order than they
        are received from the database) in the row.

        :param query: SQL query to execute
        :type query: str
        :param name: name of the results (for humans)
        :type name: str
        :param other: list of other rows that should be inclued in the dataframe
        :type other: list of rows

        """

        logging.debug(name + " querying...")
        (results, fields) = self.execute(query)
        results_df = pandas.DataFrame(other + list(results),
                                      columns = fields)
        logging.info(name + ": " + str(len(results_df.index)))
        return results_df


sql_reviews = """SELECT i.id AS id,
  i.issue AS review,
  i.summary AS summary,
  i.submitted_by as submitter,
  i.status AS status,
  pu.uuid AS uuid
FROM {main_db}.issues i
  JOIN {main_db}.trackers t ON i.tracker_id = t.id
  JOIN {main_db}.people_uidentities pu ON i.submitted_by = pu.people_id
GROUP BY i.issue
"""

sql_reviews_extra = """SELECT branch,
  url,
  change_id AS githash,
  project,
  issue_id AS id
FROM {main_db}.issues_ext_gerrit
"""

sql_reviews_opened = """SELECT issue_id AS id,
  MAX(changed_on) AS opened
FROM {main_db}.changes
WHERE (field='status' AND new_value='UPLOADED' AND old_value=1) OR
  (field='Upload')
GROUP BY issue_id
"""

sql_reviews_closed = """SELECT issue_id AS id,
  MAX(changed_on) AS closed
FROM {main_db}.changes
WHERE field='status' AND (new_value='ABANDONED' OR new_value='MERGED')
GROUP BY issue_id
"""

sql_reviews_patchsets = """SELECT issue_id AS id,
  COUNT(DISTINCT(changes.old_value)) AS patchsets
FROM {main_db}.changes
WHERE changes.old_value != ""
GROUP BY issue_id
"""

sql_reviews_persons = """SELECT uidentities.uuid AS uuid,
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

sql_reviews_events = """SELECT c.id AS id,
  i.issue AS review,
  pup.uuid AS uuid,
  c.field AS field,
  c.old_value AS patchset,
  c.new_value AS tag,
  c.changed_on AS event_date
FROM {main_db}.changes c
  JOIN {main_db}.issues i ON i.id = c.issue_id
  JOIN {main_db}.people_uidentities pup ON c.changed_by = pup.people_id
WHERE field <> 'Upload'
ORDER BY review, patchset, event_date
"""

sql_reviews_events_persons = """SELECT uidentities.uuid AS uuid,
  profiles.name AS name,
  profiles.is_bot AS bot
FROM {main_db}.changes
  JOIN {main_db}.people_uidentities
    ON people_uidentities.people_id = changes.changed_by
  JOIN {sh_db}.uidentities
    ON uidentities.uuid = people_uidentities.uuid
  LEFT JOIN {sh_db}.profiles
    ON uidentities.uuid = profiles.uuid
  GROUP BY uidentities.uuid
"""

def query_review_retrieval (db):
    """ Execute query to find out the newest time for data retrieval.

    """

    sql = """SELECT MAX(retrieved_on) as date
FROM {main_db}.trackers
"""

    (date, fields) = db.execute(sql)
    return date[0][0]


def analyze_scr (db, output, elasticsearch, dateformat, dashboard):
    """Analyze SCR database.

    """

    logging.debug("Starting SCR analysis")
    retrieval_date = query_review_retrieval(db)
    logging.info("Retrieval date: " + retrieval_date.isoformat())

    reviews_df = db.execute_df(sql_reviews, "Reviews")
    extra_df = db.execute_df(sql_reviews_extra, "Reviews (extra)")
    opened_df = db.execute_df(sql_reviews_opened, "Reviews (opened)")
    closed_df = db.execute_df(sql_reviews_closed, "Reviews (closed)")
    patchsets_df = db.execute_df(sql_reviews_patchsets, "Reviews (no. patchsets)")
    persons_df = db.execute_df(sql_reviews_persons, "Persons (submitting reviews)")
    events_df = db.execute_df(sql_reviews_events, "Events involving reviews")
    persons_events_df = db.execute_df(sql_reviews_events_persons,
                              "Persons (producing events)")

    logging.debug("Merging into extended reviews dataframe.")
    times_df = pandas.merge (opened_df, closed_df, on="id", how="left")
    times_df["closed"].fillna(retrieval_date, inplace=True)
    times_df["timeopen"] = (times_df["closed"] - times_df["opened"]) / (3600 * 24)
#    times_df["timeopen"] = times_df["closed"] - times_df["opened"]
#    times_df["timeopen"].apply(lambda x: x.item() / (3600 * 24.0 * 1e9))
    logging.info("Reviews with timing: " + str(len(times_df.index)))

    extended_df = pandas.merge (reviews_df, extra_df, on="id", how="left")
    extended_df = pandas.merge (extended_df, times_df, on="id", how="left")
    extended_df = pandas.merge (extended_df, patchsets_df, on="id", how="left")
    extended_df = pandas.merge (extended_df, persons_df, on="uuid", how="left")
    extended_df["dashboard"] = [dashboard] * len(extended_df.index)

    logging.info("Reviews with extended info: " + str(len(extended_df.index)))
    logging.debug("extended_df with NaN: " \
                  + str(extended_df[extended_df.isnull().any(axis=1)]))

    events_extended_df = pandas.merge (events_df, persons_events_df,
                                       on="uuid", how="left")
    events_extended_df = pandas.merge (events_extended_df,
                                       extended_df[["review","opened","closed",
                                                   "branch","project",
                                                   "patchsets","status",
                                                   "timeopen"]],
                                       on="review", how="left")
    events_extended_df["dashboard"] = [dashboard] * len(events_extended_df.index)
    logging.info("Events with extended info: " \
                 + str(len(events_extended_df.index)))
    logging.info("events_extended_df with NaN (will be dropped): " \
                  + str(events_extended_df[events_extended_df.isnull().any(axis=1)]))
    events_extended_df = events_extended_df.dropna()
    es_data = {}
    if output:
        logging.info("Producing JSON files in directory: " + output)
        prefix = join (output, "scr-")
        report = OrderedDict()
        report[prefix + 'reviews.json'] = extended_df
        create_report (report_files = report, destdir = './',
                       dateformat = dateformat)

    if elasticsearch:
        es_data['review'] = {'df': extended_df, 'id': 'id',
                             'mapping': scr_mapping_review}
        es_data['event'] = {'df': events_extended_df, 'id': 'id',
                             'mapping': scr_mapping_event}
    return es_data

def sql_commits_persons (allbranches, since):
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
    return sql

def sql_commits (allbranches, since):
    """Produce SQL query to select commits."""

    sql = """SELECT scmlog.id AS id,
  scmlog.author_date AS date,
  scmlog.date AS commit_date,
  uidentities.uuid AS author_uuid,
  profiles.name AS name,
  enrollments.organization_id AS org_id,
  scmlog.repository_id AS repo_id,
  LEFT(LTRIM(scmlog.message), 30) as message,
  scmlog.rev as hash,
  (((scmlog.author_date_tz DIV 3600) + 36) % 24) - 12 AS tz,
  scmlog.author_date_tz AS tz_orig,
  DATE_SUB(scmlog.author_date, INTERVAL scmlog.author_date_tz SECOND)
    AS utc_author,
  DATE_SUB(scmlog.date, INTERVAL scmlog.date_tz SECOND) AS utc_commit,
  organizations.name AS org_name,
  branches.name AS branch_name
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
    return sql

def sql_lines (allbranches, since):
    """Produce SQL query to select lines per commit.

    """

    sql = """SELECT commits_lines.commit_id AS id,
  commits_lines.added AS added,
  commits_lines.removed AS removed
FROM {main_db}.commits_lines
  JOIN {main_db}.scmlog
    ON commits_lines.commit_id = scmlog.id
  JOIN {main_db}.actions
    ON scmlog.id = actions.commit_id
  JOIN {main_db}.branches
    ON branches.id = actions.branch_id
"""
    if (not allbranches) or since:
        sql = sql + "WHERE "
    if not allbranches:
        sql = sql + 'WHERE branches.name IN ("master") '
    if (not allbranches) and since:
        sql = sql + "AND "
    if since:
        sql = sql + 'scmlog.author_date >= "' + since + '" '
    sql = sql + "GROUP BY scmlog.rev ORDER BY scmlog.author_date"
    return sql

# Query to select organizations
sql_commits_orgs = """SELECT enrollments.organization_id AS org_id,
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
sql_commits_repos = """SELECT repositories.id AS repo_id,
  repositories.name AS repo_name,
  projects.project_id AS project_id,
  projects.id AS project_name
FROM {main_db}.repositories
LEFT JOIN {prj_db}.project_repositories
  ON repositories.uri = project_repositories.repository_name
    AND repositories.type = "git"
LEFT JOIN {prj_db}.projects
  ON projects.project_id = project_repositories.project_id
GROUP BY repo_id ORDER BY repo_id"""

# Query to select repositories when there are no projects tables
sql_commits_repos_noproj = """SELECT repositories.id AS repo_id,
  repositories.name AS repo_name,
  0 AS project_id,
  "Top level" AS project_name
FROM {main_db}.repositories
ORDER BY repo_id"""

def analyze_scm (db, allbranches, since, output, elasticsearch,
                 dateformat, dashboard):
    """Analyze SCM database.

    """

    if allbranches:
        logging.info("Analyzing comits in git master branch, " \
                     + "landed in all branches.")
    else:
        logging.info("Analyzing only commits in git master branch, " \
                     + "landed in master branch.")
    if since:
        logging.info("Analyzing since: " + since + ".")
    else:
        logging.info("Analyzing since the first commit.")

    # Lines per commit
    lines_df = db.execute_df(sql_lines(allbranches, since),
                               "Lines per commit")
    # Commits
    commits_df = db.execute_df(sql_commits(allbranches, since),
                               "Commits")
    commits_df["org_id"].fillna(0, inplace=True)
    # None (NaN) is treated as float, making all the column float, convert to int
    commits_df["org_id"] = commits_df["org_id"].astype("int")
    commits_df["org_name"].fillna("Unknown", inplace=True)
    commits_df["author_name"] = commits_df["name"]
    commits_df["name"] = commits_df["name"] + " (" + commits_df["org_name"] + ")"
    commits_df["dashboard"] = [dashboard] * len(commits_df.index)
    # Persons
    persons_df = db.execute_df(sql_commits_persons(allbranches, since),
                               "Persons (authoring commits)")
    persons_df["id"] = persons_df.index
    # Organizations
    orgs_df = db.execute_df(query = sql_commits_orgs,
                            name = "Organizations (authoring commits)",
                            other = [(0,"Unknown"),])
    # Produce repos data, with or without projects, depending
    # on the availability of the projects table
    try:
        check = db.execute("SELECT 1 FROM {prj_db}.projects LIMIT 1")
        logging.info("Projects tables found, producing projects information.")
        sql_repos = sql_commits_repos
    except _mysql_exceptions.ProgrammingError, e:
        if e[0] == 1146:
            logging.info("No projects tables, producing just one project")
            sql_repos = sql_commits_repos_noproj
        else:
            raise
    repos_df = db.execute_df(sql_repos, "Projects (commits)")
    repos_df["project_id"].fillna(0, inplace=True)
    # None (NaN) is treated as float, making all the column float, convert to int
    repos_df["project_id"] = repos_df["project_id"].astype("int")
    repos_df["project_name"].fillna("Unclassified", inplace=True)
    # Capitalizing could be a good idea, but not by default.
    # repos_df["repo_name"] = repos_df["repo_name"].str.capitalize()
    # repos_df["project_name"] = repos_df["project_name"].str.capitalize()

    # Produce packed (minimal) commits dataframe
    commits_pkd_df = pandas.merge (commits_df, persons_df,
                                   left_on="author_uuid", right_on="uuid",
                                   how="left")
    commits_pkd_df = commits_pkd_df[["id_x", "date", "id_y", "org_id",
                                     "repo_id", "tz"]]
    commits_pkd_df.columns = ["id", "date", "author", "org", "repo", "tz"]
    # Produce messages and hashes dataframe for commits
    commits_messages_df = commits_df[["id", "message", "hash"]]

    es_data = {}
    if output:
        logging.info("Producing JSON files in directory: " + output)
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
        commits_comp_df = pandas.merge (commits_df, lines_df,
                                        left_on="id", right_on="id",
                                        how="left")
        commits_comp_df = pandas.merge (commits_comp_df, repos_df,
                                        left_on="repo_id", right_on="repo_id",
                                        how="left")
        commits_comp_df = pandas.merge (commits_comp_df, persons_df,
                                        left_on="author_uuid", right_on="uuid",
                                        how="left")
        commits_comp_df = \
            commits_comp_df [['id_x', 'date', 'commit_date',
                              'utc_author', 'utc_commit', 'message', 'hash', 'tz',
                              'author_uuid', 'author_name', 'bot',
                              'added', 'removed',
                              'org_id', 'org_name', 'repo_id', 'repo_name',
                              'project_id', 'project_name', 'branch_name', 'dashboard']]
        commits_comp_df.columns = [['id', 'author_date', 'commit_date',
                                    'utc_author', 'utc_commit',
                                    'message', 'hash', 'tz',
                                    'author_uuid', 'author_name', 'bot',
                                    'added', 'removed',
                                    'org_id', 'org_name', 'repo_id', 'repo_name',
                                    'project_id', 'project_name', 'branch_name', 'dashboard']]
        es_data['repo'] = {'df': repos_df, 'id': 'repo_id',
                           'mapping': scm_mapping_repo}
        es_data['commit'] = {'df': commits_comp_df, 'id': 'id',
                             'mapping': scm_mapping_commit}
    return es_data

def process_all (user, passwd = "", host = "127.0.0.1", port = 3306,
                 scmdb = None, scrdb = None, shdb = None, prjdb = None,
                 allbranches = False, since = None,
                 output = "", elasticsearch = None, esauth = None,
                 batchsize = 10000,
                 dateformat = "utime",
                 dashboard = "Dashboard",
                 deleteold = False, verbose = False, debug = False):
    """Process all databases found, and produce results in files or ElasticSearch.

    """

    if debug:
        logging.basicConfig(level=logging.DEBUG)
    elif verbose:
        logging.basicConfig(level=logging.INFO)
    if prjdb:
        logging.info("Using projects database, as specified.")
    else:
        logging.info("No projects database specified, using SCM database instead.")
        prjdb = scmdb

    # Data to upload to ElasticSearch
    es_data = {}
    if scmdb:
        logging.info("SCM database specified, analyzing it.")
        db = Database (user = user, passwd = passwd,
                       host = host, port = port,
                       maindb = scmdb, shdb = shdb,
                       prjdb = prjdb)
        es_scm = analyze_scm(db = db,
                             allbranches = allbranches,
                             since = since,
                             output = output,
                             elasticsearch = elasticsearch,
                             dateformat = dateformat,
                             dashboard = dashboard)
        for index, to_upload in es_scm.iteritems():
            es_data[index] = to_upload
    if scrdb:
        logging.info("SCR database specified, analyzing it.")
        db = Database (user = user, passwd = passwd,
                       host = host, port = port,
                       maindb = scrdb, shdb = shdb,
                       prjdb = prjdb)
        es_scr = analyze_scr(db = db,
                             output = output,
                             elasticsearch = elasticsearch,
                             dateformat = dateformat,
                             dashboard = dashboard)
        for index, to_upload in es_scr.iteritems():
            es_data[index] = to_upload
    if elasticsearch:
        (esurl, esindex) = elasticsearch
        logging.info("Feeding data to elasticsearch at: " + esurl + "/" + esindex)
        upload_elasticsearch (url = esurl,
                              index = esindex,
                              data = es_data,
                              deleteold = deleteold,
                              auth = esauth,
                              batchsize = batchsize)

if __name__ == "__main__":

    args = parse_args()


    process_all (
        user = args.user, passwd = args.passwd,
        host = args.host, port = args.port,
        scmdb = args.scmdb,
        scrdb = args.scrdb,
        shdb = args.shdb,
        prjdb = args.prjdb,
        allbranches = args.allbranches,
        since = args.since,
        output = args.output,
        elasticsearch = args.elasticsearch,
        esauth = args.esauth,
        batchsize = args.batchsize,
        dateformat = args.dateformat,
        dashboard = args.dashboard,
        deleteold = args.deleteold,
        verbose = args.verbose,
        debug = args.debug
    )
