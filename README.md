# Grimoire-demo
Some demos and auxiliary scripts for Grimoire databases and dashboards

## Running grimoireng_cloudprojects.py

This script produces ElasticSearch indexes for SCM (git) and SCR (Bicho/Gerrit) databases, using SortingHat (merged identities, affiliation) database.

First of all, ensure you have Python 2.7.x, and a recent Pandas and SciPy libraries.

Most of the configuration should be stored in a file, the configuration file. The file grimoireng_config.py is an example of its structure, and the variables that it should define. The syntax of this file is Python syntax: the file will be included as a module. The main variables defined in this file are:

* elasticsearch: a list with two values (url and index of ElasticSeatrch)
* dashboards: dictionary defining the databases to analyze

python grimoireng_cloudprojects.py --user root --esauth readwrite XXX --verbose --delete --dashboards project --config config.py

readwrite: user in the ElasticSearch site
XXX: passwd in the ElasticSearch site
config.py: configuration file
project: name of the entry in the dashboards dictionary in the config file