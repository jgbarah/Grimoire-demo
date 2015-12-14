
# -*- coding: utf-8 -*-
# Configuration file for grimoireng_cloud.py

from collections import OrderedDict

# elasticsearch = None 
elasticsearch = [
    "https://789ba13a7edced40de95ef091ac591d3.us-east-1.aws.found.io:9243",
    "scm"
]

PortSpectrum = 3307
PortAtari = 3308
dashboards = OrderedDict ([
    ("ElasticSearch", {"scmdb": "quan_cvsanaly_elastic_6863",
                       "scrdb": None,
                       "shdb": "quan_sortinghat_elastic_6863",
                       "prjdb": None,
                       "port": PortSpectrum}),
    ("Kubernetes", {"scmdb": "lcanas_cvsanaly_kubernetes_oscon2015",
                    "scrdb": None,
                    "shdb": "lcanas_sortinghat_kubernetes_oscon2015",
                    "prjdb": None,
                    "port": PortSpectrum}),
    ("Docker", {"scmdb": "quan_cvsanaly_docker_6753",
                "scrdb": None,
                "shdb": "quan_sortinghat_docker_6753",
                "prjdb": None,
                "port": PortSpectrum}),
    ("Puppet", {"scmdb": "puppet_2015q3_cvsanaly",
                "scrdb": None,
                "shdb": "puppet_2015q3_sortinghat",
                "prjdb": None,
                "port": PortSpectrum}),
    ("Midonet", {"scmdb": "cp_cvsanaly_midokura",
                 "scrdb": "cp_gerrit_midokura",
                 "shdb": "cp_sortinghat_midokura",
                 "prjdb": None,
                 "port": PortSpectrum}),
    ("OpenStack", {"scmdb": "amartin_cvsanaly_openstack_sh",
                   "scrdb": "amartin_bicho_gerrit_openstack_sh",
                   "shdb": "amartin_sortinghat_openstack_sh",
                   "prjdb": "amartin_projects_openstack_sh",
                   "port": PortAtari}),
    ("MediaWiki", {"scmdb": "acs_cvsanaly_mediawiki_5300",
                   "scrdb": "dic_gerrit_mediawiki_5829",
                   "shdb": "acs_sortinghat_mediawiki_5879",
                   "prjdb": None,
                   "port": PortAtari}),
    ("CloudStack", {"scmdb": "sduenas_cvsanaly_cloudstack_3246",
                    "scrdb": None,
                    "shdb": "amartin_sortinghat_cloudstack",
                    "prjdb": None,
                    "port": PortAtari}),
    ("OPNFV", {"scmdb": "dpose_cvsanaly_linux_foundation_6219_and_6222",
               "scrdb": "dpose_gerrit_linux_foundation_6219_and_6222",
               "shdb": "dpose_sortinghat_linux_foundation_6219_and_6222",
               "prjdb": None,
               "port": PortSpectrum}),
    ("Eclipse", {"scmdb": "cp_cvsanaly_Eclipse_5986",
                 "scrdb": "cp_gerrit_Eclipse_5467",
                 "shdb": "cp_sortinghat_Eclipse_5680",
                 "prjdb": "cp_projects_Eclipse_5680",
                 "port": PortSpectrum}),
    ("ownCloud", {"scmdb": "cp_cvsanaly_ownCloud2",
                  "scrdb": None,
                  "shdb": "cp_sortinghat_ownCloud",
                  "prjdb": None,
                  "port": PortSpectrum}),
    ("FirefoxOS", {"scmdb": "jalonso_cvsanaly_mozillab2g",
                   "scrdb": None,
                   "shdb": "jalonso_sortinghat_mozillab2g",
                   "prjdb": None,
                   "port": PortSpectrum})
])
