"""
Copyright (C) 2023 Henock Abebe. All rights reserved. 
"""

""" Package implementing qualtative analysis """

from . import config
from . import db
from . import ds

VERSION = {
    'major': 0,
    'minor': 0,
    'micro': 1}

def get_version_string():
    version = '{major}.{minor}.{micro}'.format(**VERSION)
    return version

def get_config(reload=False):
    return config.Configuration.instance(reload)

def get_db(reload=False):
    return db.Database.instance(reload)

def db_dump():
    return db().select()


__version__ = get_version_string()


