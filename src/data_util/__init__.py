"""
Copyright (C) 2023 Henock Abebe. All rights reserved. 
"""

""" Package implementing qualtative analysis """

from . import config
from . import database
from . import dataset

def db():
    return database.db.instance()

def db_dump():
    return db().select()

VERSION = {
    'major': 0,
    'minor': 0,
    'micro': 1}

def get_version_string():
    version = '{major}.{minor}.{micro}'.format(**VERSION)
    return version

def get_config(reload=False):
    return config.Configuration.instance(reload)

__version__ = get_version_string()


