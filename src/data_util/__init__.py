"""
Copyright (C) 2023 Henock Abebe. All rights reserved. 
"""

""" Package implementing qualtative analysis """

from . import config

VERSION = {
    'major': 0,
    'minor': 0,
    'micro': 1}

def get_version_string():
    version = '{major}.{minor}.{micro}'.format(**VERSION)
    return version

def get_config():
    return config.Configuration.instance()

__version__ = get_version_string()


