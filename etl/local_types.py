""" All types"""
from collections import namedtuple

AWS = namedtuple('AWS', 'bucket access_key secret_key')
Connection = namedtuple('Connection', 'host user password')
Databases = namedtuple('Databases', 'warehouse dojos events users')
