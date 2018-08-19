""" All types"""
from collections import namedtuple

Connection = namedtuple("Connection", "host user password")
Databases = namedtuple("Databases", "warehouse dojos events users")
