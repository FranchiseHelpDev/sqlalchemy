# mysql/gaerdbms.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""
.. dialect:: mysql+gaerdbms
    :name: Google Cloud SQL
    :dbapi: rdbms
    :connectstring: mysql+gaerdbms:///<dbname>?instance=<instancename>
    :url: https://developers.google.com/appengine/docs/python/cloud-sql/developers-guide

    This dialect is based primarily on the :mod:`.mysql.mysqldb` dialect with minimal
    changes.

    .. versionadded:: 0.7.8


Pooling
-------

Google App Engine connections appear to be randomly recycled,
so the dialect does not pool connections.  The :class:`.NullPool`
implementation is installed within the :class:`.Engine` by
default.

"""

import os

from .mysqldb import MySQLDialect_mysqldb
from ...pool import NullPool
import re

# hijacking converters
import datetime
import types
from google.storage.speckle.proto import jdbc_type
from google.storage.speckle.python.api import converters

def Str2Unicode(arg):
    try:
        return unicode(arg, 'utf-8')
    except UnicodeDecodeError:
        try:
            return unicode(arg, 'latin-1')
        except UnicodeDecodeError:
            return "[ERROR]"

CONVERTERS = {

    types.IntType: converters.Any2Str,
    types.LongType: converters.Any2Str,
    types.FloatType: converters.Any2Str,
    types.TupleType: converters.Tuple2Str,
    types.BooleanType: converters.Bool2Str,
    types.StringType: converters.Any2Str,
    types.UnicodeType: converters.Unicode2Str,
    datetime.date: converters.Date2Str,
    datetime.datetime: converters.Datetime2Str,
    datetime.time: converters.Time2Str,
    converters.Blob: converters.Any2Str,


    jdbc_type.BIT: int,
    jdbc_type.SMALLINT: int,
    jdbc_type.INTEGER: int,
    jdbc_type.BIGINT: int,
    jdbc_type.TINYINT: int,
    jdbc_type.REAL: float,
    jdbc_type.DOUBLE: float,
    jdbc_type.NUMERIC: float,
    jdbc_type.DECIMAL: float,
    jdbc_type.FLOAT: float,
    jdbc_type.CHAR: Str2Unicode,
    jdbc_type.VARCHAR: Str2Unicode,
    jdbc_type.LONGVARCHAR: Str2Unicode,
    jdbc_type.DATE: converters.Str2Date,
    jdbc_type.TIME: converters.Str2Time,
    jdbc_type.TIMESTAMP: converters.Str2Datetime,
    jdbc_type.BINARY: converters.Blob,
    jdbc_type.VARBINARY: converters.Blob,
    jdbc_type.LONGVARBINARY: converters.Blob,
    jdbc_type.BLOB: converters.Blob,
    jdbc_type.CLOB: Str2Unicode,
    jdbc_type.NCLOB: Str2Unicode,
    jdbc_type.NCHAR: Str2Unicode,
    jdbc_type.NVARCHAR: Str2Unicode,
    jdbc_type.LONGNVARCHAR: Str2Unicode,

    jdbc_type.ARRAY: Str2Unicode,
    jdbc_type.NULL: Str2Unicode,
    jdbc_type.OTHER: Str2Unicode,
    jdbc_type.JAVA_OBJECT: Str2Unicode,
    jdbc_type.DISTINCT: Str2Unicode,
    jdbc_type.STRUCT: Str2Unicode,
    jdbc_type.REF: Str2Unicode,
    jdbc_type.DATALINK: Str2Unicode,
    jdbc_type.BOOLEAN: Str2Unicode,
    jdbc_type.ROWID: Str2Unicode,
    jdbc_type.SQLXML: Str2Unicode,
}

def _is_dev_environment():
    return os.environ.get('SERVER_SOFTWARE', '').startswith('Development/')

class MySQLDialect_gaerdbms(MySQLDialect_mysqldb):

    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False

    @classmethod
    def dbapi(cls):
        # from django:
        # http://code.google.com/p/googleappengine/source/
        #     browse/trunk/python/google/storage/speckle/
        #     python/django/backend/base.py#118
        # see also [ticket:2649]
        # see also http://stackoverflow.com/q/14224679/34549
        from google.appengine.api import apiproxy_stub_map

        if _is_dev_environment():
            from google.appengine.api import rdbms_mysqldb
            return rdbms_mysqldb
        elif apiproxy_stub_map.apiproxy.GetStub('rdbms'):
            from google.storage.speckle.python.api import rdbms_apiproxy
            return rdbms_apiproxy
        else:
            from google.storage.speckle.python.api import rdbms_googleapi
            return rdbms_googleapi

    @classmethod
    def get_pool_class(cls, url):
        # Cloud SQL connections die at any moment
        return NullPool

    def create_connect_args(self, url):
        opts = url.translate_connect_args()
        if not _is_dev_environment():
            # 'dsn' and 'instance' are because we are skipping
            # the traditional google.api.rdbms wrapper
            opts['dsn'] = ''
            opts['instance'] = url.query['instance']
            opts['conv'] = CONVERTERS
        return [], opts

    def _extract_error_code(self, exception):
        match = re.compile(r"^(\d+)L?:|^\((\d+)L?,").match(str(exception))
        # The rdbms api will wrap then re-raise some types of errors
        # making this regex return no matches.
        code = match.group(1) or match.group(2) if match else None
        if code:
            return int(code)

dialect = MySQLDialect_gaerdbms
