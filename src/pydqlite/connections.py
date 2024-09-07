
from __future__ import unicode_literals


import ctypes
import pkg_resources



import codecs
import logging

try:
    from http.client import HTTPConnection
except ImportError:
    # pylint: disable=import-error
    from httplib import HTTPConnection

try:
    from urllib.parse import urlparse
except ImportError:
    # pylint: disable=import-error
    from urlparse import urlparse

from .constants import (
    UNLIMITED_REDIRECTS,
)

from .cursors import Cursor
from ._ephemeral import EphemeralDqlited as _EphemeralDqlited
from .extensions import PARSE_DECLTYPES, PARSE_COLNAMES


class Connection(object):

    from .exceptions import (
        Warning,
        Error,
        InterfaceError,
        DatabaseError,
        DataError,
        OperationalError,
        IntegrityError,
        InternalError,
        ProgrammingError,
        NotSupportedError,
    )

    def __init__(self, host='localhost', port=9001, database="hci_db",
                 user=None, password=None, connect_timeout=None,
                 detect_types=0, max_redirects=UNLIMITED_REDIRECTS):
        self.messages = []
        self.host = host
        self.port = port
        self.database = database
        self._headers = {}
        if not (user is None or password is None):
            self._headers['Authorization'] = 'Basic ' + \
                codecs.encode('{}:{}'.format(user, password).encode('utf-8'),
                              'base64').decode('utf-8').rstrip('\n')
        self.connect_timeout = connect_timeout
        self.max_redirects = max_redirects
        self.detect_types = detect_types
        self.parse_decltypes = detect_types & PARSE_DECLTYPES
        self.parse_colnames = detect_types & PARSE_COLNAMES
        self._ephemeral = None
        if host == ':memory:':
            self._ephemeral = _EphemeralDqlited().__enter__()
            self.host, self.port = self._ephemeral.http
            
        # 加载 Go 共享库
        #libdqlite = ctypes.CDLL('./libdqlite.so')
        # 获取 libdqlite.so 的路径
        lib_path = pkg_resources.resource_filename('pydqlite', 'libdqlite.so')

        # 加载 Go 共享库
        self.libdqlite = ctypes.CDLL(lib_path)

        # 定义函数原型
        self.libdqlite.dqlite_connect.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        self.libdqlite.dqlite_connect.restype = ctypes.c_int

        self.libdqlite.dqlite_query.argtypes = [ctypes.c_char_p]
        self.libdqlite.dqlite_query.restype = ctypes.c_char_p

        self.libdqlite.dqlite_disconnect.argtypes = []
        self.libdqlite.dqlite_disconnect.restype = None    
        self._connection = self._init_connection()
        self._current_cursor = None

    def _init_connection(self):
        # return HTTPConnection(self.host, port=self.port,
        #                       timeout=None if self.connect_timeout is None else float(self.connect_timeout))
        
     
        node_address = f"{self.host}:{self.port}".encode()
        database_name = f"{self.database}".encode()
        connection = self.libdqlite.dqlite_connect(node_address, database_name)
        return connection

    def _retry_request(self, method, uri, body=None, headers={}):
        tries = 10
        while tries:
            tries -= 1
            try:
                self._connection.request(method, uri, body=body,
                                         headers=dict(self._headers, **headers))
                return self._connection.getresponse()
            except Exception:
                if not tries:
                    raise
                self._connection.close()
                self._connection = self._init_connection()

    # def _fetch_response(self, method, uri, body=None, headers={}):
    #     """
    #     Fetch a response, handling redirection.
    #     """
    #     response = self._retry_request(method, uri, body=body, headers=headers)
    #     redirects = 0

    #     while response.status == 301 and \
    #             response.getheader('Location') is not None and \
    #             (self.max_redirects == UNLIMITED_REDIRECTS or redirects < self.max_redirects):
    #         redirects += 1
    #         uri = response.getheader('Location')
    #         location = urlparse(uri)

    #         logging.getLogger(__name__).debug("status: %s reason: '%s' location: '%s'",
    #                                           response.status, response.reason, uri)

    #         if self.host != location.hostname or self.port != location.port:
    #             self._connection.close()
    #             self.host = location.hostname
    #             self.port = location.port
    #             self._connection = self._init_connection()

    #         response = self._retry_request(method, uri, body=body, headers=headers)

    #     return response

    def close(self):
        """Close the connection now (rather than whenever .__del__() is
        called).

        The connection will be unusable from this point forward; an
        Error (or subclass) exception will be raised if any operation
        is attempted with the connection. The same applies to all
        cursor objects trying to use the connection. Note that closing
        a connection without committing the changes first will cause an
        implicit rollback to be performed."""
        if self.libdqlite:
            self.libdqlite.dqlite_disconnect()
        #self._connection.close()
        if self._ephemeral is not None:
            self._ephemeral.__exit__(None, None, None)
            self._ephemeral = None

    def __del__(self):
        self.close()

    def commit(self):
        """Database modules that do not support transactions should
        implement this method with void functionality."""
        pass

    # def rollback(self):
    #     """处理事务回滚"""
    #     print("Rolling back in rollback ...... in pydqlite")
        
    #     try:
    #         # 如果 pydqlite 支持事务，可以调用底层的 rollback API
    #         # self.libdqlite.dqlite_rollback()

    #         # 如果不支持，可以抛出异常
    #         raise NotImplementedError("Dqlite does not support rollback.")
    #     except Exception as e:
    #         print(f"Rollback failed: {e}")

    def query(self, operation, parameters=None):

        result =  self.libdqlite.dqlite_query(operation)
        return result
    
    def cursor(self):
        """返回新的游标对象，并保存游标"""
        if self._current_cursor is not None:
            print("old Cursor 1111111111111111111")
            return self._current_cursor
        else:
            print("new Cursor222222222222222222222222")
            self._current_cursor = Cursor(self)
            return self._current_cursor
    

    def execute(self, statement, parameters=None):
        """执行查询并返回游标"""
        print("2222222jjjjjjjjjjjjjjjjjjjjjjjjjjjjjj")
        if self._current_cursor is None:
            self._current_cursor = self.cursor()
        self._current_cursor.execute(statement, parameters)
        return self._current_cursor
