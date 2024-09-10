
from __future__ import unicode_literals

from collections import OrderedDict
import json
import logging
import sys
import re
import datetime

try:
    # pylint: disable=no-name-in-module
    from urllib.parse import urlencode
except ImportError:
    # pylint: disable=no-name-in-module
    from urllib import urlencode

from .exceptions import Error, ProgrammingError

from .row import Row
from .extensions import _convert_to_python, _adapt_from_python, _column_stripper


if sys.version_info[0] >= 3:
    basestring = str
    _urlencode = urlencode
else:
    # avoid UnicodeEncodeError from urlencode
    def _urlencode(query, doseq=0):
        return urlencode(dict(
            (k if isinstance(k, bytes) else k.encode('utf-8'),
             v if isinstance(v, bytes) else v.encode('utf-8'))
            for k, v in query.items()), doseq=doseq)


class Cursor(object):
    arraysize = 1

    def __init__(self, connection, debug=False):
        self._connection = connection
        self.messages = []
        self.lastrowid = None
        self.description = None
        self.rownumber = 0
        self.rowcount = -1
        self.arraysize = 1
        self._rows = None
        self._column_type_cache = {}
        self.debug = debug

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    @property
    def connection(self):
        return self._connection

    def close(self):
        self._rows = None

    # def _request(self, method, uri, body=None, headers={}):
    #     logger = logging.getLogger(__name__)
    #     debug = logger.getEffectiveLevel() < logging.DEBUG
    #     logger.debug(
    #         'request method: %s uri: %s headers: %s body: %s',
    #         method,
    #         uri,
    #         headers,
    #         body)
    #     response = self.connection._fetch_response(
    #         method, uri, body=body, headers=headers)
    #     logger.debug(
    #         "status: %s reason: %s",
    #         response.status,
    #         response.reason)
    #     response_text = response.read().decode('utf-8')
    #     logger.debug("raw response: %s", response_text)
    #     try:
    #         response_json = json.loads(
    #             response_text, object_pairs_hook=OrderedDict)
    #     except Exception as e:
    #         raise(e)
    #     if debug:
    #         logger.debug(
    #             "formatted response: %s",
    #             json.dumps(
    #                 response_json,
    #                 indent=4))
    #     return response_json

    #TODO: remove this -- pass args directly to DB and let it do param subst
    def _substitute_params(self, operation, parameters):
        '''
        SQLite natively supports only the types TEXT, INTEGER, REAL, BLOB and
        NULL
        '''

        param_matches = 0

        qmark_re = re.compile(r"(\?)")
        named_re = re.compile(r"(:{1}[a-zA-Z]+?\b)")

        qmark_matches = qmark_re.findall(operation)
        named_matches = named_re.findall(operation)
        param_matches = len(qmark_matches) + len(named_matches)

        # Matches but no parameters
        if param_matches > 0 and parameters is None:
            raise ProgrammingError('paramater required but not given: %s' %
                                   operation)

        # No regex matches and no parameters.
        if parameters is None:
            return operation

        if len(qmark_matches) > 0 and len(named_matches) > 0:
            raise ProgrammingError('different paramater types in operation not'
                                   'permitted: %s %s' % 
                                   (operation, parameters))

        if isinstance(parameters, dict):
            # parameters is a dict or a dict subclass
            if len(qmark_matches) > 0:
                raise ProgrammingError('Unamed binding used, but you supplied '
                                       'a dictionary (which has only names): '
                                       '%s %s' % (operation, parameters))
            for op_key in named_matches:
                try:
                    operation = operation.replace(op_key, 
                                                 _adapt_from_python(parameters[op_key[1:]]))
                except KeyError:
                    raise ProgrammingError('the named parameters given do not '
                                           'match operation: %s %s' %
                                           (operation, parameters))
        else:
            # parameters is a sequence
            if param_matches != len(parameters):
                raise ProgrammingError('incorrect number of parameters '
                                       '(%s != %s): %s %s' % (param_matches, 
                                       len(parameters), operation, parameters))
            if len(named_matches) > 0:
                raise ProgrammingError('Named binding used, but you supplied a'
                                       ' sequence (which has no names): %s %s' %
                                       (operation, parameters))
            for i in range(len(parameters)):
                operation = operation.replace('?', 
                                              _adapt_from_python(parameters[i]), 1)

        return operation

    def _get_sql_command(self, sql_str):
        return sql_str.split(None, 1)[0].upper()

    # def _parse_query_result(self, result_str):
    #     """
    #     解析从 dqlite_query 返回的结果字符串，并将其转换为行列表。
    #     """
    #     rows = []
        
    #     # 简单处理结果字符串，假设结果是以换行分隔的行，每行以逗号分隔
    #     for row in result_str.split('\n'):
    #         if row.strip():  # 跳过空行
    #             rows.append(tuple(row.split(',')))  # 将每行以逗号分隔成字段
        
    #     return rows


    # def _parse_query_result(self, query_result_str):
    #     """
    #     解析查询结果字符串，提取列名和数据行。

    #     :param query_result_str: 从查询返回的原始结果字符串
    #     :return: (columns, rows) - 列名和数据行的元组
    #     """
    #     lines = query_result_str.strip().split("\n")
        
    #     # 查找列名 "Columns:" 部分
    #     columns = []
    #     rows = []
    #     in_columns_section = False
        
    #     for i, line in enumerate(lines):
    #         # 检查是否有 "Columns:" 标记
    #         if line.startswith("Columns:"):
    #             in_columns_section = True
    #             continue
            
    #         if in_columns_section:
    #             # 空行表示列部分的结束
    #             if line.strip() == "":
    #                 break
    #             # 添加列名
    #             columns.append(line.strip())

    #     # 获取数据部分，从列结束之后的下一行开始读取
    #     data_start_index = i + 1 + len(columns)  # 跳过列名部分
    #     for line in lines[data_start_index:]:
    #         if line.strip():
    #             # 忽略非数据行
    #             if line.startswith("Results:"):
    #                 continue
    #             # 按空格分隔数据列
    #             rows.append(line.split())

    #     return columns, rows


    def process_datetime(self, value):
        try:
        # 先尝试解析带有毫秒的 ISO 8601 格式
            try:
                dt = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                # 如果失败，再尝试解析不带毫秒的 ISO 8601 格式
                dt = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
            
        except ValueError as e:
            raise ValueError(f"Couldn't parse datetime string: {value}") from e

        formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')
        return formatted_date
        
        
    def _parse_query_result(self, query_result_str):
        # 解析 JSON 字符串
        try:
            result_json = json.loads(query_result_str)
        except json.JSONDecodeError as e:
            print(f"Failed to parse query result: {e} 返回两个空列表")
            return [], []

        # 提取列名
        columns = []
        if "columns" in result_json:
            for col_info in result_json["columns"]:
                columns.append((
                    col_info.get("name"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    col_info.get("type")
                ))

        # 提取行数据并根据列类型转换数据
        rows = []
        if "rows" in result_json:
            for row in result_json["rows"]:
                parsed_row = []
                for value, col_info in zip(row, result_json["columns"]):
                    # 如果列类型是 DATETIME，使用 process_datetime 进行转换
                    if col_info.get("type") == "TIME":
                        value = self.process_datetime(value)
                    parsed_row.append(value)
                rows.append(parsed_row)

        return columns, rows

    def execute(self, operation, parameters=None):
        print(f"222222 execute {self.rownumber}")
        self.rownumber = 0
        # Step 1: 替换参数并编码为字节
        operation = self._substitute_params(operation, parameters)
        query = operation.encode()  # 转换为字节类型
        print(f"Executing query: {query}")

        # Step 2: 执行查询，调用 dqlite_query 并检查结果
        try:
            query_result = self._connection.libdqlite.dqlite_query(query)
            print(f"Raw query result: {query_result}")
        except Exception as e:
            print(f"Query execution failed: {e}")
            self._rows = []
            self.rowcount = 0
            return self


        # Step 3: 检查是否是 `UPDATE` 或 `DELETE` 操作
        if operation.strip().upper().startswith(("UPDATE", "DELETE", "INSERT")):
            # 对于修改操作（UPDATE, DELETE, INSERT），只需要返回影响的行数
            if query_result:
                query_result_str = query_result.decode()
                print(f"Decoded query result: {query_result_str}")
                # 假设影响的行数可以通过解析结果中的某个值来获得
                # 例如，解析一个返回的行数（如果有）
                try:
                    _, affected_rows = self._parse_query_result(query_result_str)
                    self.rowcount = len(affected_rows)
                except Exception as e:
                    print(f"Failed to parse query result for row count: {e}")
                    self.rowcount = 0
            else:
                self.rowcount = 0
            self._rows = []
            self.description = None
            return self

        # Step 3: 解析查询结果
        if query_result:
            query_result_str = query_result.decode()
            print(f"Decoded query result: {query_result_str}")

            # Step 4: 解析行数据
            try:
                column_names, self._rows = self._parse_query_result(query_result_str)
                self.rowcount = len(self._rows)
                print(f"Parsed rows: {self._rows}")
            except Exception as e:
                print(f"Failed to parse query result: {e}")
                self._rows = []
                self.rowcount = 0
                return self

            # Step 5: 构造 `description`，从列名生成元数据
            self.description = [
                (col_name, None, None, None, None, None, None, None)
                for col_name in column_names
            ]
            print(f"Description (column metadata): {self.description}")
            
            
        else:
            # 如果查询没有返回任何结果
            print("No result or query failed")
            self._rows = []
            self.rowcount = 0
            self.description = None
            #self.rownumber = 0
        
        return self


    # def execute(self, operation, parameters=None):
    #     operation = self._substitute_params(operation, parameters)
    #     query = operation.encode()  # 转换为字节类型
    #     print(query)
    #     query_result = self._connection.libdqlite.dqlite_query(query)
    #     print(query_result)
    #     if query_result:
    #         # 解析查询结果并写入 self._rows
    #         query_result_str = query_result.decode()
    #         print(query_result_str)
    #         self._rows = self._parse_query_result(query_result_str)
    #         self.rowcount = len(self._rows)
    #         print(f"Query result written to self._rows: {self._rows}")
    #     else:
    #         print("No result or query failed")
    #         self._rows = []
    #         self.rowcount = 0
        
    #     return self

    def executemany(self, operation, seq_of_parameters=None):
        if not isinstance(operation, basestring):
            raise ValueError("argument must be a string, not '{}'".format(type(operation).__name__))

        self._rows = []
        self.rowcount = 0

        # 对每组参数执行一次查询
        for parameters in seq_of_parameters:
            # 替换参数
            query = self._substitute_params(operation, parameters).encode()  # 转换为字节类型
            print(f"Executing query: {query}")
            
            # 执行查询
            query_result = self._connection.libdqlite.dqlite_query(query)
            print(query_result)

            if query_result:
                # 解析查询结果并追加到 self._rows
                query_result_str = query_result.decode()
                print(f"Query result: {query_result_str}")
                rows = self._parse_query_result(query_result_str)
                self._rows.extend(rows)
                self.rowcount += len(rows)
            else:
                print("No result or query failed for this execution.")

        print(f"Total rows affected: {self.rowcount}")
        return self

    # def fetchone(self):
    #     '''Fetch the next row'''
    #     if self._rows is None or self.rownumber >= len(self._rows):
    #         return None
    #     result = self._rows[self.rownumber]
    #     self.rownumber += 1
    #     return result
    
    def fetchone(self):
        print(f"1111111111 刚进来  Fetching row at position {self.rownumber}")
        # 检查是否还有数据行未被读取
        if self.rownumber < len(self._rows):
            row = self._rows[self.rownumber]
            self.rownumber += 1  # 增加行号
            print(f"22222 增加了1 Fetching row at position {self.rownumber}")
            return row
        print(f"3333333  没有增加 Fetching row at position {self.rownumber}")
        return None  # 没有更多行时返回 None

    def fetchmany(self, size=None):
        remaining = self.arraysize if size is None else size
        remaining = min(remaining, self.rowcount - self.rownumber)
        return [self.fetchone() for i in range(remaining)]

    def fetchall(self):
        print("Fetching all rows in pydqlite/cursors.py..........")
        rows = []
        while self.rownumber < self.rowcount:
            rows.append(self.fetchone())
        return rows

    def setinputsizes(self, sizes):
        raise NotImplementedError(self)

    def setoutputsize(self, size, column=None):
        raise NotImplementedError(self)

    def scroll(self, value, mode='relative'):
        raise NotImplementedError(self)

    def next(self):
        raise NotImplementedError(self)

    def __iter__(self):
        while self.rownumber < self.rowcount:
            yield self.fetchone()
