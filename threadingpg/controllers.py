import queue
import select
import socket
import threading
import multiprocessing
import ctypes
import sys

import psycopg2
import psycopg2.extensions
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager

from . import query
from . import condition
from . import data

class Controller:
    def __init__(self) -> None:
        pass
    
    def connect(self, dbname:str, user:str, password:str, port:int, host:str="localhost" ):
        '''
        Start Connection. auto commit (psycopg2.connect())\n
        Parameters
        -
        dbname(str): postgresql database name.\n
        user(str): user id.\n
        password(str): password\n
        port(int): port number\n
        host(str): host address. default "localhost"\n
        '''
        self.dsn = psycopg2.extensions.make_dsn(host=host, dbname=dbname, user=user, password=password, port=port)
        self.__connection:psycopg2.extensions.connection = psycopg2.connect(self.dsn)
        self.__connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    
    def close(self):
        self.__connection.close()
        
    def cancel(self):
        self.__connection.cancel()
    
    def reset(self):
        self.__connection.reset()
        
    def get_connection(self) -> psycopg2.extensions.connection:
        return self.__connection
    
    @contextmanager
    def get(self):
        '''
        base cursor.close()\n
        Usage
        -
        with get() as (cursor, conn):
            cursor.execute(query)
            result = cursor.fetchone()
        
        '''
        cursor = self.__connection.cursor()
        try:
            yield cursor, self.__connection
        finally:
            cursor.close()
            
    def execute(self, excutable_query:str):
        with self.get() as (cursor, _):
            cursor.execute(excutable_query)
    
    def get_code_by_datatype(self):
        typedict = {}
        with self.get() as (_cursor, _):
            _cursor.execute("select oid, typname from pg_type")
            rs = _cursor.fetchall()
            for r in rs:
                typedict[str(r[1])] = r[0]
        return typedict
    
    def get_datatype_by_code(self):
        typedict = {}
        with self.get() as (_cursor, _):
            _cursor.execute("select oid, typname from pg_type")
            rs = _cursor.fetchall()
            for r in rs:
                typedict[str(r[0])] = r[1]
        return typedict
    
    # Table
    def create_table(self, table:data.Table):
        column_dict = {}
        not_null_dict = {}
        unique_dict = {}
        references_dict = {}
        
        for column_name in dir(table):
            column = getattr(table, column_name)
            if isinstance(column, data.Column):
                column_dict[column_name] = column.data_type
                    
                if column.is_nullable is False:
                    not_null_dict[column_name] = 1
                    
                if column.is_unique:
                    unique_dict[column_name] = 1
                    
                if 0<len(column.references):
                    for reference in column.references:
                        if column_name not in references_dict:
                            references_dict[column_name] = {}
                        
                        if reference.table_name not in references_dict[column_name]:
                            references_dict[column_name][reference.table_name] = []
                        
                        references_dict[column_name][reference.table_name].append(reference.name)
                        
        # PRIMARY KEY	해당 제약 조건이 있는 컬럼의 값은 테이블내에서 유일해야 하고 반드시 NOT NULL 이어야 합니다.
        # CHECK	해당 제약 조건이 있는 컬럼은 지정하는 조건에 맞는 값이 들어가야 합니다.
        # REFERENCES	해당 제약 조건이 있는 컬럼의 값은 참조하는 테이블의 특정 컬럼에 값이 존재해야 합니다.
                    
        create_query = query.create_table(table.table_name, column_dict, not_null_dict, unique_dict, references_dict)
        with self.get() as (cursor, _):
            cursor.execute(create_query)
        
    def drop_table(self, table:data.Table):
        drop_quary = query.drop_table(table.table_name)
        with self.get() as (cursor, _):
            cursor.execute(drop_quary)
            
    def is_exist_table(self, table:data.Table, table_schema:str = 'public') -> bool:
        result = False
        is_exist_table_query = query.is_exist_table(table.table_name, table_schema)
        with self.get() as (cursor, _):
            cursor.execute(is_exist_table_query)
            result_fetch = cursor.fetchone()
            result = result_fetch[0]
        return result
    
    # Columns
    def get_columns(self, table:data.Table, table_schema:str = 'public') -> dict:
        '''
        Parameter
        -
        table (threadingpg.data.Table): Table with 'table_name'.\n
        table_schema (str): based on query\n
        Return
        -
        column data (dict)
        {'column_name':{column data},\n
        'column_name':{column data}}
        '''
        result = {}
        get_columns_query = query.get_columns(table.table_name, table_schema)
        with self.get() as (cursor, _):
            cursor.execute(get_columns_query)
            type_code_by_data_name = {}
            for desc in cursor.description:
                type_code_by_data_name[desc.name] = desc.type_code
            
            column_data_results = cursor.fetchall()
            for column_data_result in column_data_results:
                column_data = {}
                for index, data_name in enumerate(type_code_by_data_name):
                    column_data[data_name] = column_data_result[index]
                column_name = column_data['column_name']
                result[column_name] = column_data
                
        return result
    
    def is_exist_column(self, column:data.Column, table_schema:str='public') -> bool:
        result = False
        is_exist_column_query = query.is_exist_column(column.table_name, column.name, table_schema)
        with self.get() as (cursor, _):
            cursor.execute(is_exist_column_query)
            result_fetch = cursor.fetchone()
            result = result_fetch[0]
        return result
    
    def get_column_names(self, table:data.Table, table_schema='public') -> list:
        result = []
        get_column_names_query = query.get_column_names(table.table_name, table_schema)
        with self.get() as (cursor, _):
            cursor.execute(get_column_names_query)
            result = [row[0] for row in cursor]
        return result

    # Row
    def select(self, 
               table: data.Table, 
               where: condition.Condition=None, 
               order_by: condition.Condition=None, 
               limit_count: int = None) -> tuple:
        '''
        Parameter
        -
        table (data.Table) : \n
        where (condition.Condition): default None\n
        order_by (condition.Condition): default None\n
        limit_count (int): default None\n
        Return
        -
        ([str], [tuple])\n
        [str] : list of column name\n
        [tuple] : list of row(tuple)
        
        '''
        where_str = where.parse() if where else None
        order_by_str = order_by.parse() if order_by else None
        select_query = query.select(table_name= table.table_name, 
                                    condition_query= where_str, 
                                    order_by_query= order_by_str, 
                                    limit_count= limit_count)
        rows = None
        columns = None
        with self.get() as (cursor, _):
            cursor.execute(select_query)
            columns = [desc.name for desc in cursor.description]
            rows = cursor.fetchall()
        return (columns, rows)
        
    def insert_row(self, table: data.Table, row: data.Row):
        '''
        Parameters
        -
        table (Table): with table_name and column data\n
        row (Row): insert row data\n
        '''
        value_by_column_name = {}
        for variable_name in dir(table):
            variable = getattr(table, variable_name)
            if isinstance(variable, data.Column):
                if variable_name in row.__dict__:
                    value_by_column_name[variable_name] = row.__dict__[variable_name]
        insert_query = query.insert(table.table_name, value_by_column_name)
        with self.get() as (cursor, _):
            cursor.execute(insert_query)
            
        
    def insert_dict(self, table: data.Table, insert_data: dict):
        '''
        Parameters
        -
        table (Table): with table_name and column data\n
        insert_data (dict): insert data. ex) {'column_name':'value'}
        '''
        insert_query = query.insert(table.table_name, insert_data)
        with self.get() as (cursor, _):
            cursor.execute(insert_query)
            
    
    def update_row(self, table: data.Table, row:data.Row, where:condition.Condition):
        '''
        table (data.Table)
        row (data.Row)
        where (condition.Condition)
        '''
        value_by_column_name = {}
        for variable_name in dir(table):
            column = getattr(table, variable_name)
            if isinstance(column, data.Column):
                if column.name in row.__dict__ and row.__dict__[column.name]:
                    value_by_column_name[column.name] = row.__dict__[column.name]
        update_query = query.update(table.table_name, value_by_column_name, where.parse())
        with self.get() as (cursor, _):
            cursor.execute(update_query)
    
    def delete_row(self, table: data.Table, where:condition.Condition):
        '''
        table (data.Table)
        where (condition.Condition)
        '''
        delete_query = query.delete(table.table_name, where.parse())
        with self.get() as (cursor, _):
            cursor.execute(delete_query)

    

class Pool(Controller):
    def __init__(self, dbname:str, user:str, password:str, port:int, host:str="localhost", minconn:int = 1, maxconn:int = 5) -> None:
        '''
        Start Connection Pool. auto commit (set ThreadedConnectionPool())\n
        Parameters
        -
        dbname(str): postgresql database name.\n
        user(str): user id.\n
        password(str): password\n
        port(int): port number\n
        host(str): host address. default "localhost"\n
        minconn(int): ThreadedConnectionPool's minconn. default 1\n
        maxconn(int): ThreadedConnectionPool's maxconn. default 5\n
        '''
        self.dsn = psycopg2.extensions.make_dsn(host=host, dbname=dbname, user=user, password=password, port=port)
        self.__pool = ThreadedConnectionPool(minconn, maxconn, self.dsn)
        
    def close(self):
        '''
        connection_pool.closeall()
        '''
        if self.__pool is not None and self.__pool.closed is False:
            self.__pool.closeall()

    @contextmanager
    def get(self):
        '''
        Auto .getconn(), .putconn() and cursor.close()\n
        Usage
        -
        with get() as (cursor, conn):
            cursor.execute(query)
            result = cursor.fetchone()
        
        '''
        conn:psycopg2.extensions.connection = self.__pool.getconn()
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            yield cursor, conn
        finally:
            cursor.close()
            self.__pool.putconn(conn)
    
# Trigger
class TriggerListner(Controller):
    def __init__(self) -> None:
        super().__init__()
        self.__is_listening = multiprocessing.Value(ctypes.c_bool, True)
        self.notify_queue = queue.Queue()
   
    def create_function(self,
                        function_name:str, 
                        channel_name:str,
                        is_replace:bool = True,
                        is_get_operation:bool = True,
                        is_get_timestamp:bool = True,
                        is_get_tablename:bool = True,
                        is_get_new:bool = True,
                        is_get_old:bool = True,
                        is_update:bool = True,
                        is_insert:bool = True,
                        is_delete:bool = True,
                        is_raise_unknown_operation:bool = True,
                        is_after_trigger:bool = True,
                        is_inline:bool = False,
                        in_space:str = '    '):
        create_trigger_function_query = query.create_function(function_name, 
                                                                    channel_name,
                                                                    is_replace,
                                                                    is_get_operation,
                                                                    is_get_timestamp,
                                                                    is_get_tablename,
                                                                    is_get_new,
                                                                    is_get_old,
                                                                    is_update,
                                                                    is_insert,
                                                                    is_delete,
                                                                    is_raise_unknown_operation,
                                                                    is_after_trigger,
                                                                    is_inline,
                                                                    in_space)
        with self.get() as (cursor, _):
            cursor.execute(create_trigger_function_query)
            
    def create_trigger(self, 
                       table:data.Table, 
                       trigger_name:str, 
                       function_name:str,
                       is_replace:bool = False,
                       is_after:bool = True,
                       is_insert:bool = True,
                       is_update:bool = True,
                       is_delete:bool = True):
        '''
        Parameters
        -
        table (threadingpg.data.Table):\n
        trigger_name (str):\n
        function_name (str):\n
        is_replace (bool):\n
        is_after (bool):\n
        is_insert (bool):\n
        is_update (bool):\n
        is_delete (bool):\n
        '''
        create_trigger_query = query.create_trigger(table.table_name, 
                                                    trigger_name, 
                                                    function_name,
                                                    is_replace,
                                                    is_after,
                                                    is_insert,
                                                    is_update,
                                                    is_delete)
        with self.get() as (cursor, _):
            cursor.execute(create_trigger_query)
            
    def drop_trigger(self, table:data.Table, trigger_name:str):
        drop_trigger_query = query.drop_trigger(table.table_name, trigger_name)
        with self.get() as (cursor, _):
            cursor.execute(drop_trigger_query)
    
    def drop_function(self, function_name:str):
        drop_function_query = query.drop_function(function_name)
        with self.get() as (cursor, _):
            cursor.execute(drop_function_query)
    
    def start_listening(self):
        self.__is_listening.value = True
        self.__close_sender, self.__close_receiver = socket.socketpair()
        
        if sys.platform == "linux":
            self.__channel_listen_epoll = select.epoll()
            self.__channel_listen_epoll.register(self.__close_receiver, select.EPOLLET | select.EPOLLIN | select.EPOLLHUP | select.EPOLLRDHUP)
        elif sys.platform == "darwin":
            pass
            
        self.__listening_thread = threading.Thread(target=self.__listening, args=(self.__close_receiver, ))
        self.__listening_thread.start()
    
    def stop_listening(self):
        self.__is_listening.value = False
        self.__close_sender.shutdown(socket.SHUT_RDWR)
        self.__listening_thread.join()
        self.close()
        
    def __listening(self, close_receiver:socket.socket):
        if sys.platform == "linux":
            while self.__is_listening.value:
                events = self.__channel_listen_epoll.poll()
                if self.__is_listening.value:
                    for detect_fileno, detect_event in events:
                        if detect_fileno == close_receiver.fileno():
                            self.__is_listening.value = False
                            break
                        elif detect_fileno == self.get_connection().fileno():
                            if detect_event & (select.EPOLLIN | select.EPOLLPRI):
                                self.get_connection().poll()
                                while self.get_connection().notifies:
                                    notify = self.get_connection().notifies.pop(0)
                                    self.notify_queue.put_nowait(notify.payload)
                           
        else:
            while self.__is_listening.value:
                readables, writeables, exceptions = select.select([self.get_connection(), close_receiver],[],[])
                for s in readables:
                    if s == self.get_connection():
                        if self.get_connection().closed:
                            self.__is_listening.value = False
                            break
                        self.get_connection().poll()
                        while self.get_connection().notifies:
                            notify = self.get_connection().notifies.pop(0)
                            self.notify_queue.put_nowait(notify.payload)
                            
                    elif s == close_receiver:
                        self.__is_listening.value = False
                        break
                for exce in exceptions:
                    pass
                
        self.notify_queue.put_nowait(None)
        
    def listen_channel(self, channel_name:str):
        listen_channel_query = query.listen_channel(channel_name)
        # with self.get() as (cursor, conn):
        cursor = self.get_connection().cursor()
        cursor.execute(listen_channel_query)
        
        if sys.platform == "linux":
            self.__channel_listen_epoll.register(self.get_connection(), select.EPOLLET | select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP)
        elif sys.platform == "darwin":
            pass

    def unlisten_channel(self, channel_name):
        unlisten_channel_query = query.unlisten_channel(channel_name)
        # with self.get() as (cursor, conn):
        cursor = self.get_connection().cursor()
        cursor.execute(unlisten_channel_query)
        
        if sys.platform == "linux":
            self.__channel_listen_epoll.unregister(self.get_connection())
        elif sys.platform == "darwin":
            pass