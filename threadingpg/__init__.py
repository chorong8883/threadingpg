import queue
import select
import socket
import threading
import multiprocessing
import ctypes
import collections
import traceback
import sys
import psycopg2
import psycopg2.extensions
from psycopg2.pool import ThreadedConnectionPool
# from psycopg2.extensions import cursor
from psycopg2.extensions import connection
# from psycopg2.extensions import make_dsn
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from contextlib import contextmanager
from threadingpg import query
from threadingpg import condition
from threadingpg import data

# class NotifyType(enum.Enum):
#     Default = 0
#     TableName = 1
#     RowData = 2
    
# class Notify:
#     def __init__(self, function_name:str, channel_name:str, table_name:str, trigger_name:str) -> None:
#         pass

class Connector():
    def __init__(self, dbname:str, user:str, password:str, port:int, host:str="localhost") -> None:
        '''
        Start Connection Pool.(set ThreadedConnectionPool())\n
        Parameters
        -
        dbname(str): postgresql database name.\n
        user(str): user id.\n
        password(str): password\n
        port(int): port number\n
        host(str): host address. default "localhost"\n
        '''
        self.dsn = psycopg2.extensions.make_dsn(host=host, dbname=dbname, user=user, password=password, port=port)
        self.__pool = ThreadedConnectionPool(1, 5, self.dsn)
        self.__is_listening = multiprocessing.Value(ctypes.c_bool, True)
        
    
    def close(self):
        '''
        connection_pool.closeall()
        '''
        if self.__is_listening.value:
            self.stop_channel_listener()
            
        if self.__pool is not None and self.__pool.closed is False:
            self.__pool.closeall()

    @contextmanager
    def get(self):
        '''
        Auto .getconn(), .putconn() and cursor.close()\n
        Usage\n
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
    
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
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
    
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
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
        print(f"get_columns_query : {get_columns_query}")
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

    
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
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
        
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    # Function
    def create_trigger_function1(self,
                                function_name:str, 
                                channel_name:str):
        create_trigger_function_query = query.create_trigger_function1(function_name, channel_name)
        print(f"create_trigger_function_query :\n{create_trigger_function_query}")
        with self.get() as (cursor, _):
            cursor.execute(create_trigger_function_query)
            
    def create_trigger_function(self,
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
                                is_inline:bool = True,
                                in_space:str = '    '):
        create_trigger_function_query = query.create_trigger_function(function_name, 
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
        print(create_trigger_function_query)
        with self.get() as (cursor, _):
            cursor.execute(create_trigger_function_query)
            
    def create_trigger(self, 
                       table:data.Table, 
                       trigger_name:str, 
                       function_name:str,
                       is_replace:bool = False,
                       is_after:bool = False,
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
        print(create_trigger_query)
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
    
    def start_channel_listener(self, message_queue:queue.Queue):
        self.__listen_connector = psycopg2.connect(self.dsn)
        self.__listen_connector.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        self.__is_listening.value = True
        self.__connection_by_fileno = collections.defaultdict(connection)
        self.__close_sender, self.__close_receiver = socket.socketpair()
        
        if sys.platform == "linux":
            self.__channel_listen_epoll = select.epoll()
            self.__channel_listen_epoll.register(self.__close_receiver, select.EPOLLET | select.EPOLLIN | select.EPOLLHUP | select.EPOLLRDHUP)        
            
        self.__message_queue = message_queue
        
        self.__listening_thread = threading.Thread(target=self.__listening)
        self.__listening_thread.start()
        
    
    def stop_channel_listener(self):
        self.__is_listening.value = False
        self.__close_sender.shutdown(socket.SHUT_RDWR)
        self.__listening_thread.join()
        
    def __listening(self):
        if sys.platform == "linux":
            try:
                while self.__is_listening.value:
                    events = self.__channel_listen_epoll.poll()
                    if self.__is_listening.value:
                        for detect_fileno, detect_event in events:
                            if detect_fileno == self.__close_receiver.fileno():
                                self.__is_listening.value = False
                                print("exit epoll")
                                break
                            elif detect_event & (select.EPOLLIN | select.EPOLLPRI):
                                conn:connection = self.__connection_by_fileno[detect_fileno]
                                res = conn.poll()
                                print(f"{detect_event:#06x} EPOLLIN:{select.EPOLLIN:#06x} EPOLLPRI:{select.EPOLLPRI:#06x} conn[{detect_fileno}] len:{len(conn.notifies)} res:{res}")
                                while conn.notifies:
                                    notify = conn.notifies.pop(0)
                                    self.__message_queue.put_nowait(notify.payload)
                            else:
                                print(f"else {detect_event:#06x} EPOLLOUT:{select.EPOLLOUT:#06x} EPOLLHUP:{select.EPOLLHUP:#06x} conn[{detect_fileno}]")
                                conn:connection = self.__connection_by_fileno[detect_fileno]
                                print(f"else {conn}")
                print("__listening exit ")
            except Exception as e:
                print(f"{e}\n{traceback.format_exc()}")
        else:
            try:
                while self.__is_listening.value:
                    print("__listening")
                    
                    readables, writeables, exceptions = select.select([self.__listen_connector, self.__close_receiver],[],[])
                    print(readables)
                    print(writeables)
                    print(exceptions)
                    for s in readables:
                        if s == self.__listen_connector:
                            if self.__listen_connector.closed:
                                print("postgre Connection Closed.")
                                break
                            self.__listen_connector.poll()
                            while self.__listen_connector.notifies:
                                notify = self.__listen_connector.notifies.pop(0)
                                self.__message_queue.put_nowait(notify.payload)
                        elif s == self.__close_receiver:
                            self.__is_listening.value = False
                            break
                print("End Notify Listener Thread.")
            except Exception as e:
                print(f"{e}\n{traceback.format_exc()}")
            
        self.__message_queue.put_nowait(None)
        
    def listen_channel(self, channel_name:str):
        listen_channel_query = query.listen_channel(channel_name)
        cursor = self.__listen_connector.cursor()
        cursor.execute(listen_channel_query)
        cursor.close()
        self.__connection_by_fileno[self.__listen_connector.fileno()] = self.__listen_connector
        if sys.platform == "linux":
            self.__channel_listen_epoll.register(self.__listen_connector, select.EPOLLET | select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP | select.EPOLLRDHUP)

    def unlisten_channel(self, channel_name):
        unlisten_channel_query = query.unlisten_channel(channel_name)
        cursor = self.__listen_connector.cursor()
        cursor.execute(unlisten_channel_query)
        cursor.close()
        if sys.platform == "linux":
            self.__channel_listen_epoll.unregister(self.__listen_connector)