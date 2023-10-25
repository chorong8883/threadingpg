from typing import Any
import psycopg2
import psycopg2.extensions
from psycopg2.pool import ThreadedConnectionPool
# from psycopg2.extensions import cursor
# from psycopg2.extensions import connection
# from psycopg2.extensions import make_dsn
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from contextlib import contextmanager
from threadingpg import query
from threadingpg import condition
from threadingpg import data
import inspect
import traceback

class Connector():
    def __init__(self, dbname:str, user:str, password:str, port:int, host:str="localhost") -> None:
        self.dsn = psycopg2.extensions.make_dsn(host=host, dbname=dbname, user=user, password=password, port=port)
        self.__pool = ThreadedConnectionPool(1, 5, self.dsn)
    
    def close(self):
        if self.__pool is not None and self.__pool.closed is False:
            self.__pool.closeall()

    @contextmanager
    def get(self):
        conn:psycopg2.extensions.connection = self.__pool.getconn()
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            yield cursor, conn
        finally:
            cursor.close()
            self.__pool.putconn(conn)
            
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    ################################################################################################################
    # Table
    def create_table(self, table:data.Table):
        try:
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
                            
            print("references_dict" , references_dict)
            # PRIMARY KEY	해당 제약 조건이 있는 컬럼의 값은 테이블내에서 유일해야 하고 반드시 NOT NULL 이어야 합니다.
            # CHECK	해당 제약 조건이 있는 컬럼은 지정하는 조건에 맞는 값이 들어가야 합니다.
            # REFERENCES	해당 제약 조건이 있는 컬럼의 값은 참조하는 테이블의 특정 컬럼에 값이 존재해야 합니다.
                        
            create_query = query.create_table(table.table_name, column_dict, not_null_dict, unique_dict, references_dict)
            print(create_query)
            with self.get() as (cursor, _):
                cursor.execute(create_query)
        except Exception as e:
            print(f"[{e.__class__.__name__}] create_table : {e}\n{traceback.format_exc()}")
    
    def drop_table(self, table:data.Table):
        try:
            q = query.drop_table(table.table_name)
            with self.get() as (cursor, _):
                cursor.execute(q)
        except Exception as e:
            print(f"[{e.__class__.__name__}] drop_table : {e}")
            
    def is_exist_table(self, table:data.Table, table_schema:str = 'public') -> bool:
        result = False
        q = query.is_exist_table(table.table_name, table_schema)
        with self.get() as (cursor, _):
            cursor.execute(q)
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
        result = {}
        q = query.get_columns(table.table_name, table_schema)
        with self.get() as (cursor, _):
            cursor.execute(q)
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
               limit_count: int = None) -> [tuple]:
        '''
        Parameter
        -
        table (data.Table) : \n
        where (condition.Condition): default None\n
        order_by (condition.Condition): default None\n
        limit_count (int): default None\n
        Return
        -
        list of tuple
        
        '''
        where_str = where.parse() if where else None
        order_by_str = order_by.parse() if order_by else None
        select_query = query.select(table_name= table.table_name, 
                                    condition_query= where_str, 
                                    order_by_query= order_by_str, 
                                    limit_count= limit_count)
        result_rows = []
        with self.get() as (cursor, _):
            cursor.execute(select_query)
            result_rows = cursor.fetchall()
        return result_rows
        
    def insert_row(self, table: data.Table, row: data.Row):
        value_by_column_name_dict = {}
        for variable_name in dir(table):
            variable = getattr(table, variable_name)
            if isinstance(variable, data.Column):
                if variable_name in row.__dict__:
                    value_by_column_name_dict[variable_name] = row.__dict__[variable_name]
        insert_query = query.insert(table.table_name, value_by_column_name_dict)
        with self.get() as (cursor, _):
            cursor.execute(insert_query)
            
        
    def insert_dict(self, table: data.Table, insert_data_dict: dict):
        # value_by_column_name_dict = {}
        # for d in dir(table):
        #     col = getattr(table, d)
        #     if isinstance(col, data.Column):
        #         column_name = col.column_name if col.column_name else d
        #         if column_name in insert_data_dict:
        #             value_by_column_name_dict[column_name] = insert_data_dict[column_name]
        insert_query = query.insert(table.table_name, insert_data_dict)
        with self.get() as (cursor, _):
            cursor.execute(insert_query)
            
    
    def update_row(self, table: data.Table, row:data.Row, where:condition.Condition):
        value_by_column_name = {}
        for variable_name in dir(table):
            column = getattr(table, variable_name)
            if isinstance(column, data.Column):
                if column.name in row.__dict__ and row.__dict__[column.name]:
                    value_by_column_name[column.name] = row.__dict__[column.name]
        update_query = query.update(table.table_name, value_by_column_name, where.parse())
        with self.get() as (cursor, _):
            cursor.execute(update_query)
        


# class __BaseConnector:
#     def __init__(self):
#         self.listening_thread = None
    
    
#     @contextmanager
#     def get(self):
#         cursor:cursor = None
#         _conn:connection = None
#         try:
#             yield cursor, _conn
#         finally:
#             if cursor != None:
#                 cursor.close()
    
#     def check_table_info(self, table_info:BaseTableInfo) -> bool:
#         result = True
#         column_names = self.get_column_names(table_info.table_name)
#         if self.check_table_exist_column(table_info, column_names):
#             if self.check_table_order_column(table_info, column_names) is not True:
#                 self.get_reorder_column_table(table_info, column_names)
#         else:
#             result = False
#         return result
            
#     def check_table_exist_column(self, table_info:BaseTableInfo, column_names:list) -> bool:
#         result = True
#         for column_name in column_names:
#             if column_name not in table_info.column_dict:
#                 result = False
#         return result
    
#     def check_table_order_column(self, table_info:BaseTableInfo, column_names:list) -> bool:
#         result = True
#         for index, column_name in enumerate(column_names):
#             if column_name != table_info.column_list[index].name:
#                 result = False
#         return result
    
#     def get_reorder_column_table(self, table_info:BaseTableInfo, column_names:list):
#         table_info.clear_list()
#         for column_name in column_names:
#             table_info.add_column(column_name, table_info.column_dict[column_name])
        
                
            
    
#     def get_rows(self, table_info:BaseTableInfo, condition_query:str=None, order_by_query:str=None, limit_count:int = None) -> list[BaseRowData]:
#         rows = []
#         q = functions.select(table_info.table_name, condition_query=condition_query, order_by_query=order_by_query, limit_count=limit_count)
        
#         with self.get() as (cursor, _):
#             cursor.execute(q)
#             rows = cursor.fetchall()
#         results = []
#         for row in rows:
#             if len(table_info.column_list) == len(row):
#                 row_data = BaseRowData()
#                 for index, col in enumerate(table_info.column_list):
#                     row_data.set_variable(col.name, row[index])
#                 results.append(row_data)
#             # else:
#             #     print("len(table_info.column_list) != len(row)")
#         return results
    
#     def insert(self, table_info:BaseTableInfo, insert_row_data:BaseRowData):
#         valriables_dict = {}
#         for col in table_info.column_list:
#             if col.name in insert_row_data.__dict__:
#                 valriables_dict[col.name] = insert_row_data.__dict__[col.name]
#         q = functions.insert(table_info.table_name, valriables_dict)
#         with self.get() as (cursor, _):
#             cursor.execute(q)
            
#     def update(self, table_info:BaseTableInfo, insert_row_data:BaseRowData, condition_query:str):
#         valriables_dict = {}
#         for col in table_info.column_list:
#             if col.name in insert_row_data.__dict__:
#                 valriables_dict[col.name] = insert_row_data.__dict__[col.name]        
#         q = functions.update(table_info.table_name, valriables_dict, condition_query)
#         with self.get() as (cursor, _):
#             cursor.execute(q)
            
#     def create_table(self, table_info:BaseTableInfo):
#         valriables_dict = {}
#         for col in table_info.column_list:
#             valriables_dict[col.name] = col.value_type
            
#         q = functions.create_table(table_info.table_name, valriables_dict)
#         with self.get() as (cursor, _):
#             cursor.execute(q)
            
#     def drop_table(self, table_name:str):
#         q = functions.drop_table(table_name)
#         with self.get() as (cursor, _):
#             cursor.execute(q)
            
#     def is_exist_table(self, table_name:str, table_schema = 'public') -> bool:
#         result = False
#         q = functions.is_exist_table(table_name, table_schema)
#         with self.get() as (cursor, _):
#             cursor.execute(q)
#             result_fetch = cursor.fetchone()
#             result = result_fetch[0]
#         return result
        
#     def is_exist_column(self, table_name:str, column_name:str, table_schema='public') -> bool:
#         result = False
#         q = functions.is_exist_column(table_name, column_name, table_schema)
#         with self.get() as (cursor, _):
#             cursor.execute(q)
#             result_fetch = cursor.fetchone()
#             result = result_fetch[0]
#         return result

#     def get_column_names(self, table_name:str, table_schema='public') -> list:
#         result = []
#         q = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name = '{table_name}'"
#         with self.get() as (cursor, _):
#             cursor.execute(q)
#             result = [row[0] for row in cursor]
#         return result
    
#     def is_exist_row(self, table_name:str, condition_query:str):
#         result = False
#         q = functions.is_exist_row(table_name, condition_query)
#         with self.get() as (cursor, _):
#             cursor.execute(q)
#             result_fetch = cursor.fetchone()
#             result = result_fetch[0]
#         return result
    
#     def set_trigger(self, table_name:str, trigger_name:str, function_name:str, channel_name:str, notify_data_type:str):
#         '''
#         notify_data_type:\n
#         table_name\n
#         code_name\n
#         row_data\n
#         '''
        
#         notify_function_q = functions.notify_function(function_name, channel_name, notify_data_type)
#         create_trigger_q = functions.create_trigger(trigger_name, table_name, function_name)
#         with self.get() as (cursor, _):
#             try:
#                 cursor.execute(notify_function_query)
#             except Exception as e:
#                 pass
#             try:
#                 cursor.execute(create_trigger_query)
#             except Exception as e:
#                 pass
            
    
#     def drop_trigger(self, table_name:str, trigger_name:str, function_name:str):
#         drop_trigger_q = functions.drop_trigger(trigger_name, table_name)
#         drop_function_q = functions.drop_function(function_name)
#         with self.get() as (cursor, _):
#             try:
#                 cursor.execute(drop_trigger_query)
#             except Exception as e:
#                 pass
#             try:
#                 cursor.execute(drop_function_query)
#             except Exception as e:
#                 pass
    
#     def start_listening(self, notify_queue : queue.Queue):
#         self.connection_by_fileno = collections.defaultdict(connection)
#         self.epoll = select.epoll()
#         self.is_listening = multiprocessing.Value(ctypes.c_bool, True)
#         self.notify_queue = notify_queue
#         self.close_sender, self.close_recver = socket.socketpair()
#         self.epoll.register(self.close_recver, select.EPOLLET)
        
#         self.listening_thread = threading.Thread(target=self.listening)
#         self.listening_thread.start()
    
#     def stop_listening(self):
#         self.is_listening.value = False
#         self.close_sender.shutdown(socket.SHUT_RDWR)
#         self.listening_thread.join()
#         # print("finish stop_listening")
        
#     def listening(self):
#         while self.is_listening.value:
#             events = self.epoll.poll()
#             if self.is_listening.value:
#                 for detect_fileno, detect_event in events:
#                     if detect_fileno == self.close_recver.fileno():
#                         self.is_listening.value = False
#                         break
#                     elif detect_fileno in self.connection_by_fileno:
#                         if detect_event & (select.EPOLLIN | select.EPOLLPRI):
#                             conn:connection = self.connection_by_fileno[detect_fileno]
#                             res = conn.poll()
#                             # print(f"{detect_event:#06x} conn[{detect_fileno}] len:{len(conn.notifies)} res:{res}")
#                             while conn.notifies:
#                                 notify = conn.notifies.pop(0)
#                                 self.notify_queue.put_nowait(notify.payload)
#                     #     else:
#                     #         print(f"{detect_event:#06x} conn[{detect_fileno}]")
#                     # else:
#                     #     print(f"unknown {detect_fileno} {detect_event:#06x}")
        
#         self.notify_queue.put_nowait(None)
#         # print("finish listening")
            
#     def listen_channel(self, channel_name):
#         listen_channel_q = functions.listen_channel(channel_name)
#         with self.get() as (cursor, _conn):
#             cursor.execute(listen_channel_query)
#             self.connection_by_fileno[_conn.fileno()] = _conn
#             self.epoll.register(_conn.fileno(), select.EPOLLET | select.EPOLLIN | select.EPOLLPRI | select.EPOLLOUT | select.EPOLLHUP | select.EPOLLRDHUP)
#             # print(f"self.epoll.register, {_conn.fileno}")
#         # try:
#         #     while True:
#         #         with self._postgre_connector.get() as (_, conn):
#         #             select.select([conn],[],[])
#         #             if conn.closed:
#         #                 clog.write(LogType.INFORMATION, "postgre Connection Closed.")
#         #                 break
#         #             conn.poll()
#         #             while conn.notifies:
#         #                 notify = conn.notifies.pop(0)
#         #                 notify_queue.put_nowait(notify.payload)
#         #     clog.write(LogType.INFORMATION, "End Notify Listener Thread.")
#         # except Exception as e:
#         #     clog.write(LogType.EXCEPTION, f"{e}")
    
#     def unlisten_channel(self, channel_name):
#         unlisten_channel_q = functions.unlisten_channel(channel_name)
#         with self.get() as (cursor, _conn):
#             cursor.execute(unlisten_channel_query)
#             self.epoll.unregister(_conn)
        
# class ConnectorPool(__BaseConnector):
#     def __init__(self) -> None:
#         super().__init__()
#         pass
    
#     def connect(self, host:str, port:int, database_name:str, user_id:str, password:str, minconn:int, maxconn:int):
#         self.__minconn = minconn
#         self.__maxconn = maxconn
#         self.__dsn = make_dsn(host=host,
#                             port=port,
#                             dbname=database_name,
#                             user=user_id,
#                             password=password)
#         self.__pool = ThreadedConnectionPool(self.__minconn, self.__maxconn, self.__dsn)
    
#     def close(self):
#         if self.__pool is not None and self.__pool.closed is False:
#             self.__pool.closeall()
    
#     @contextmanager
#     def get(self):
#         conn:connection = self.__pool.getconn()
#         conn.autocommit = True
#         cursor = conn.cursor()
#         try:
#             yield cursor, conn
#         finally:
#             cursor.close()
#             self.__pool.putconn(conn)