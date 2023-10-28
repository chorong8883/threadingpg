def select(table_name:str, condition_query:str=None, order_by_query:str=None, limit_count:int=None) -> str:
    query = f"SELECT * FROM {table_name}"
    
    if condition_query is not None and condition_query != "":
        query += f" WHERE {condition_query}"
    
    if order_by_query is not None and order_by_query != "":
        query += f" ORDER BY {order_by_query}"
    
    if limit_count is not None and 0<limit_count:
        query += f" LIMIT {limit_count}"
    
    query += ";"
    return query

def insert(table_name:str, value_by_column_name_dict:dict) -> str:
    '''
    Parameters
    -
    table_name(str): table name
    variables_dict(dict): key is column name, value is value
    '''
    column_names = ''
    values = ''
    for column_name in value_by_column_name_dict:
        if value_by_column_name_dict[column_name] is not None:
            column_names += f"{column_name},"
            values += f"{convert_value_to_query(value_by_column_name_dict[column_name])},"
        
    query = f"INSERT INTO {table_name} ({column_names[:-1]}) VALUES ({values[:-1]});"
    return query

def update(table_name:str, variables_dict:dict, condition_query:str):
    update_query = ''
    for column_name in variables_dict.keys():
        if column_name in variables_dict:
            if variables_dict[column_name] is not None:
                update_query += f"{column_name}={convert_value_to_query(variables_dict[column_name])},"
    return f"UPDATE {table_name} SET {update_query[:-1]} WHERE {condition_query};"

def delete(table_name:str, condition_query:str):
    return f"DELETE FROM {table_name} WHERE {condition_query};"

def convert_value_to_query(value, is_in_list = False) -> str:
    value_query = ''
    
    is_value_list = isinstance(value, list)
    if is_value_list:
        if not is_in_list:
            value_query += "'"
        value_query += "{"
        for v in value:
            value_query += f"{convert_value_to_query(v, True)},"
            
        value_query = value_query[:-1]
        value_query += "}"
        if not is_in_list:
            value_query += "'"
        value_query += ","
    else:
        if isinstance(value, str):
            if is_in_list:
                value_query += f'"{value}",'
            else:
                value_query += f"'{value}',"
        elif isinstance(value, bool):
            if value:
                value_query += f'true,'
            else:
                value_query += f'false,'
        else:
            value_query += f"{value},"
        
    return value_query[:-1]


def create_table(table_name:str, data_type_by_column_name_dict:dict, not_null_dict:dict, unique_dict:dict, references:dict) -> str:
    '''
    Parameters
    -
    table_name(str): table name
    variables_dict(dict): key is column name, value is datatype
    '''
    res = ''
    for column_name in data_type_by_column_name_dict:
        res += f"{column_name} {data_type_by_column_name_dict[column_name]} "
        if column_name in unique_dict:
            res += f"UNIQUE "
        if column_name in not_null_dict:
            res += f"NOT NULL "
        
        if column_name in references:
            res += f"REFERENCES "
            for reference_table_name in references[column_name]:
                res += f"{reference_table_name} "
                if isinstance(references[column_name][reference_table_name], list):
                    res += "("
                    for reference_column_name in references[column_name][reference_table_name]:
                        res += f"{reference_column_name}, "
                    res = f"{res[:-2]}) "
            
        res = f"{res[:-1]},"
    query = f"CREATE TABLE {table_name} ({res[:-1]});"
    return query

def drop_table(table_name:str) -> str:
    return f"DROP TABLE {table_name};"
    
def is_exist_table(table_name:str, table_schema:str) -> str:
    return f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{table_schema}' AND table_name = '{table_name}');"

def is_exist_column(table_name:str, column_name:str, table_schema:str) -> str:
    return f"SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name = '{table_name}' AND column_name = '{column_name}');"

def get_columns(table_name:str, table_schema:str) -> str:
    return f"SELECT * FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name = '{table_name}';"

def get_column_names(table_name:str, table_schema:str) -> str:
    return f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name = '{table_name}';"

def get_type_name(type_code:int) -> str:
    return f"SELECT {type_code}::regtype::text;"

def create_function(function_name:str, 
                    channel_name:str,
                    is_replace:bool,
                    is_get_operation:bool,
                    is_get_timestamp:bool,
                    is_get_tablename:bool,
                    is_get_new:bool,
                    is_get_old:bool,
                    is_update:bool,
                    is_insert:bool,
                    is_delete:bool,
                    is_raise_unknown_operation:bool,
                    is_after_trigger:bool,
                    is_inline:bool,
                    in_space:str = '    ') -> str:
    
    if not (function_name and channel_name):
            raise ValueError("function_name channel_name")
        
    if not (is_get_operation or is_get_timestamp or is_get_tablename or is_get_new or is_get_old):
        raise ValueError("get nothing")
    
    if not (is_update or is_insert or is_delete):
        raise ValueError("none case")
    if is_inline:
        in_space = ""
    query_list = []
        
    command = "CREATE "
    if is_replace:
        command += "OR REPLACE"
    query_list.append(command)
    query_list.append(f"FUNCTION {function_name}()")
    query_list.append("RETURNS trigger")
    query_list.append("AS $$")
    query_list.append("DECLARE")
    query_list.append(f"{in_space}rec RECORD;")
    query_list.append(f"{in_space}payload json;")
    query_list.append("BEGIN")
    
    if is_update or is_insert or is_delete:
        query_list.append(f"{in_space}CASE TG_OP")
    
    if is_update:
        query_list.append(f"{in_space}{in_space}WHEN 'UPDATE' THEN")
        query_list.append(f"{in_space}{in_space}{in_space}rec := NEW;")
                
    if is_insert:
        query_list.append(f"{in_space}{in_space}WHEN 'INSERT' THEN")
        query_list.append(f"{in_space}{in_space}{in_space}rec := NEW;")
    
    if is_delete:
        query_list.append(f"{in_space}{in_space}WHEN 'DELETE' THEN")
        query_list.append(f"{in_space}{in_space}{in_space}rec := OLD;")
        
    if is_update or is_insert or is_delete:
        query_list.append(f"{in_space}{in_space}ELSE")
        query_list.append(f"""{in_space}{in_space}{in_space}RAISE EXCEPTION 'Unknown TG_OP: "%". Should not occur!', TG_OP;""")
        query_list.append(f"{in_space}END CASE;")
    
    query_list.append(f"{in_space}payload := json_build_object(")
    
    payload_variables = []
    if is_get_timestamp:
        payload_variables.append(f"{in_space}{in_space}'timestamp', CURRENT_TIMESTAMP")        
    if is_get_operation:
        payload_variables.append(f"{in_space}{in_space}'operation', LOWER(TG_OP)")
    if is_get_tablename:
        payload_variables.append(f"{in_space}{in_space}'table_name', TG_TABLE_NAME")
    if is_get_new:
        if is_update or is_insert:
            payload_variables.append(f"{in_space}{in_space}'new_record', row_to_json(NEW)")
    if is_get_old:
        if is_update or is_delete:
            payload_variables.append(f"{in_space}{in_space}'old_record', row_to_json(OLD)")
    
    join_payload_variables_str = ",\n"
    if is_inline:
        join_payload_variables_str = ", "
    payload_variables_str = join_payload_variables_str.join(payload_variables)
    
    query_list.append(f"{payload_variables_str}")
    query_list.append(f"{in_space});")
    query_list.append(f"{in_space}PERFORM pg_notify('{channel_name}', payload::text);")
    query_list.append(f"{in_space}RETURN rec;")
    query_list.append("END;")
    query_list.append("$$ LANGUAGE plpgsql;")
    join_str = "\n"
    if is_inline:
        join_str = " "
    return join_str.join(query_list)


def drop_function(function_name:str) -> str:
    return f'DROP FUNCTION {function_name}();'

def select_function(function_name:str, is_definition:bool) -> str:
    query = f"SELECT "
    if is_definition:
        query += "routine_definition "
    else:
        query += "* "
    query += f"FROM INFORMATION_SCHEMA.ROUTINES where routine_name = '{function_name}';"    
    return query

        
def create_trigger(table_name:str,
                   trigger_name:str, 
                   function_name:str,
                   is_replace:bool,
                   is_after:bool,
                   is_insert:bool,
                   is_update:bool,
                   is_delete:bool) -> str:
    '''
    Parameters
    -
    table_name (str):\n
    trigger_name (str):\n 
    function_name (str):\n
    is_replace (bool): REPLACE command available PostgreSQL version >= 14\n
    is_after (bool):\n
    is_insert (bool):\n
    is_update (bool):\n
    is_delete (bool):\n
    
    '''
        
    query = f"CREATE "
    if is_replace:
        query += f"OR REPLACE "
    query += f"TRIGGER {trigger_name} "
    
    if is_after:
        query += f"AFTER "
    else:
        query += f"BEFORE "
    
    trigger_types = []
    if is_insert:
        trigger_types.append("INSERT ")
    if is_update:
        trigger_types.append("UPDATE ")
    if is_delete:
        trigger_types.append("DELETE ")
            
    query += "OR ".join(trigger_types)
    query += f"ON {table_name} FOR EACH ROW EXECUTE PROCEDURE {function_name}();"
    return query
    
def drop_trigger(table_name:str, trigger_name:str) -> str:
    return f"DROP TRIGGER {trigger_name} on {table_name};"

def select_trigger() -> str:
    return f"SELECT * FROM pg_trigger;"

        
def listen_channel(channel_name:str) -> str:
    return f"LISTEN {channel_name};"

def unlisten_channel(channel_name:str) -> str:
    return f"UNLISTEN {channel_name};"







# def create_notify_function_return_table_name(_cursor:cursor, function_name:str, channel_name:str):
#     query = f'''
#             CREATE OR REPLACE FUNCTION {function_name}() RETURNS trigger AS $$
#             DECLARE
#                 payload TEXT;
#             BEGIN
#                 payload := json_build_object('table_name',TG_TABLE_NAME,'action',LOWER(TG_OP));
#                 PERFORM pg_notify('{channel_name}', payload);
#                 RETURN new;
#             END;
#             $$ LANGUAGE plpgsql;
#             '''
#     _cursor.execute(query)

# def create_notify_function_return_code_name(_cursor:cursor, function_name:str, channel_name:str):
#     query = f'''
#             CREATE OR REPLACE FUNCTION {function_name}() RETURNS trigger AS $trigger$
#             DECLARE
#                 payload TEXT;
#             BEGIN
#                 payload := json_build_object('code_name', NEW.code_name, 'bid_price_0',NEW.bid_price_0);
#                 PERFORM pg_notify('{channel_name}', payload);
#                 RETURN NEW;
#             END;        
#             $trigger$ LANGUAGE plpgsql;
#             '''
#     _cursor.execute(query)
        
# def create_notify_function_return_row(_cursor:cursor, function_name:str, channel_name:str):
#     query = f'''
#             CREATE OR REPLACE FUNCTION {function_name}() RETURNS trigger AS $trigger$
#             DECLARE
#                 rec RECORD;
#                 dat RECORD;
#                 payload TEXT;
#             BEGIN
#                 -- Set record row depending on operation
#                 CASE TG_OP
#                 WHEN 'UPDATE' THEN
#                     rec := NEW;
#                     dat := OLD;
#                 WHEN 'INSERT' THEN
#                     rec := NEW;
#                 WHEN 'DELETE' THEN
#                     rec := OLD;
#                 ELSE
#                     RAISE EXCEPTION 'Unknown TG_OP: "%". Should not occur!', TG_OP;
#                 END CASE;
                
#                 payload := json_build_object('action',LOWER(TG_OP),'identity',TG_TABLE_NAME,'record',row_to_json(rec));
#                 PERFORM pg_notify('{channel_name}',payload);                    
#                 RETURN rec;
#             END;        
#             $trigger$ LANGUAGE plpgsql;
#             '''
#             # payload := json_build_object('timestamp',CURRENT_TIMESTAMP,'action',LOWER(TG_OP),'schema',TG_TABLE_SCHEMA,'identity',TG_TABLE_NAME,'record',row_to_json(rec), 'old',row_to_json(dat));
#     _cursor.execute(query)
    
# def drop_function(_cursor:cursor, function_name:str):
#     query = f'DROP FUNCTION {function_name}();'
#     _cursor.execute(query)
        
# def create_trigger(_cursor:cursor, trigger_name:str, table_name:str, function_name:str):
#     query = f"CREATE TRIGGER {trigger_name} AFTER INSERT OR UPDATE ON {table_name} FOR EACH ROW EXECUTE PROCEDURE {function_name}();"
#     _cursor.execute(query)
    
# def drop_trigger(_cursor:cursor, trigger_name:str, table_name:str):
#     query = f"DROP TRIGGER {trigger_name} on {table_name};"
#     _cursor.execute(query)
        
# def listen(_cursor:cursor, channel_name:str):
#     query = f"LISTEN {channel_name};"
#     _cursor.execute(query)


