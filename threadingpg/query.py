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
    query = f"CREATE TABLE {table_name} ({res[:-1]})"
    return query

def drop_table(table_name:str) -> str:
    return f"DROP TABLE {table_name}"
    
def is_exist_table(table_name:str, table_schema:str) -> str:
    return f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{table_schema}' AND table_name = '{table_name}');"

def is_exist_column(table_name:str, column_name:str, table_schema:str) -> str:
    return f"SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name = '{table_name}' AND column_name = '{column_name}')"

def get_columns(table_name:str, table_schema:str) -> str:
    return f"SELECT * FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name = '{table_name}'"

def is_exist_row(table_name:str, condition_query:str) -> str:
    return f"SELECT EXISTS (SELECT FROM {table_name} WHERE {condition_query} LIMIT 1);"




def get_type_name(type_code:int):
    return f"SELECT {type_code}::regtype::text"



def notify_function(function_name:str, channel_name:str, notify_data_type:str):
    '''
    notify_data_type:\n
    table_name\n
    code_name\n
    row_data\n
    '''
    query = f"CREATE OR REPLACE FUNCTION {function_name}() RETURNS trigger AS $$"
            
    if notify_data_type == "table_name":
        query+= f"""
                DECLARE
                    payload TEXT;
                BEGIN
                    payload := json_build_object('table_name',TG_TABLE_NAME, 'action',LOWER(TG_OP));
                    PERFORM pg_notify('{channel_name}', payload);
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """ 
    
    elif notify_data_type == "code_name":
        query+= f"""
                DECLARE
                    payload TEXT;
                BEGIN
                    payload := json_build_object('code_name',NEW.code_name, 'unit_price',NEW.unit_price);
                    PERFORM pg_notify('{channel_name}', payload);
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """
    elif notify_data_type == "row_data":
        query+= f"""
                DECLARE
                    payload TEXT;
                BEGIN
                    payload := row_to_json(NEW);
                    PERFORM pg_notify('{channel_name}', payload);
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """
                    
    elif notify_data_type == "not_define":
        query+= f"""
                DECLARE
                    rec RECORD;
                    dat RECORD;
                    payload TEXT;
                BEGIN
                    CASE TG_OP
                    WHEN 'UPDATE' THEN
                        rec := NEW;
                        dat := OLD;
                    WHEN 'INSERT' THEN
                        rec := NEW;
                    WHEN 'DELETE' THEN
                        rec := OLD;
                    ELSE
                        RAISE EXCEPTION 'Unknown TG_OP: "%". Should not occur!', TG_OP;
                    END CASE;
                    
                    payload := json_build_object('action',LOWER(TG_OP), 'identity',TG_TABLE_NAME, 'record',row_to_json(rec));
                    PERFORM pg_notify('{channel_name}', payload);
                    RETURN rec;
                END;
                $$ LANGUAGE plpgsql;
                """
    # exmple
    # payload := json_build_object('timestamp',CURRENT_TIMESTAMP,'action',LOWER(TG_OP),'schema',TG_TABLE_SCHEMA,'identity',TG_TABLE_NAME,'record',row_to_json(rec), 'old',row_to_json(dat));
    return query

def drop_function(function_name:str):
    return f'DROP FUNCTION {function_name}();'
        
def create_trigger(trigger_name:str, table_name:str, function_name:str):
    return f"CREATE TRIGGER {trigger_name} AFTER INSERT OR UPDATE ON {table_name} FOR EACH ROW EXECUTE PROCEDURE {function_name}();"
    
def drop_trigger(trigger_name:str, table_name:str):
    return f"DROP TRIGGER {trigger_name} on {table_name};"
        
def listen_channel(channel_name:str):
    return f"LISTEN {channel_name};"

def unlisten_channel(channel_name:str):
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


