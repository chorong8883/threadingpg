class Trigger:
    def __init__(self,a,s) -> None:
        pass
    
class NotifyFunction:
    def __init__(self, 
                 function_name:str, 
                 channel_name:str,
                 is_get_operation:bool = True,
                 is_get_timestamp:bool = True,
                 is_get_tablename:bool = True,
                 is_get_new:bool = True,
                 is_get_old:bool = True,
                 is_update:bool = True,
                 is_insert:bool = True,
                 is_delete:bool = True,
                 is_raise_unknown_operation:bool = False) -> None:
        self.function_name = function_name
        self.channel_name = channel_name
        self.is_get_operation = is_get_operation
        self.is_get_timestamp = is_get_timestamp
        self.is_get_tablename = is_get_tablename
        self.is_get_new = is_get_new
        self.is_get_old = is_get_old
        self.is_update = is_update
        self.is_insert = is_insert
        self.is_delete = is_delete
        self.is_raise_unknown_operation = is_raise_unknown_operation
    
    def get_func(self):
        if not (self.function_name and self.channel_name):
            raise ValueError("function_name channel_name")
            
        if not (self.is_get_operation or self.is_get_timestamp or self.is_get_tablename or self.is_get_new or self.is_get_old):
            raise ValueError("get nothing")
        
        if not (self.is_update or self.is_insert or self.is_delete):
            raise ValueError("none case")
         
        query = f"CREATE OR REPLACE FUNCTION {self.function_name}() RETURNS trigger AS $$\n"
        query += f"DECLARE\n"
        if self.is_update or self.is_insert:
            query += f"\tnewrec RECORD;\n"
        if self.is_update or self.is_delete:
            query += f"\toldrec RECORD;\n"
        query += f"\tpayload TEXT;\n"
        query += f"BEGIN\n"
        
        if self.is_update or self.is_insert or self.is_delete:
            query += "\tCASE TG_OP\n"
        
        if self.is_update:
            query += "\tWHEN 'UPDATE' THEN\n"
            query += "\t\tnewrec := NEW;\n"
            query += "\t\toldrec := OLD;\n"
                    
        if self.is_insert:
            query+= "\tWHEN 'INSERT' THEN\n"
            query += "\t\tnewrec := NEW;\n"
        
        if self.is_delete:
            query+= "\tWHEN 'DELETE' THEN\n"
            query += "\t\toldrec := OLD;\n"
            
        if self.is_update or self.is_insert or self.is_delete:
            if self.is_raise_unknown_operation:
                query += "\tELSE\n"
                query += """\t\tRAISE EXCEPTION 'Unknown TG_OP: "%". Should not occur!', TG_OP;\n"""
                        
            query+= "\tEND CASE;\n"
        
        query+= f"\tpayload := json_build_object(\n"
        if self.is_get_timestamp:
            query+= f"\t\t'timestamp', CURRENT_TIMESTAMP,\n"
        if self.is_get_operation:
            query+= f"\t\t'operation', LOWER(TG_OP),\n"
        if self.is_get_tablename:
            query+= f"\t\t'tablename', TG_TABLE_NAME,\n"
        if self.is_get_new:
            if self.is_update or self.is_insert:
                query+= f"\t\t'new_record', row_to_json(newrec),\n"
        if self.is_get_old:
            if self.is_update or self.is_delete:
                query+= f"\t\t'old_record', row_to_json(oldrec),\n"
        query+= f"\t);\n"
        query += f"\tPERFORM pg_notify('{self.channel_name}', payload);\n"
        
        if self.is_update or self.is_insert:
            query += "\tRETURN newrec;\n"
        elif self.is_update or self.is_delete:
            query += "\tRETURN oldrec;\n"
            
        query += "END;\n"
        query += "$$ LANGUAGE plpgsql;\n"
        return query