import collections

    
class Column:
    def __init__(self, 
                 data_type:str, 
                 column_name:str = None,
                 null_ok:bool = True) -> None:
        self.column_name = column_name
        self.data_type = data_type
        self.null_ok = null_ok
        # self.precision: int = None
        # self.scale: int = None
        # self.type_code: int = None
    
    def set(self, value):
        self.v = value
        
class Row:
    def __init__(self) -> None:
        pass

class Table:
    def __init__(self) -> None:
        self.table_name:str = None
        self.column_dict = collections.defaultdict(Column)
        self.column_list = []
        
        
    
    def add_column(self,
                   column_name:str, 
                   data_type:str,
                   null_ok:bool = True,
                   variable_name:str = None) -> None:
        key = (variable_name if variable_name else column_name)
        col = Column(column_name, data_type, null_ok)
        self.column_dict[key] = col
        self.column_list.append(col)