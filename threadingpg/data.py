import abc

class Column(metaclass=abc.ABCMeta):
    def __init__(self, 
                 column_name:str,
                 data_type:str,
                 null_ok:bool = True,
                 precision: int = None,
                 scale: int = None,
                 type_code: int = None) -> None:
        self.column_name = column_name
        self.data_type = data_type
        self.null_ok = null_ok
        self.precision = precision
        self.scale = scale
        self.type_code = type_code
    
    def set(self, value):
        self.v = value
        
class Row(metaclass=abc.ABCMeta):
    def __init__(self, index:int = -1) -> None:
        self.index = index

class Table(metaclass=abc.ABCMeta):
    def __init__(self, table_name:str) -> None:
        self.table_name = table_name