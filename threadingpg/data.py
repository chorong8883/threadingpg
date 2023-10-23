import abc

class Column(metaclass=abc.ABCMeta):
    def __init__(self, 
                 data_type:str,
                 column_name:str = "",
                 precision: int = None,
                 scale: int = None,
                 type_code: int = None,
                 is_unique:bool = False,
                 is_nullable:bool = True
                 ) -> None:
        self.table_catalog = ""
        self.table_schema = ""
        self.table_name = ""
        
        self.column_name = column_name
        # self.ordinal_position
        # self.column_default
        self.is_nullable = is_nullable
        self.data_type = data_type
        
        self.precision = precision
        self.scale = scale
        self.type_code = type_code
        
        self.is_unique = is_unique
        
        self.character_maximum_length = None
        # self.character_octet_length
        self.numeric_precision = None
        # self.numeric_precision_radix
        self.numeric_scale = None
        # self.datetime_precision
        # self.interval_type
        # self.interval_precision
        # self.character_set_catalog
        # self.character_set_schema
        # self.character_set_name
        # self.collation_catalog
        # self.collation_schema
        # self.collation_name
        # self.domain_catalog
        # self.domain_schema
        # self.domain_name
        # self.udt_catalog
        # self.udt_schema
        self.udt_name = None
        # self.scope_catalog
        # self.scope_schema
        # self.scope_name
        # self.maximum_cardinality
        # self.dtd_identifier
        # self.is_self_referencing
        # self.is_identity
        # self.identity_generation
        # self.identity_start
        # self.identity_increment
        # self.identity_maximum
        # self.identity_minimum
        # self.identity_cycle
        # self.is_generated
        # self.generation_expression
        self.is_updatable = None
    
    def set(self, value):
        self.v = value
        

        
class Row(metaclass=abc.ABCMeta):
    def __init__(self, index:int = -1) -> None:
        self.index = index

class Table(metaclass=abc.ABCMeta):
    def __init__(self, table_name:str) -> None:
        self.table_name = table_name
        
    def new_row(self) -> Row:
        row = Row()
        for variable_name in self.__dict__:
            variable = self.__dict__[variable_name]
            if isinstance(variable, Column):
                setattr(row, variable_name, None)
        return row
    
    def get_row(self, data:tuple) -> Row:
        '''
        Parameter
        -
        data (tuple) : row data. ex) row = connector.select()
        '''
        index = 0
        row = Row()
        for variable_name in self.__dict__:
            variable = self.__dict__[variable_name]
            if isinstance(variable, Column):
                if data and index<len(data):
                    setattr(row, variable_name, data[index])
                else:
                    setattr(row, variable_name, None)
                index += 1
        return row
        