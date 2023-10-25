import abc

class ColumnList(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        pass

class Column(metaclass=abc.ABCMeta):
    def __init__(self, 
                 data_type:str,
                 precision: int = None,
                 scale: int = None,
                 type_code: int = None,
                 is_nullable:bool = True,
                 is_unique:bool = False,
                 is_primary_key:bool = False,
                 ) -> None:
        self.table_catalog = ""
        self.table_schema = ""
        self.table_name = ""
        
        self.name = ""
        # self.ordinal_position
        # self.column_default
        self.is_nullable = is_nullable
        self.is_primary_key = is_primary_key
        self.references:list[Column] = []
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
    
    def add_reference(self, column):
        if isinstance(column, Column):
            TypeError("column should be 'data.Column' type")
        self.references.append(column)
        
                 

        
class Row(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        pass
        

class Table(metaclass=abc.ABCMeta):
    table_name:str = None
    def __init__(self) -> None:
        for variable_name in dir(self):
            if variable_name not in self.__dict__:
                variable = getattr(self, variable_name)
                if isinstance(variable, Column):
                    setattr(self, variable_name, variable)
                    variable.table_name = self.table_name
                    variable.name = variable_name
        
    def convert_row(self, index_by_column_name:dict, data:tuple) -> Row:
        '''
        Parameter
        -
        data (tuple) : row data
        '''
        row = Row()
        for variable_name in self.__dict__:
            variable = self.__dict__[variable_name]
            if isinstance(variable, Column):
                index = index_by_column_name[variable_name]
                setattr(row, variable_name, data[index])
        return row
        