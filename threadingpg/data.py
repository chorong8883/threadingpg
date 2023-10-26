import abc

class ColumnList(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        pass

class Column(metaclass=abc.ABCMeta):
    def __init__(self, 
                 data_type: str,
                #  precision: int = None,
                #  scale: int = None,
                #  type_code: int = None,
                 is_nullable:bool = True,
                 is_unique:bool = False,
                 is_primary_key:bool = False,
                 ) -> None:
        '''
        Column Data by 'Column' of psycopg2 and 'information_schema.columns' of postgresql.\n
        Parameter
        -
        data_type (str): based on query. threadingpg.datatype
        '''
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
        # self.precision = precision
        # self.scale = scale
        # self.type_code = type_code
        self.precision = None
        self.scale = None
        self.type_code = None
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
    
    def append_reference(self, column):
        if isinstance(column, Column):
            TypeError("column should be 'threadingpg.data.Column' type")
        self.references.append(column)
        
    def remove_reference(self, column):
        if isinstance(column, Column):
            TypeError("column should be 'threadingpg.data.Column' type")
        self.references.remove(column)
        
class Row(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        '''
        Abstract Class(metaclass=abc.ABCMeta)\n
        set variable name = column name\n
        '''
        pass
    def set_data(self, column_name_list:list[str], row_data:tuple):
        '''
        Parameter
        -
        column_name_list (list[str]):
        row_data (tuple):
        '''
        for index, column_name in enumerate(column_name_list):
            if column_name in self.__dict__:
                setattr(self, column_name, row_data[index])

class Table(metaclass=abc.ABCMeta):
    table_name:str = None
    def __init__(self, table_name:str=None) -> None:
        '''
        Abstract Class(metaclass=abc.ABCMeta)\n
        Declare 'table_name(str)'\n
        Define columns.\n
        Should be same variable name in this class and column name in postgresql.\n 
        usage:
        -
        class MyTable(threadingpg.data.Table):\n
            table_name="ref"\n
            index = threadingpg.data.Column(data_type=datatype.serial)\n
            name = threadingpg.data.Column(data_type=datatype.varchar())\n
            
        class MyTable(threadingpg.data.Table):\n
            def __init__(self) -> None:
                self.index = threadingpg.data.Column(data_type=datatype.serial)\n
                self.name = threadingpg.data.Column(data_type=datatype.varchar())\n
                super().__init__("ref") # important position. set self.index.name = 'index'
        '''
        if not self.table_name and table_name:
            self.table_name = table_name
        for variable_name in dir(self):
            if variable_name not in self.__dict__:
                variable = getattr(self, variable_name)
                if isinstance(variable, Column):
                    setattr(self, variable_name, variable)
                    variable.table_name = self.table_name
                    variable.name = variable_name
                    
            elif isinstance(self.__dict__[variable_name], Column):
                self.__dict__[variable_name].table_name = self.table_name
                self.__dict__[variable_name].name = variable_name
                
                    
        