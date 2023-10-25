import abc
from . import data
from . import query

class Condition(metaclass=abc.ABCMeta):
    conditions = None
    condition_type:str = ""
    column_name:str = ""
    value = None
    def parse(self) -> str:
        return ""

class Equal(Condition):
    def __init__(self, column:data.Column, value) -> None:
        self.condition_type = "="
        self.column_name = column.name
        self.value = value
    def parse(self) -> str:
        return f"{self.column_name} {self.condition_type} {query.convert_value_to_query(self.value)}"

class Greater(Condition):
    def __init__(self, column:data.Column, value) -> None:
        self.condition_type = ">"
        self.column_name = column.name
        self.value = value
    def parse(self) -> str:
        return f"{self.column_name} {self.condition_type} {query.convert_value_to_query(self.value)}"
        
class And(Condition):
    def __init__(self, *conditions:Condition) -> None:
        if not isinstance(conditions, tuple):
            raise TypeError("Parameter Type should be 'Condition list(tuple)'")
        if len(conditions) < 2:
            raise ValueError("Should be '1 < len(conditions)'")
        self.is_orderby = False
        for c in conditions:
            if isinstance(c, OrderBy):
                self.is_orderby = True
        if self.is_orderby:
            self.condition_type = ","
        else:
            self.condition_type = "AND"
        self.conditions = conditions
        
    def parse(self) -> str:
        result = ""
        for c in self.conditions:
            if self.is_orderby:
                result += f"{c.parse()}{self.condition_type} "
            else:
                result += f"{c.parse()} {self.condition_type} "
        
        if self.is_orderby:
            slice_length = -(len(self.condition_type)+1)
            result = f"{result[:slice_length] if result else ''}"
        else:
            slice_length = -(len(self.condition_type)+2)
            result = f"({result[:slice_length] if result else ''})"
        return result
        
class Or(Condition):
    def __init__(self, *conditions:Condition) -> None:
        if not isinstance(conditions, tuple):
            raise TypeError("Parameter Type should be 'Condition list(tuple)'")
        if len(conditions) < 2:
            raise ValueError("Should be '1 < Condition list length'")
        self.condition_type = "OR"
        self.conditions = conditions
        
    def parse(self) -> str:
        result = ""
        for c in self.conditions:
            result += f"{c.parse()} {self.condition_type} "
        slice_length = -(len(self.condition_type)+2)
        return f"({result[:slice_length] if result else ''})"
    
class OrderBy(Condition):
    def __init__(self, column:data.Column, is_desc:bool = False) -> None:
        self.condition_type = " "
        self.column_name = column.name
        self.value = 'DESC' if is_desc else ''
    def parse(self) -> str:
        return f"{self.column_name}{self.condition_type}{self.value}"