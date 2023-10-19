from . import pgclass
from . import functions

class Condition():
    conditions = None
    condition_type:str = ""
    column_name:str = ""
    value = None
    def parse(self) -> str:
        return ""

class Equal(Condition):
    def __init__(self, column:pgclass.Column, value) -> None:
        self.condition_type = "="
        self.column_name = column.column_name
        self.value = value
    def parse(self) -> str:
        return f"{self.column_name} {self.condition_type} {functions.convert_value_to_query(self.value)}"

class Greater(Condition):
    def __init__(self, column:pgclass.Column, value) -> None:
        self.condition_type = ">"
        self.column_name = column.column_name
        self.value = value
    def parse(self) -> str:
        return f"{self.column_name} {self.condition_type} {functions.convert_value_to_query(self.value)}"
        
class And(Condition):
    def __init__(self, *conditions:Condition) -> None:
        if not isinstance(conditions, tuple):
            raise TypeError("Parameter Type should be 'Condition tuple'")
        if len(conditions) < 2:
            raise ValueError("Must be '1 < Condition list length'")
        self.condition_type = "AND"
        self.conditions = conditions
        
    def parse(self) -> str:
        result = ""
        for c in self.conditions:
            result += f"{c.parse()} {self.condition_type} "
        slice_length = -(len(self.condition_type)+2)
        return f"({result[:slice_length] if result else ''})"
        
class Or(Condition):
    def __init__(self, *conditions:Condition) -> None:
        if not isinstance(conditions, tuple):
            raise TypeError("Parameter Type should be 'Condition tuple'")
        if len(conditions) < 2:
            raise ValueError("Must be '1 < Condition list length'")
        self.condition_type = "OR"
        self.conditions = conditions
        
    def parse(self) -> str:
        result = ""
        for c in self.conditions:
            result += f"{c.parse()} {self.condition_type} "
        slice_length = -(len(self.condition_type)+2)
        return f"({result[:slice_length] if result else ''})"
    
