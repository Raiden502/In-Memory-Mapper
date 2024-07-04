from typing import Optional, Dict, List, Any, Callable

class Field:
    
    def __init__(self, type: Any, value: Any = None, pk: bool = False, nullable: bool = False, unique: bool = False):
        self.type:Any = type
        self.value:Any = value
        self.pk:bool = pk
        self.nullable:bool = nullable
        self.unique:bool = unique

    def get_default(self) -> Any:
        return self.value() if callable(self.value) else self.value
    
    def set_value(self, val) -> Any:
        if type(val) == self.type:
            setattr(self, 'value', val)