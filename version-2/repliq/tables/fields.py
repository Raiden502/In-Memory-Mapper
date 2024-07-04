from typing import Any , Optional, List, Dict
from .types import Types

class Field:
    
    def __init__(self, type: Types, default_value: Any = None, pk: bool = False, nullable: bool = False, unique: bool = False):
        self.type:Types = type
        self.default_value = default_value if not callable(default_value) else None
        self.pk:bool = pk
        self.nullable:bool = nullable
        self.unique:bool = unique
    
    def get_default(self):
        if self.default_value_callback:
            return self.default_value_callback()
        return self.default_value

