from typing import Dict, Any
import uuid
from copy import deepcopy
from .fields import Field
from .table_metaclass import TableMeta

class Model(metaclass = TableMeta):

    @classmethod
    def default_uniqueId(cls) -> int:
        return uuid.uuid4().int & ((1 << 31) - 1)
    
    def __init__(self, **kwargs: dict[str, Any]) -> None:
        for field_name, field_value in self.__class__.__dict__.items():
            if isinstance(field_value, Field):
                field_copy = Field(**field_value.__dict__)
                field_copy.set_value(kwargs.get(field_name, field_copy.get_default()))
                setattr(self, field_name, field_copy)
    

    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for field_name, field_value in self.__class__.__dict__.items():
            if isinstance(field_value, Field):
                result[field_name] = field_value.get_default()
        return result