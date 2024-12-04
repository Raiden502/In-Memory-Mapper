from typing import Any
from .metaclass import TableMeta
from .fields import Field

class Model(metaclass = TableMeta):

    def __init__(self) -> None:
        self.fields = {}
        
    def __init__(self, **kwargs: dict[str, Any]) -> None:
        for field_name, field_value in self.__class__.__dict__.items():
            if isinstance(field_value, Field):
                field_copy = Field(**field_value.__dict__)
                field_copy.set_value(kwargs.get(field_name, field_copy.get_default()))
                setattr(self, field_name, field_copy)