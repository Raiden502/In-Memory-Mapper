from typing import Any
from datetime import datetime
from .fields import Field

class TableMeta(type):
    
    def __new__(cls, name, bases, dct) -> Any:
        cls_fields = {k: v for k, v in dct.items() if isinstance(v, Field)}

        for field_name, field_value in cls_fields.items():
            if isinstance(field_value, Field):
                dct[field_name] = field_value
            
        cls_instance = super().__new__(cls, name, bases, dct)
        cls_instance.__tablename__ = name.lower()
        cls_instance.__metadata__ = {}
        cls_instance.__version__ = 0.1
        cls_instance.__last_modified__ = datetime.now()
        cls_instance.__owner__ = None
        return cls_instance