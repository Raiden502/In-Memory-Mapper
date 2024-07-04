from typing import Any , Optional, List, Dict
from datetime import datetime

from .fields import Field


class TableMeta(type):
    
    def __new__(cls, name, bases, dct) -> Any:
        cls_fields = {k: v for k, v in dct.items()}
        
        for field_name, field_value in cls_fields.items():
            if isinstance(field_value, Field):
                dct[field_name] = field_value
                
        cls_instance = super().__new__(cls, name, bases, dct)
        
        if name!='Model':
            MetaData = cls_fields['Meta'].__dict__
            cls_instance.__tablename__ = MetaData.get('__tablename__').lower()
            cls_instance.__tabledesc__ = MetaData.get('__tabledesc__').lower()
            cls_instance.__version__ = MetaData.get('__version__')
            cls_instance.__last_modified__ = datetime.now()
            cls_instance.__owner__ = None
        return cls_instance