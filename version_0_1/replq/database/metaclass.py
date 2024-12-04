from typing import Any
from datetime import datetime

class DatabaseMeta(type):
    
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

    def __new__(cls, name, bases, dct) -> Any:
        
        cls_instance = super().__new__(cls, name, bases, dct)
        cls_instance.__dbname__ =  ""
        cls_instance.__metadata__ = {}
        cls_instance.__security__ = {}
        cls_instance.__version__ = 0.1
        cls_instance.__last_modified__ = datetime.now()
        cls_instance.__owner__ = None
        return cls_instance
