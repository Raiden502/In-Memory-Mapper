from typing import List, Any, Dict, Tuple, Optional
from .schema import Model, Field

class Table:
    
    def __init__(self) -> None:
        self.data : List[List[Any]] = []
        self.headers : Dict[str, int] = {}
        self.properties : Dict[str, Any] = {}
        self.constraints : Dict[str, Any] = {}


class Database:

    __instance__: Optional['Database'] = None

    def __new__(cls) -> 'Database':
        if Database.__instance__ == None:
            Database.__instance__ : Database = super(Database, cls).__new__(cls)
            Database.__instance__.database : Dict[Any, Table] = {}
            
        return Database.__instance__

    def migrate(self, model:Model):

        tableProperties = model.__dict__
        __classname__ = tableProperties.get('__classname__')
        
        table = Table()
        table.properties['class'] = __classname__
        
        index = 0
        for field_name, field_value in tableProperties.items():
            if isinstance(field_value, Field):
                table.headers[field_value.get_name()] = index
                table.constraints[field_value.get_name()] = {
                    'type': field_value.type,
                    'nullable': field_value.nullable,
                    'unique': field_value.unique,
                    'default': field_value.get_value(),
                }
                index +=1
                
        
        self.database[__classname__] = table
        