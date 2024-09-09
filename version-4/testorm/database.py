from typing import List, Any, Dict, Tuple, Optional
from .schema import Model, Field

class ResultantSet:
    def __init__(self, data=[], meta={}) -> None:
        self.data = data
        self.headers = meta
        

class Table:
    
    def __init__(self) -> None:
        self.tabledata : List[Tuple[Any]] = []
        self.tableheaders : Dict[str, int] = {}
        self.properties : Dict[str, Any] = {}


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
                table.tableheaders[field_value.name] = index
                index +=1
        
        self.database[__classname__] = table
        