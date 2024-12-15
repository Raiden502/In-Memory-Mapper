from typing import Optional, Dict, Any
from .table import Table
from ..table.fields import Field

class DBEngine:
    __instance: Optional['DBEngine'] = None

    def __new__(cls, *args, **kwargs) -> 'DBEngine':
        if DBEngine.__instance == None:
            DBEngine.database = {}
            DBEngine.__instance = super(DBEngine, cls).__new__(cls, *args, **kwargs)

        return DBEngine.__instance

    def migrate(self, model):

        properties = model.__dict__
        __classname__ = properties.get('__classname__')
        
        temp_table = Table()
        temp_table.set_name(__classname__)
        
        index = 0
        for _ , field_value in properties.items():
            if isinstance(field_value, Field) and field_value._type:
                temp_table.set_column(field_value._name, index)
                temp_table.set_constraints(field_value._name, field_value.get_constraints())
                temp_table.set_headers(field_value._name, field_value._type)
                if field_value._unique:
                    temp_table.set_hashes(field_value._name)
                index +=1

        self.database[__classname__] = temp_table