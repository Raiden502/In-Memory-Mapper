import json
import os
from typing import Dict, Any

from .metaclass import DatabaseMeta
from ..tables import Field



class ReplQ(object, metaclass = DatabaseMeta):
    
    def __init__(self) -> None:
        if not hasattr(self, '_initialized'):
            self.db = {
                "name": "library",
                "user" : {
                    "name":"dummy",
                    "password":"dummy",
                },
                "tables":{
                    
                },
                "data":{
                    
                }
            }
    
    def serialize_field(self, field: Field) -> Dict[str, Any]:
        return {
            'type': field.type,
            'default_value': field.default_value,
            'pk': field.pk,
            'nullable': field.nullable,
            'unique': field.unique,
        }
    
    def model_init(self, Model):
        table = {}
        tableProperties = Model.__dict__
        table['__tablename__'] = tableProperties.get('__tablename__')
        table['__tabledesc__'] = tableProperties.get('__tabledesc__')
        table['__version__'] = tableProperties.get('__version__')

        properties = {}
        for field_name, field_value in tableProperties.items():
            if isinstance(field_value, Field):
                properties[field_name] = self.serialize_field(field_value)

        table['schema'] = properties
        self.db['tables'][table['__tablename__']] = table
        self.db['data'][table['__tablename__']] = []
        
    def upgrade(self):
        print("-->")
        json_object = json.dumps(self.db, indent=4)
        
        data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
        os.makedirs(data_dir, exist_ok=True)
        
        # Writing to schema.json in the data directory
        schema_path = os.path.join(data_dir, "schema.json")
        with open(schema_path, "w") as outfile:
            outfile.write(json_object)
        
    
    def getDb(self):
        return self.db