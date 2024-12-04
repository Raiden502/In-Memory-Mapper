import json
import os
from typing import Dict, Any

from .metaclass import DatabaseMeta
from ..tables import Field



class ReplQ(object, metaclass = DatabaseMeta):
    
    def __init__(self) -> None:
        if not hasattr(self, '_initialized'):
            self.schema = {}
            self.storage = {}
    
    def serialize_field(self, field: Field) -> Dict[str, Any]:
        return {
            'type': field.type,
            'default_value': field.default_value,
            'pk': field.pk,
            'nullable': field.nullable,
            'unique': field.unique,
        }
    
    def model_init(self, Model):
        properties = {}
        tableProperties = Model.__dict__
        __tablename__ = tableProperties.get('__tablename__')
        
        for field_name, field_value in tableProperties.items():
            if isinstance(field_value, Field):
                properties[field_name] = self.serialize_field(field_value)

        self.schema[__tablename__] = properties
        self.storage[__tablename__] = []
        
    def upgrade(self):
        schema_object = json.dumps(self.schema, indent=4)
        storage_object = json.dumps(self.storage, indent=4)
        data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
        os.makedirs(data_dir, exist_ok=True)
        
        # Writing to schema.json in the data directory
        schema_path = os.path.join(data_dir, "schema.json")
        storage_path = os.path.join(data_dir, "storage.json")
        with open(schema_path, "w") as outfile:
            outfile.write(schema_object)
            
        with open(storage_path, "w") as outfile:
            outfile.write(storage_object)
        
    
    def getSchema(self):
        return self.schema