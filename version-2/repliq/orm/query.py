
from ..tables import Field


class Orm:
    
    def __init__(self) -> None:
        self.properties = {}
        self.Model = None
    
    def model(self, model):
        self.Model = model
        tableProperties = model.__dict__        
        for field_name, field_value in tableProperties.items():
            if isinstance(field_value, Field):
                self.properties[field_name] = self.serialize_field(field_value)
        
        return self
    
    def select(self, **kwargs):
        fields = kwargs
        