from ..database import Replq
from ..tables import Model, Field

class Session():

    def __init__(self) -> None:
        self.db = Replq("")

    def add(self, table:Model) -> None:
        tableProperties = table.__class__.__dict__
        tableName = tableProperties['__tablename__']
        properties = {}
        for field_name, field_value in tableProperties.items():
            if isinstance(field_value, Field):
                properties[field_name] = field_value.get_default()
                
        self.db._temporaryData = {
            "name":tableName,
            "data": properties
        }
        
    def commit(self) -> None:
        self.db._tablesData[self.db._temporaryData['name']].append(self.db._temporaryData['data'])