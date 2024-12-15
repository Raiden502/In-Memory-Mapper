from typing import Any, Dict, List
from .metaclass import DatabaseMeta
from ..tables import Model, Field


class Replq(object, metaclass = DatabaseMeta):
    
    def __init__(self, name:str) -> None:
        if not hasattr(self, '_initialized'):
            self._dbname__:str = name
            self._tablesProperties: Dict[str, Dict[str, Any]] = {}
            self._tablesData: Dict[str, List[Any]] = {}
            self._temporaryData:Dict[str, Any] = None
            self._initialized = True

    def addTable(self, table: Model) -> None:
        tableProperties = table.__dict__
        tableName = tableProperties['__tablename__']
        properties = {}
        for field_name, field_value in tableProperties.items():
            if isinstance(field_value, Field):
                properties[field_name] = field_value.__dict__
        self._tablesProperties[tableName] = properties
        self._tablesData[tableName] = []
    
    def getTableProperties(self) -> None:
        return self._tablesProperties

    def getDbProperties(self) -> None:
        return self._tablesData
    