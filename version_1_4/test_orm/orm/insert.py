from typing import Dict, Any, List
import uuid
from ..db.database import DBEngine
from .._exceptions import ModelNotFound, UniqueConstraint

class insert:
    
    def __init__(self, model = None) -> None:
        
        self._model = model
        self.m_name = model.__classname__
        self._db = DBEngine().database
        self._field : List[Any] = []
        
        if self.m_name not in self._db:
            raise ModelNotFound(f"{self.m_name} model not migrated")

    def add(self, **field) ->  'insert':
        self._field = [field]
        return self
    
    def add_all(self, fields=[]) -> 'insert':
        self._field = fields
        return self

    def execute(self):
        columns = self._db[self.m_name].columns
        data = self._db[self.m_name].data
        unique_hashes = self._db[self.m_name].unique_hashes
        total = len(columns)
        
        model_defaults = self._model.__dict__
        
        for row in self._field:
            temp = [None] * total
            
            for cname, cidx in columns.items():
                
                fname = cname.split('.')[1]
                field = model_defaults[fname]
                fvalue = field.get_defaults(insert_value = row.get(fname))
                
                if field._unique and cname in unique_hashes:
                    if fvalue in unique_hashes[cname] :
                        raise UniqueConstraint("values are not unique")
                    unique_hashes[cname].add(fvalue)
                
                temp[cidx] = fvalue
            
            data.append(temp)

