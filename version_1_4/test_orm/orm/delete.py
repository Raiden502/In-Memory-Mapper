from typing import List, Any
from .._exceptions import NoUpdateFields
from ..db.database import DBEngine
from ..db.result_set import ResultantSet

class delete:
    
    def __init__(self, model = None):
        self._db = DBEngine()
        self._model = model
        self._conditions :List[Any] = []
        
    def filter(self, *conditions) -> 'delete':
        '''
            filters the data based on condition
        '''
        self._conditions = conditions
        return self
    
    def __compute_filters(self, data, columns):
        temp_data = set()
        for idx, item in enumerate(data):
            if(all([callback(columns, item) for callback in self._conditions])):
                temp_data.add(idx)
        
        return temp_data
    
    def execute(self):
        table_name = self._model.__classname__
        current_table = self._db.database[table_name]
        
        if self._conditions:
            filtered_id = self.__compute_filters(current_table.data, current_table.columns)
            
            temp_data = [item for idx, item in enumerate(current_table.data) if idx not in filtered_id]
            current_table.data = temp_data
        
        else:
            current_table.data = []
