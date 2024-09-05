from typing import List, Any, Dict, Tuple
from .database import Database, Table, ResultantSet

class SelectOrm:
    
    def __init__(self, model = None):
        self.__model = model
        self.__field :List[Any] = []
        self.__conditions :List[Any] = []
        self.__order_by_cond :List[Any] = []
        self.__group_by_cond :List[Any] = []
        self.__filtered :List[Any] = []
        self.__db :Database = Database()

    def select(self, *field) -> 'SelectOrm':
        self.__field = field
        return self
    
    def table(self, model = None):
        self.__model = model
        return self
        
    def where(self, conditions: List[Any] = []) -> 'SelectOrm':
        self.__conditions = conditions
        return self
    
    def order_by(self, conditions: List[Any] = []):
        self.__order_by_cond = conditions
        return self
    
    def group_by(self, conditions: List[Any] = []) -> 'SelectOrm':
        self.__group_by_cond = conditions
        return self
    
    def execute(self):
        table = self.__db.database[self.__model.__classname__]
        tableData = table.tabledata
        headers = table.tableheaders
        filtered_data = ResultantSet(tableData, headers)
        
        if self.__conditions:         
            temp_data = ResultantSet([], headers) # where conditions
            for data in filtered_data.data:
                check = all([callback(headers, data) for callback in self.__conditions])
                if(check):
                    temp_data.data.append(data)
            filtered_data = temp_data
        
        if self.__order_by_cond:   
            # order by
            filtered_data.data.sort(
                key=lambda data: tuple(key(filtered_data.__meta__, data) for key, _ in self.__order_by_cond),
                reverse=any(reverse for _, reverse in self.__order_by_cond)
            )
        
        if self.__group_by_cond:
            temp_data = ResultantSet(filtered_data.data, headers)
            temp_data.data = sorted(temp_data.data, key = lambda obj: tuple(obj[headers[field.name]] for field in self.__group_by_cond))
            filtered_data = temp_data
        

        if self.__field:
            
            temp_index = []
            temp_meta = {}
            temp_data = []
            
            for index, field in enumerate(self.__field):
                temp_index.append(filtered_data.__meta__[f'{field.name}'])
                temp_meta[f'{field.name}'] = index
            
            
            for obj in filtered_data.data:
                
                temp_tuple = []
                for field in temp_index:
                    temp_tuple.append(obj[field])
                
                temp_data.append(tuple(temp_tuple))
            
            filtered_data.__meta__ = temp_meta
            filtered_data.data = temp_data
        
        return filtered_data

        

class InsertOrm:
    
    def __init__(self, model = None) -> None:
        self.__model = model
        self.__record : Dict[str, Any] = {}
        self.__db : Database = Database()
    
    def insert(self, model = None):
        if model:
            self.__model = model
            
            if self.__model.__classname__ not in self.__db.database:
                raise Exception(f"{self.__model.__classname__} model not migrated")
        
        return self

    def fields(self, rows : Dict[str, Any] = {}) ->  'InsertOrm':
        self.__record = rows
        return self
    
    def execute(self):
        headers = self.__db.database[self.__model.__classname__].tableheaders
        tabledata = self.__db.database[self.__model.__classname__].tabledata
        total = len(headers)
        
        for row in self.__record:
            temp = [None] * total
            for k, pos in headers.items():
                temp[pos] = row[k.split('.')[1]]
            
            tabledata.append(tuple(temp))