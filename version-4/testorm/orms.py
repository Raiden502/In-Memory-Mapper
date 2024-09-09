from typing import List, Any, Dict, Tuple
from .database import Database, Table, ResultantSet

class SelectOrm:
    
    def __init__(self, model = None):
        self.__model = model
        self.__field :List[Any] = []
        self.__conditions :List[Any] = []
        self.__orderByCond :List[Any] = []
        self.__groupByCond :List[Any] = []
        self.__db :Database = Database()
        self.__onCondition = None
        self.__joinModel = None
        self.__joinType = None

    def select(self, *field) -> 'SelectOrm':
        self.__field = field
        return self
    
    def table(self, model = None):
        self.__model = model
        return self
        
    def where(self, *conditions: List[Any]) -> 'SelectOrm':
        self.__conditions = conditions
        return self
    
    def order_by(self, *conditions: List[Any]):
        self.__orderByCond = conditions
        return self
    
    def group_by(self, *conditions: List[Any]) -> 'SelectOrm':
        self.__groupByCond = conditions
        return self
    
    def join(self, joinModel=None, condition=None):
        self.__joinModel = joinModel
        self.__onCondition = condition
        self.__joinType = "inner"
        
        if self.__joinModel == None or self.__onCondition == None:
            raise ValueError('joinModel and onCondition must be set')

        return self
    
    def leftJoin(self, joinModel=None, condition=None):
        self.__joinModel = joinModel
        self.__onCondition = condition
        self.__joinType = "left"
        
        if self.__joinModel == None or self.__onCondition == None:
            raise ValueError('joinModel and onCondition must be set')

        return self

    def rightJoin(self, joinModel=None, condition=None):
        self.__joinModel = joinModel
        self.__onCondition = condition
        self.__joinType = "right"
        
        if self.__joinModel == None or self.__onCondition == None:
            raise ValueError('joinModel and onCondition must be set')

        return self
    
    def execute(self):
        table = self.__db.database[self.__model.__classname__]
        tableData = table.tabledata
        headers = table.tableheaders
        filtered_data = ResultantSet(tableData, headers)
        
        if self.__joinModel:
            joinTable = self.__db.database[self.__joinModel.__classname__]
            joinTableData = joinTable.tabledata
            joinHeaders = joinTable.tableheaders
            
            currData = filtered_data.data
            currHeaders = filtered_data.headers
            
            tempData = []
            
            if self.__joinType == "left":
                for obj in currData:
                    condition_passed = False
                    for joinObj in joinTableData:
                        if self.__onCondition(currHeaders, obj, joinHeaders, joinObj):
                            tempTuple = obj+joinObj
                            tempData.append(tempTuple)
                            condition_passed = True
                            
                    if not condition_passed:
                        tempTuple = obj+tuple([None]*len(joinObj))
                        tempData.append(tempTuple)
            
            elif self.__joinType == "right":
                for joinObj in joinTableData:
                    condition_passed = False
                    for obj in currData:
                        if self.__onCondition(currHeaders, obj, joinHeaders, joinObj):
                            tempTuple = obj+joinObj
                            tempData.append(tempTuple)
                            condition_passed = True
                            
                    if not condition_passed:
                        tempTuple = tuple([None]*len(obj))+joinObj
                        tempData.append(tempTuple)
                        
            else:
                for obj in currData:
                    for joinObj in joinTableData:
                        if self.__onCondition(currHeaders, obj, joinHeaders, joinObj):
                            tempTuple = obj+joinObj
                            tempData.append(tempTuple)
                
            tempHeaders ={}
            index = 0
            for k in currHeaders.keys():
                tempHeaders[k] = index
                index+=1

            for k in joinHeaders.keys():
                tempHeaders[k] = index
                index+=1
            
            filtered_data = ResultantSet(tempData, tempHeaders)
        
        if self.__conditions:         
            temp_data = ResultantSet([], filtered_data.headers) # where conditions
            for data in filtered_data.data:
                check = all([callback(temp_data.headers, data) for callback in self.__conditions])
                if(check):
                    temp_data.data.append(data)
            filtered_data = temp_data
        
        if self.__orderByCond:   
            # order by
            filtered_data.data.sort(
                key=lambda data: tuple(key(filtered_data.headers, data) for key, _ in self.__orderByCond),
                reverse=any(reverse for _, reverse in self.__orderByCond)
            )
        
        if self.__groupByCond:
            temp_data = ResultantSet(filtered_data.data, filtered_data.headers)
            temp_data.data = sorted(temp_data.data, key = lambda obj: tuple(obj[temp_data.headers[field.name]] for field in self.__groupByCond))
            filtered_data = temp_data
        

        if self.__field:
            
            temp_index = []
            temp_meta = {}
            temp_data = []
            
            for index, field in enumerate(self.__field):
                temp_index.append(filtered_data.headers[f'{field.name}'])
                temp_meta[f'{field.name}'] = index
            
            
            for obj in filtered_data.data:
                
                temp_tuple = []
                for field in temp_index:
                    temp_tuple.append(obj[field])
                
                temp_data.append(tuple(temp_tuple))
            
            filtered_data.headers = temp_meta
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