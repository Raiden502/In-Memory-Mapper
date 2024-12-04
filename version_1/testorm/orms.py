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
    
    def __computeJoins(self, currentTable, joinTable, joinType, onCondition):
        joinData = joinTable.data
        currData = currentTable.data
        joinHeaders = joinTable.headers
        currHeaders = currentTable.headers
        tempData = []
        tempHeaders ={}
        
        if joinType == "left":
            for obj in currData:
                condition_passed = False
                for joinObj in joinData:
                    if onCondition(currHeaders, obj, joinHeaders, joinObj):
                        tempTuple = obj+joinObj
                        tempData.append(tempTuple)
                        condition_passed = True
                        
                if not condition_passed:
                    tempTuple = obj+tuple([None]*len(joinObj))
                    tempData.append(tempTuple)
        
        elif joinType == "right":
            for joinObj in joinData:
                condition_passed = False
                for obj in currData:
                    if onCondition(currHeaders, obj, joinHeaders, joinObj):
                        tempTuple = obj+joinObj
                        tempData.append(tempTuple)
                        condition_passed = True
                        
                if not condition_passed:
                    tempTuple = tuple([None]*len(obj))+joinObj
                    tempData.append(tempTuple)
                    
        else:
            for obj in currData:
                for joinObj in joinData:
                    if onCondition(currHeaders, obj, joinHeaders, joinObj):
                        tempTuple = obj+joinObj
                        tempData.append(tempTuple)
            
        index = 0
        for k in currHeaders.keys():
            tempHeaders[k] = index
            index+=1

        for k in joinHeaders.keys():
            tempHeaders[k] = index
            index+=1

        return ResultantSet(tempData, tempHeaders)
    
    def __computeFilters(self, filteredData):
        temp_data = []
        for item in filteredData.data:
            if(all([callback(filteredData.headers, item) for callback in self.__conditions])):
                temp_data.append(item)
        
        return ResultantSet(temp_data, filteredData.headers)
    
    def __computeGroupBy(self, filteredData):
        temp_data = ResultantSet(filteredData.data, filteredData.headers)
        temp_data.data = sorted(temp_data.data, key = lambda obj: tuple(obj[temp_data.headers[field.name]] for field in self.__groupByCond))
        return temp_data
    
    def __computeOrderBy(self, filteredData):
        filteredData.data.sort(
                key=lambda data: tuple(key(filteredData.headers, data) for key, _ in self.__orderByCond),
                reverse=any(reverse for _, reverse in self.__orderByCond)
            )
        return filteredData
    
    def __computeSelect(self, filteredData):
        temp_index = []
        temp_meta = {}
        temp_data = []
        
        for index, field in enumerate(self.__field):
            temp_index.append(filteredData.headers[f'{field.name}'])
            temp_meta[f'{field.name}'] = index
        
        
        for obj in filteredData.data:
            temp_tuple = []
            for field in temp_index:
                temp_tuple.append(obj[field])
            
            temp_data.append(tuple(temp_tuple))
        
        filteredData.headers = temp_meta
        filteredData.data = temp_data

        return filteredData
    
    def execute(self):
        currentTable = self.__db.database[self.__model.__classname__]
        filteredData = ResultantSet(currentTable.data, currentTable.headers)
        
        if self.__joinModel:
            joinTable = self.__db.database[self.__joinModel.__classname__]
            filteredData = self.__computeJoins(filteredData, joinTable, self.__joinType, self.__onCondition)
        
        if self.__conditions:
            filteredData = self.__computeFilters(filteredData)
        
        if self.__groupByCond:
            filteredData = self.__computeGroupBy(filteredData)
        
        if self.__orderByCond:
            filteredData = self.__computeOrderBy(filteredData)
            
        if self.__field:
            filteredData = self.__computeSelect(filteredData)
            
        return filteredData


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
        headers = self.__db.database[self.__model.__classname__].headers
        tabledata = self.__db.database[self.__model.__classname__].data
        total = len(headers)
        
        for row in self.__record:
            temp = [None] * total
            for k, pos in headers.items():
                temp[pos] = row[k.split('.')[1]]
            
            tabledata.append(tuple(temp))