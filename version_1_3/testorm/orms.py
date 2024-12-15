from typing import List, Any, Dict, Tuple
from .resultant_set import ResultantSet
from .database import Database, Table

class select:
    
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    INNER = 'INNER'
    
    def __init__(self, *fields, model = None):
        self.__model = model
        self.__field :List[Any] = fields
        self.__conditions :List[Any] = []
        self.__orderByCond :List[Any] = []
        self.__groupByCond :List[Any] = []
        self.__db :Database = Database()
        self.__onCondition = None
        self.__joinModel = None
        self.__joinType = None
    
    def model(self, model = None):
        '''
            select the model to be fetched
        '''
        self.__model = model
        return self
        
    def filter(self, *conditions) -> 'select':
        '''
            filters the data based on condition
        '''
        self.__conditions = conditions
        return self
    
    def sort(self, *conditions: List[Any]):
        '''
            sort the data based on conditions
        '''
        self.__orderByCond = conditions
        return self
    
    def cluster(self, *conditions: List[Any]) -> 'select':
        '''
            clusters the data based on conditions
        '''
        self.__groupByCond = conditions
        return self
    
    def join(self, model=None, condition=None, type = None):
        self.__joinModel = model
        self.__onCondition = condition
        self.__joinType = type or self.INNER
        
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
        
        if joinType == self.LEFT:
            for obj in currData:
                condition_passed = False
                for joinObj in joinData:
                    if onCondition(currHeaders, obj, joinHeaders, joinObj):
                        tempTuple = obj+joinObj
                        tempData.append(tempTuple)
                        condition_passed = True
                        
                if not condition_passed:
                    tempTuple = obj+([None]*len(joinObj))
                    tempData.append(tempTuple)
        
        elif joinType == self.RIGHT:
            for joinObj in joinData:
                condition_passed = False
                for obj in currData:
                    if onCondition(currHeaders, obj, joinHeaders, joinObj):
                        tempTuple = obj+joinObj
                        tempData.append(tempTuple)
                        condition_passed = True
                        
                if not condition_passed:
                    tempTuple = ([None]*len(obj))+joinObj
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
        temp_data.data = sorted(temp_data.data, key = lambda obj: (obj[temp_data.headers[field.get_name()]] for field in self.__groupByCond))
        return temp_data
    
    def __computeOrderBy(self, filteredData):
        filteredData.data = sorted(
            filteredData.data,
            key=lambda data: tuple(data[filteredData.headers[key]] for key, _ in self.__orderByCond),
            reverse=any(reverse for _, reverse in self.__orderByCond)  
        )
        return filteredData
    
    def __computeSelect(self, filteredData):
        temp_index = []
        temp_meta = {}
        temp_data = []
        
        for index, field in enumerate(self.__field):
            temp_index.append(filteredData.headers[f'{field.get_name()}'])
            temp_meta[f'{field.get_name()}'] = index
        
        
        for obj in filteredData.data:
            temp_tuple = []
            for field in temp_index:
                temp_tuple.append(obj[field])
            
            temp_data.append(temp_tuple)
        
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


class insert:
    
    def __init__(self, model = None) -> None:
        
        self.__model = model
        self.__record : Dict[str, Any] = {}
        self.__db : Database = Database()
        
        if self.__model.__classname__ not in self.__db.database:
            raise Exception(f"{self.__model.__classname__} model not migrated")

    def fields(self, rows : Dict[str, Any] = {}) ->  'insert':
        self.__record = rows
        return self
    
    def execute(self):
        headers = self.__db.database[self.__model.__classname__].headers
        tabledata = self.__db.database[self.__model.__classname__].data
        constraints = self.__db.database[self.__model.__classname__].constraints
        total = len(headers)

        for row in self.__record:
            temp = [None] * total
            for k, pos in headers.items():
                field_key = k.split('.')[1]
                temp[pos] = row.get(field_key,  constraints[f'{self.__model.__classname__}.{field_key}']['default'])
            
            tabledata.append(temp)
            

class delete:
    LEFT = 'LEFT'
    # RIGHT = 'RIGHT'
    # INNER = 'INNER'
    
    def __init__(self, *fields, model = None):
        self.__model = model
        self.__conditions :List[Any] = []
        self.__groupByCond :List[Any] = []
    
    def filter(self, *conditions) -> 'select':
        '''
            filters the data based on condition
        '''
        self.__conditions = conditions
        return self
    
    def join(self, model=None, condition=None):
        self.__joinModel = model
        self.__onCondition = condition
        self.__joinType = self.INNER
        
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
        
        if joinType == self.LEFT:
            for obj in currData:
                condition_passed = False
                for joinObj in joinData:
                    if onCondition(currHeaders, obj, joinHeaders, joinObj):
                        tempTuple = obj+joinObj
                        tempData.append(tempTuple)
                        condition_passed = True
                        
                if not condition_passed:
                    tempTuple = obj+([None]*len(joinObj))
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
    
    def execute(self):
        currentTable = self.__db.database[self.__model.__classname__]
        filteredData = ResultantSet(currentTable.data, currentTable.headers)
        
        if self.__joinModel:
            joinTable = self.__db.database[self.__joinModel.__classname__]
            filteredData = self.__computeJoins(filteredData, joinTable, self.__joinType, self.__onCondition)
        
        if self.__conditions:
            filteredData = self.__computeFilters(filteredData)


class update:
    LEFT = 'LEFT'
    # RIGHT = 'RIGHT'
    # INNER = 'INNER'
    
    def __init__(self, *fields, model = None):
        self.__model = model
        self.__conditions :List[Any] = []
        self.__groupByCond :List[Any] = []
    
    def filter(self, *conditions) -> 'select':
        '''
            filters the data based on condition
        '''
        self.__conditions = conditions
        return self
    
    def join(self, model=None, condition=None):
        self.__joinModel = model
        self.__onCondition = condition
        self.__joinType = self.INNER
        
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
        
        if joinType == self.LEFT:
            for obj in currData:
                condition_passed = False
                for joinObj in joinData:
                    if onCondition(currHeaders, obj, joinHeaders, joinObj):
                        tempTuple = obj+joinObj
                        tempData.append(tempTuple)
                        condition_passed = True
                        
                if not condition_passed:
                    tempTuple = obj+([None]*len(joinObj))
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
    
    def execute(self):
        currentTable = self.__db.database[self.__model.__classname__]
        filteredData = ResultantSet(currentTable.data, currentTable.headers)
        
        if self.__joinModel:
            joinTable = self.__db.database[self.__joinModel.__classname__]
            filteredData = self.__computeJoins(filteredData, joinTable, self.__joinType, self.__onCondition)
        
        if self.__conditions:
            filteredData = self.__computeFilters(filteredData)