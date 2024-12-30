from types import FunctionType
from typing import List, Any, Tuple
from ..db.database import DBEngine
from ..db.result_set import ResultantSet
from ..models.fields import Field
from ..models.model_meta import TableMeta

class select:
    
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    INNER = 'INNER'
    
    def __init__(self, *fields, model = None):
        self._db = DBEngine()
        self._model = model
        self._field :Tuple[Any, ...] = fields
        self._conditions :List[Any] = []
        self._cluster_conditions :List[Any] = []
        self._sort :List[Any] = []
        self._cluster :List[Any] = []
        self._joinon = None
        self._join_model = None
        self._join_type = None
    
    def model(self, model = None):
        """
            select the model to be fetched
        """
        self._model = model
        return self
        
    def filter(self, *conditions) -> 'select':
        """
            filters the data based on condition
        """
        self._conditions = conditions
        return self
    
    def sort(self, *conditions: List[Any]):
        """
            sort the data based on conditions
        """
        self._sort = conditions
        return self
    
    def cluster(self, *conditions: List[Any]) -> 'select':
        """
            clusters the data based on conditions
        """
        self._cluster = conditions
        return self
    
    def filter_cluster(self, *conditions) -> 'select':
        """
            filters the data based on condition
        """
        self._cluster_conditions = conditions
        return self
    
    def join(self, model=None, condition=None, type = None):
        self._join_model = model
        self._joinon = condition
        self._join_type = type or self.INNER
        
        if self._join_model is None or self._joinon is None:
            raise ValueError('joinModel and on_condition must be set')

        return self
        
    def __compute_joins(self, current_table, join_table, join_type, on_condition):
        join_data = join_table.data
        current_data = current_table.data
        join_headers = join_table.columns
        curr_headers = current_table.columns
        temp_data = []
        temp_headers ={}
        
        if join_type == self.LEFT:
            for obj in current_data:
                condition_passed = False
                for join_obj in join_data:
                    if on_condition(curr_headers, obj, join_headers, join_obj):
                        temp_tuple = obj+join_obj
                        temp_data.append(temp_tuple)
                        condition_passed = True
                        
                if not condition_passed:
                    temp_tuple = obj+([None]*len(join_obj))
                    temp_data.append(temp_tuple)
        
        elif join_type == self.RIGHT:
            for join_obj in join_data:
                condition_passed = False
                for obj in current_data:
                    if on_condition(curr_headers, obj, join_headers, join_obj):
                        temp_tuple = obj+join_obj
                        temp_data.append(temp_tuple)
                        condition_passed = True
                        
                if not condition_passed:
                    temp_tuple = ([None]*len(obj))+join_obj
                    temp_data.append(temp_tuple)
                    
        else:
            for obj in current_data:
                for join_obj in join_data:
                    if on_condition(curr_headers, obj, join_headers, join_obj):
                        temp_tuple = obj+join_obj
                        temp_data.append(temp_tuple)
            
        index = 0
        for k in curr_headers.keys():
            temp_headers[k] = index
            index+=1

        for k in join_headers.keys():
            temp_headers[k] = index
            index+=1

        return ResultantSet(temp_data, temp_headers)
    
    def __compute_filters(self, filtered_data):
        temp_data = []
        for item in filtered_data.data:
            if all([callback(filtered_data.columns, item) for callback in self._conditions]):
                temp_data.append(item)
        
        return ResultantSet(temp_data, filtered_data.columns)
    
    def __compute_cluster_filters(self, filtered_data):
        temp_data = []
        for item in filtered_data.data:
            if all([callback(filtered_data.columns, item) for callback in self._cluster_conditions]):
                temp_data.append(item)
        
        return ResultantSet(temp_data, filtered_data.columns)
    
    def __compute_cluster(self, filtered_data):
        temp_data = ResultantSet(filtered_data.data, filtered_data.columns)
        temp_data.data = sorted(temp_data.data, key = lambda obj: tuple(obj[temp_data.columns[field._name]] for field in self._cluster))
        return temp_data
    
    def __compute_sortby(self, filtered_data):
        filtered_data.data = sorted(
            filtered_data.data,
            key=lambda data: tuple(data[filtered_data.columns[key]] for key, _ in self._sort),
            reverse=any(reverse for _, reverse in self._sort)  
        )
        return filtered_data
    
    def __compute_fields(self, filtered_data):
        temp_meta = {}
        temp_data = []
        
        if len(self._field)==0:
            return filtered_data
        
        for index, field in enumerate(self._field):
            if isinstance(field, Field):
                temp_meta[field._name]= filtered_data.columns[field._name]
            
            elif isinstance(field, FunctionType):
                temp_meta[field(type='name')] = index

            elif isinstance(field, str):
                temp_meta[field] = index

        for obj in filtered_data.data:
            temp_tuple = []
            
            for field in self._field:
                
                if isinstance(field, Field):
                    temp_tuple.append(obj[filtered_data.columns[field._name]])
                
                elif isinstance(field, FunctionType):
                    temp_tuple.append(field(columns=filtered_data.columns, data = obj, type='value'))
                
                elif isinstance(field, str):
                    temp_tuple.append(obj[filtered_data.columns[field]])
            
            temp_data.append(temp_tuple)
    
        filtered_data.columns = temp_meta
        filtered_data.data = temp_data

        return filtered_data
    
    def execute(self):
        filtered_data = None
        if isinstance(self._model, TableMeta):
            current_table = self._db.database[self._model.__classname__]
            filtered_data = ResultantSet(current_table.data, current_table.columns)
            
        if isinstance(self._model, ResultantSet):
            filtered_data = self._model
        
        if self._join_model:
            join_table = self._db.database[self._join_model.__classname__]
            filtered_data = self.__compute_joins(filtered_data, join_table, self._join_type, self._joinon)
        
        if self._conditions:
            filtered_data = self.__compute_filters(filtered_data)
        
        if self._cluster:
            filtered_data = self.__compute_cluster(filtered_data)
            
        if self._cluster_conditions:
            filtered_data = self.__compute_cluster_filters(filtered_data)
        
        if self._sort:
            filtered_data = self.__compute_sortby(filtered_data)
            

        filtered_data = self.__compute_fields(filtered_data)

        return filtered_data