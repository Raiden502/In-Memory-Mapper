from .resultant_set import ResultantSet
from typing import List, Any, Dict, Tuple
import re

class Field:
    def __init__(
        self, 
        type, 
        default = None,
        nullable = False, 
        unique = False,
        check = None
    ):
        self.__name = None
        self.type = type
        self.__default = default
        self.nullable = nullable
        self.unique = unique
        self.check = check
    
    def set_value(self, value):
        self.__default = value
    
    def set_name(self, value):
        self.__name = value
        self.asc = (self.__name, False)
        self.desc = (self.__name, True)
    
    def get_name(self):
        return self.__name

    def get_value(self):
        return self.__default
    
    def __eq__(self, value):
        
        def callback(headers, data):
            if isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise Exception("Compares only with signle record")
            
                if len(value.data[0]) > 1:
                    raise Exception("Compares only with one column")
                
                return data[headers[self.__name]] == value.data[0][0]
            
            return data[headers[self.__name]] == value
        
        return callback
    
    def __ne__(self, value):
        def callback(headers, data):
            if isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise Exception("Compares only with signle record")
                
                if len(value.data[0]) > 1:
                    raise Exception("Compares only with one column")
                
                return data[headers[self.__name]] != value.data[0][0]
            
            return data[headers[self.__name]] != value
        
        return callback
    
    def __lt__(self, value):
        def callback(headers, data):
            if isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise Exception("Compares only with signle record")
                
                if len(value.data[0]) > 1:
                    raise Exception("Compares only with one column")
                
                return data[headers[self.__name]] < value.data[0][0]
            
            return data[headers[self.__name]] < value
        
        return callback
    
    def __gt__(self, value):
        def callback(headers, data):
            if isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise Exception("Compares only with signle record")
                
                if len(value.data[0]) > 1:
                    raise Exception("Compares only with one column")
                
                return data[headers[self.__name]] > value.data[0][0]
            
            return data[headers[self.__name]] > value
        
        return callback
    
    def __le__(self, value):
        def callback(headers, data):
            if isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise Exception("Compares only with signle record")
                
                if len(value.data[0]) > 1:
                    raise Exception("Compares only with one column")
                
                return data[headers[self.__name]] <= value.data[0][0]
        
            return data[headers[self.__name]] <= value
        
        return callback
    
    def __ge__(self, value):
        def callback(headers, data):
            if isinstance(value, ResultantSet):
                
                if len(value.data) > 1:
                    raise Exception("Compares only with signle record")
                
                if len(value.data[0]) > 1:
                    raise Exception("Compares only with one column")
                
                return data[headers[self.__name]] >= value.data[0][0]
            
            return data[headers[self.__name]] >= value
        
        return callback
    
    def in_(self, conditions:List[Any]=[]):
        '''
            takes field and value and performs in operations which checks for multple matchs with the coresponding field
        '''
        
        def callback(headers, data):
            if isinstance(conditions, ResultantSet):
                return data[headers[self.__name]] in [ obj[0] for obj in conditions.data]
            
            return data[headers[self.__name]] in conditions
        
        return callback
    
    def sw_(self, value:Any, startIdx = None, endIdx=None, ilike = False):
    
        '''
            takes field and value and performs in operations which checks for multple matchs with the coresponding field
        '''
        
        def callback(headers, data):
            compare = data[headers[self.__name]]
            value_inp = value
            if ilike:
                value_inp = value_inp.lower()
                compare = compare.lower()
                
            if startIdx!=None and endIdx!=None:
                return compare.startswith(value_inp, startIdx, endIdx)
            
            return compare.startswith(value_inp)
        
        return callback


    def ew_(self, value, startIdx = None, endIdx=None, ilike = False):

        '''
            takes field and value and performs in operations which checks for multple matchs with the coresponding field
        '''
        
        def callback(headers, data):
            compare = data[headers[self.__name]]
            value_inp = value
            if ilike:
                value_inp = value_inp.lower()
                compare = compare.lower()
                
            if startIdx!=None and endIdx!=None:
                return compare.endswith(value_inp, startIdx, endIdx)
            
            return compare.endswith(value_inp)
        
        return callback


    def reg_(self, value, ilike=False):
        '''
            takes field and value and performs in operations which checks for multple matchs with the coresponding field
        '''
        
        def callback(headers, data):
            if ilike:
                return re.search(value.lower(), data[headers[self.__name]].lower())
            
            return re.search(value, data[headers[self.__name]])
        
        return callback



    def btw_(self, start, end):
        '''
            takes field and value and performs in operations which checks for multple matchs with the coresponding field
        '''

        
        def callback(headers, data):
            return data[headers[self.__name]] in range(start, end)

        return callback
    
    # def asc(self):
    #     return (self.__name, False)

    # def desc(self):
    #     return (self.__name, True)


class TableMeta(type):
    
    def __new__(cls, name, bases, dct):
        cls_fields = {k: v for k, v in dct.items()}
        
        for field_name, field_value in cls_fields.items():
            if isinstance(field_value, Field):
                dct[field_name] = field_value
                field_value.set_name(f'{name}.{field_name}')
                
        cls_instance = super().__new__(cls, name, bases, dct)
        
        if name!='Model':
            MetaData = cls_fields['Meta'].__dict__
            cls_instance.__classname__ = name
            cls_instance.__version__ = MetaData.get('__version__')
            cls_instance.__last_modified__ = ""
            
        return cls_instance
        
class Model(metaclass = TableMeta):

    def __init__(self, **kwargs):
        for field_name, field_value in self.__class__.__dict__.items():
            if isinstance(field_value, Field):
                field_copy = Field(**field_value.__dict__)
                field_copy.set_value(kwargs.get(field_name, field_copy.get_value()))
                setattr(self, field_name, field_copy)