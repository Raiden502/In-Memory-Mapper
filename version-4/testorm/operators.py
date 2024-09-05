import re
from typing import List, Any, Dict, Tuple
from .schema import Field
from .database import ResultantSet


def _eq(field: Any, value: Any):
    '''
        takes field and value and performs equal operations with the coresponding field
    '''
    
    def callback(headers, data):
        
        if isinstance(value, ResultantSet):
            if len(value.data) > 1:
                raise Exception("Compares only with signle record")
        
            if len(value.data[0]) > 1:
                raise Exception("Compares only with one column")
            
            return data[headers[field.name]] == value.data[0][0]
        
        return data[headers[field.name]] == value
    
    return callback

def _or(conditions:List[Any] = []):
    '''
        takes mutliple conditions and performs or operations with the coresponding callbacks
    '''
    
    def callback(headers, data):
        
        return any([callback(headers, data) for callback in conditions])
    
    return callback


def _ne(field:Any, value:Any):
    '''
        takes field and value and performs not equal operations with the coresponding field
    '''
        
    def callback(headers, data):
        if isinstance(value, ResultantSet):
            if len(value.data) > 1:
                raise Exception("Compares only with signle record")
            
            if len(value.data[0]) > 1:
                raise Exception("Compares only with one column")
            
            return data[headers[field.name]] != value.data[0][0]
        
        return data[headers[field.name]] != value
    
    return callback

def _lt(field:Any, value:Any):
    '''
        takes field and value and performs lesser than operations with the coresponding field
    '''

    def callback(headers, data):
        if isinstance(value, ResultantSet):
            if len(value.data) > 1:
                raise Exception("Compares only with signle record")
            
            if len(value.data[0]) > 1:
                raise Exception("Compares only with one column")
            
            return data[headers[field.name]] < value.data[0][0]
        
        return data[headers[field.name]] < value
    
    return callback

def _le(field:Any, value:Any):
    '''
        takes field and value and performs lesser than equals operations with the coresponding field
    '''
    
    def callback(headers, data):
        if isinstance(value, ResultantSet):
            if len(value.data) > 1:
                raise Exception("Compares only with signle record")
            
            if len(value.data[0]) > 1:
                raise Exception("Compares only with one column")
            
            return data[headers[field.name]] <= value.data[0][0]
    
        return data[headers[field.name]] <= value
    
    return callback

def _gt(field:Any, value:Any):
    '''
        takes field and value and performs greater than operations with the coresponding field
    '''
    
    def callback(headers, data):
        if isinstance(value, ResultantSet):
            if len(value.data) > 1:
                raise Exception("Compares only with signle record")
            
            if len(value.data[0]) > 1:
                raise Exception("Compares only with one column")
            
            return data[headers[field.name]] > value.data[0][0]
        
        return data[headers[field.name]] > value
    
    return callback

def _ge(field:Any, value:Any):
    '''
        takes field and value and performs greater than equals operations with the coresponding field
    '''
    
    def callback(headers, data):
        if isinstance(value, ResultantSet):
            
            if len(value.data) > 1:
                raise Exception("Compares only with signle record")
            
            if len(value.data[0]) > 1:
                raise Exception("Compares only with one column")
            
            return data[headers[field.name]] >= value.data[0][0]
        
        return data[headers[field.name]] >= value
    
    return callback

def _in(field:Any, conditions:List[Any]=[]):
    '''
        takes field and value and performs in operations which checks for multple matchs with the coresponding field
    '''
    
    def callback(headers, data):
        if isinstance(conditions, ResultantSet):
            return data[headers[field.name]] in [ obj[0] for obj in conditions.data]
        
        return data[headers[field.name]] in conditions
    
    return callback

def _sw(field:Any, value:Any, startIdx = None, endIdx=None, ilike = False):
    
    '''
        takes field and value and performs in operations which checks for multple matchs with the coresponding field
    '''
    
    def callback(headers, data):    
        if startIdx!=None and endIdx!=None:
            if ilike:
                return data[headers[field.name]].lower().startswith(value.lower(), startIdx, endIdx)

            return data[headers[field.name]].startswith(value, startIdx, endIdx)
        
        if ilike:
            return data[headers[field.name]].lower().startswith(value)
        
        return data[headers[field.name]].startswith(value)
    
    return callback


def _ew(field, value, startIdx = None, endIdx=None, ilike = False):

    '''
        takes field and value and performs in operations which checks for multple matchs with the coresponding field
    '''
    
    def callback(headers, data):
        if startIdx!=None and endIdx!=None:
            if ilike:
                return data[headers[field.name]].lower().endswith(value.lower(), startIdx, endIdx)

            return data[headers[field.name]].endswith(value, startIdx, endIdx)
        
        if ilike:
            return data[headers[field.name]].lower().endswith(value)
        
        return data[headers[field.name]].endswith(value)
    
    return callback


def _reg(field, value, ilike=False):
    '''
        takes field and value and performs in operations which checks for multple matchs with the coresponding field
    '''
    
    def callback(headers, data):
        if ilike:
            return re.search(value.lower(), data[headers[field.name]].lower())
        
        return re.search(value, data[headers[field.name]])
    
    return callback



def _btw(field, start, end):
    '''
        takes field and value and performs in operations which checks for multple matchs with the coresponding field
    '''

    
    def callback(headers, data):
        return data[headers[field.name]] in range(start, end)

    return callback


def _asc(field):

    return (lambda headers, data: data[headers[field.name]], False)

def _desc(field):
        
    return (lambda headers, data: data[headers[field.name]], True)