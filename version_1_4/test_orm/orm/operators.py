import re
from typing import List, Any, Dict, Tuple
from ..table.fields import Field
from ..db.result_set import ResultantSet



# def eq_(field: Any, value: Any):
#     '''
#         takes field and value and performs equal operations with the coresponding field
#     '''
    
#     def callback(headers, data):
#         if isinstance(value, ResultantSet):
#             if len(value.data) > 1:
#                 raise Exception("Compares only with signle record")
        
#             if len(value.data[0]) > 1:
#                 raise Exception("Compares only with one column")
            
#             return data[headers[field._name]] == value.data[0][0]
        
#         return data[headers[field._name]] == value
    
#     return callback

def or_(*conditions):
    '''
        takes mutliple conditions and performs or operations with the coresponding callbacks
    '''
    
    def callback(headers, data):
        
        return any([callback(headers, data) for callback in conditions])
    
    return callback


# def ne_(field:Any, value:Any):
#     '''
#         takes field and value and performs not equal operations with the coresponding field
#     '''
        
#     def callback(headers, data):
#         if isinstance(value, ResultantSet):
#             if len(value.data) > 1:
#                 raise Exception("Compares only with signle record")
            
#             if len(value.data[0]) > 1:
#                 raise Exception("Compares only with one column")
            
#             return data[headers[field._name]] != value.data[0][0]
        
#         return data[headers[field._name]] != value
    
#     return callback

# def lt_(field:Any, value:Any):
#     '''
#         takes field and value and performs lesser than operations with the coresponding field
#     '''

#     def callback(headers, data):
#         if isinstance(value, ResultantSet):
#             if len(value.data) > 1:
#                 raise Exception("Compares only with signle record")
            
#             if len(value.data[0]) > 1:
#                 raise Exception("Compares only with one column")
            
#             return data[headers[field._name]] < value.data[0][0]
        
#         return data[headers[field._name]] < value
    
#     return callback

# def le_(field:Any, value:Any):
#     '''
#         takes field and value and performs lesser than equals operations with the coresponding field
#     '''
    
#     def callback(headers, data):
#         if isinstance(value, ResultantSet):
#             if len(value.data) > 1:
#                 raise Exception("Compares only with signle record")
            
#             if len(value.data[0]) > 1:
#                 raise Exception("Compares only with one column")
            
#             return data[headers[field._name]] <= value.data[0][0]
    
#         return data[headers[field._name]] <= value
    
#     return callback

# def gt_(field:Any, value:Any):
#     '''
#         takes field and value and performs greater than operations with the coresponding field
#     '''
    
#     def callback(headers, data):
#         if isinstance(value, ResultantSet):
#             if len(value.data) > 1:
#                 raise Exception("Compares only with signle record")
            
#             if len(value.data[0]) > 1:
#                 raise Exception("Compares only with one column")
            
#             return data[headers[field._name]] > value.data[0][0]
        
#         return data[headers[field._name]] > value
    
#     return callback

# def ge_(field:Any, value:Any):
#     '''
#         takes field and value and performs greater than equals operations with the coresponding field
#     '''
    
#     def callback(headers, data):
#         if isinstance(value, ResultantSet):
            
#             if len(value.data) > 1:
#                 raise Exception("Compares only with signle record")
            
#             if len(value.data[0]) > 1:
#                 raise Exception("Compares only with one column")
            
#             return data[headers[field._name]] >= value.data[0][0]
        
#         return data[headers[field._name]] >= value
    
#     return callback

# def in_(field:Any, conditions:List[Any]=[]):
#     '''
#         takes field and value and performs in operations which checks for multple matchs with the coresponding field
#     '''
    
#     def callback(headers, data):
#         if isinstance(conditions, ResultantSet):
#             return data[headers[field._name]] in [ obj[0] for obj in conditions.data]
        
#         return data[headers[field._name]] in conditions
    
#     return callback

# def sw_(field:Any, value:Any, startIdx = None, endIdx=None, ilike = False):
    
#     '''
#         takes field and value and performs in operations which checks for multple matchs with the coresponding field
#     '''
    
#     def callback(headers, data):    
#         if startIdx!=None and endIdx!=None:
#             if ilike:
#                 return data[headers[field._name]].lower().startswith(value.lower(), startIdx, endIdx)

#             return data[headers[field._name]].startswith(value, startIdx, endIdx)
        
#         if ilike:
#             return data[headers[field._name]].lower().startswith(value)
        
#         return data[headers[field._name]].startswith(value)
    
#     return callback


# def ew_(field, value, startIdx = None, endIdx=None, ilike = False):

#     '''
#         takes field and value and performs in operations which checks for multple matchs with the coresponding field
#     '''
    
#     def callback(headers, data):
#         if startIdx!=None and endIdx!=None:
#             if ilike:
#                 return data[headers[field._name]].lower().endswith(value.lower(), startIdx, endIdx)

#             return data[headers[field._name]].endswith(value, startIdx, endIdx)
        
#         if ilike:
#             return data[headers[field._name]].lower().endswith(value)
        
#         return data[headers[field._name]].endswith(value)
    
#     return callback


# def reg_(field, value, ilike=False):
#     '''
#         takes field and value and performs in operations which checks for multple matchs with the coresponding field
#     '''
    
#     def callback(headers, data):
#         if ilike:
#             return re.search(value.lower(), data[headers[field._name]].lower())
        
#         return re.search(value, data[headers[field._name]])
    
#     return callback



# def btw_(field, start, end):
#     '''
#         takes field and value and performs in operations which checks for multple matchs with the coresponding field
#     '''

    
#     def callback(headers, data):
#         return data[headers[field._name]] in range(start, end)

#     return callback


# def asc_(field):

#     return (lambda headers, data: data[headers[field._name]], False)

# def desc_(field):
        
#     return (lambda headers, data: data[headers[field._name]], True)


def on_(f1, f2, operator):
    
    if not isinstance(f1, Field) or not isinstance(f2, Field):
        raise ValueError("values should be instance of field")
    
    def callback(h1, d1, h2, d2):
        if operator=='=':
            return d1[h1[f1._name]] == d2[h2[f2._name]]
        
        elif operator=='<=':
            return d1[h1[f1._name]] <= d2[h2[f2._name]]
        
        elif operator == '>=':
            return d1[h1[f1._name]] >= d2[h2[f2._name]]
        
        elif operator == '<':
            return d1[h1[f1._name]] < d2[h2[f2._name]]
        
        elif operator == '>':
            return d1[h1[f1._name]] > d2[h2[f2._name]]

        elif operator == '!=':
            return d1[h1[f1._name]] != d2[h2[f2._name]]

        else:
            raise ValueError("Operator not defined for on condition")
    
    return callback
        