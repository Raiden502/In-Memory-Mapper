from types import FunctionType
from .expression_calc import ExpressionCalculate
from ..models.fields import Field


def alias(value, name):
    
    def get_name():
        return name
    
    def get_value(**kwargs):
        if isinstance(value, Field):
            return kwargs['data'][kwargs['columns'][f'{value._name}']]
        
        if isinstance(value, FunctionType):
            return value(headers=kwargs['columns'], data=kwargs['data'])
        
        return value
    
    def callback(**kwargs):
        if kwargs['type'] == 'name':
            return get_name()
        
        return get_value(**kwargs)

    return callback


def expr(expression):
    
    obj = ExpressionCalculate()
    postfix_tokens = obj.infix_to_postfix(obj.tokenize(expression))

    def callback(headers, data):
        value = obj.calculate_postfix(postfix_tokens, data, headers)
        return value
    
    return callback

def or_(*conditions):
    '''
        takes mutliple conditions and performs or operations with the coresponding callbacks
    '''
    
    def callback(headers, data):
        
        return any([callback(headers, data) for callback in conditions])
    
    return callback

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
        