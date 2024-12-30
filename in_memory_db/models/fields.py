from types import FunctionType
from typing import List, Any, Dict, Tuple
import re
import uuid
from ..db.result_set import ResultantSet
from .._exceptions import MoreColException, MoreRowsException


class Field:
    
    @staticmethod
    def unique_key():
        return str(uuid.uuid4())

    def __init__(
        self, 
        type, 
        default = None,
        nullable = False, 
        unique = False,
        pk = False
    ):
        self.desc = None
        self.asc = None
        self._name = None
        self._cls = None
        self._pk = pk
        self._type = type
        self._default = default
        self._nullable = nullable
        self._unique = unique
        self._on_update = None
        self._on_create = None
        self._on_delete = None
        
        if self._pk:
            self._unique = True
            self._nullable = False
            self._default = Field.unique_key

    def get_defaults(self, insert_value = None):

        insert_value = insert_value or self._default
        if isinstance(insert_value, FunctionType):
            insert_value = insert_value()

        if insert_value is None and self._nullable:
            return None
        
        if not isinstance(insert_value, self._type):
            raise TypeError("Value type does not match field type")
        
        return insert_value

        
    def set_name(self, cls,  value):
        self._cls = cls
        self._name = value
        self.asc = (self._name, False)
        self.desc = (self._name, True)
    
    def get_constraints(self):
        constraints = set()
        if self._nullable:
            constraints.add('NULLABLE')
        if self._unique:
            constraints.add('UNIQUE')
        
        return constraints
    
    def __add__(self, value):
        
        def callback(headers, data):
            if isinstance(value, Field):
                return data[headers[self._name]] + data[headers[value._name]]
            
            elif isinstance(value, int) or isinstance(value, float):
                return data[headers[self._name]] + value
            
            elif isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise MoreRowsException
            
                if len(value.data[0]) > 1:
                    raise MoreColException
                
                return data[headers[self._name]] + value.data[0][0]

            elif isinstance(value, FunctionType):
                return data[headers[self._name]] + value(headers, data)
            
            return NotImplemented
        
        return callback

    
    def __sub__(self, value):
        def callback(headers, data):
            if isinstance(value, Field):
                return data[headers[self._name]] - data[headers[value._name]]
            
            elif isinstance(value, int) or isinstance(value, float):
                return data[headers[self._name]] - value
            
            elif isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise MoreRowsException
            
                if len(value.data[0]) > 1:
                    raise MoreColException
                
                return data[headers[self._name]] - value.data[0][0]
            
            elif isinstance(value, FunctionType):
                return data[headers[self._name]] + value(headers, data)
            
            return NotImplemented
        
        return callback

    def __mul__(self, value):
        def callback(headers, data):

            if isinstance(value, Field):
                return data[headers[self._name]] * data[headers[value._name]]
            
            elif isinstance(value, int) or isinstance(value, float):
                return data[headers[self._name]] * value
            
            elif isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise MoreRowsException
            
                if len(value.data[0]) > 1:
                    raise MoreColException
                
                return data[headers[self._name]] * value.data[0][0]
            
            elif isinstance(value, FunctionType):
                return data[headers[self._name]] + value(headers, data)
            
            return NotImplemented
        
        return callback

    def __truediv__(self, value):
        def callback(headers, data):
            if isinstance(value, Field):
                return data[headers[self._name]] / data[headers[value._name]]
            
            elif isinstance(value, int) or isinstance(value, float):
                return data[headers[self._name]] / value
            
            elif isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise MoreRowsException
            
                if len(value.data[0]) > 1:
                    raise MoreColException
                
                return data[headers[self._name]] / value.data[0][0]
            
            elif isinstance(value, FunctionType):
                return data[headers[self._name]] + value(headers, data)
            
            return NotImplemented
        
        return callback

    def __floordiv__(self, value):
        def callback(headers, data):
            if isinstance(value, Field):
                return data[headers[self._name]] // data[headers[value._name]]
            
            elif isinstance(value, int) or isinstance(value, float):
                return data[headers[self._name]] // value
            
            elif isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise MoreRowsException
            
                if len(value.data[0]) > 1:
                    raise MoreColException
                
                return data[headers[self._name]] // value.data[0][0]
            
            elif isinstance(value, FunctionType):
                return data[headers[self._name]] + value(headers, data)
            
            return NotImplemented
        
        return callback

    def __mod__(self, value):
        def callback(headers, data):
            if isinstance(value, Field):
                return data[headers[self._name]] % data[headers[value._name]]
            
            elif isinstance(value, int) or isinstance(value, float):
                return data[headers[self._name]] % value
            
            elif isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise MoreRowsException
            
                if len(value.data[0]) > 1:
                    raise MoreColException
                
                return data[headers[self._name]] % value.data[0][0]
            
            elif isinstance(value, FunctionType):
                return data[headers[self._name]] + value(headers, data)
            
            return NotImplemented
        
        return callback

    def __pow__(self, value):
        def callback(headers, data):
            if isinstance(value, Field):
                return data[headers[self._name]] ** data[headers[value._name]]
            
            elif isinstance(value, int) or isinstance(value, float):
                return data[headers[self._name]] ** value
            
            elif isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise MoreRowsException
            
                if len(value.data[0]) > 1:
                    raise MoreColException
                
                return data[headers[self._name]] ** value.data[0][0]
            
            elif isinstance(value, FunctionType):
                return data[headers[self._name]] + value(headers, data)
            
            return NotImplemented
        
        return callback
    
    def __eq__(self, value):
        
        def callback(headers, data):
            if isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise MoreRowsException
            
                if len(value.data[0]) > 1:
                    raise MoreColException
                
                return data[headers[self._name]] == value.data[0][0]
            
            elif isinstance(value, Field):
                return data[headers[self._name]] == data[headers[value._name]]
            
            elif isinstance(value, FunctionType):
                return data[headers[self._name]] == value(headers, data)
            
            return data[headers[self._name]] == value
        
        return callback
    
    def __ne__(self, value):
        def callback(headers, data):
            if isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise MoreRowsException
                
                if len(value.data[0]) > 1:
                    raise MoreColException
                
                return data[headers[self._name]] != value.data[0][0]
            
            elif isinstance(value, Field):
                return data[headers[self._name]] != data[headers[value._name]]
            
            elif isinstance(value, FunctionType):
                return data[headers[self._name]] != value(headers, data)
            
            return data[headers[self._name]] != value
        
        return callback
    
    def __lt__(self, value):
        def callback(headers, data):
            if isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise MoreRowsException
                
                if len(value.data[0]) > 1:
                    raise MoreColException
                
                return data[headers[self._name]] < value.data[0][0]

            elif isinstance(value, Field):
                return data[headers[self._name]] < data[headers[value._name]]
            
            elif isinstance(value, FunctionType):
                return data[headers[self._name]] < value(headers, data)
            
            return data[headers[self._name]] < value
        
        return callback
    
    def __gt__(self, value):
        def callback(headers, data):
            if isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise MoreRowsException
                
                if len(value.data[0]) > 1:
                    raise MoreColException
                
                return data[headers[self._name]] > value.data[0][0]
            
            elif isinstance(value, Field):
                return data[headers[self._name]] > data[headers[value._name]]
            
            elif isinstance(value, FunctionType):
                return data[headers[self._name]] > value(headers, data)
            
            return data[headers[self._name]] > value
        
        return callback
    
    def __le__(self, value):
        def callback(headers, data):
            if isinstance(value, ResultantSet):
                if len(value.data) > 1:
                    raise MoreRowsException
                
                if len(value.data[0]) > 1:
                    raise MoreColException
                
                return data[headers[self._name]] <= value.data[0][0]
            
            elif isinstance(value, Field):
                return data[headers[self._name]] <= data[headers[value._name]]
            
            elif isinstance(value, FunctionType):
                return data[headers[self._name]] <= value(headers, data)
        
            return data[headers[self._name]] <= value
        
        return callback
    
    def __ge__(self, value):
        def callback(headers, data):
            if isinstance(value, ResultantSet):
                
                if len(value.data) > 1:
                    raise MoreRowsException
                
                if len(value.data[0]) > 1:
                    raise MoreColException
                
                return data[headers[self._name]] >= value.data[0][0]
            
            elif isinstance(value, Field):
                return data[headers[self._name]] >= data[headers[value._name]]
            
            elif isinstance(value, FunctionType):
                return data[headers[self._name]] >= value(headers, data)
            
            return data[headers[self._name]] >= value
        
        return callback
    
    def in_(self, conditions:List[Any]=[]):
        """
            takes field and value and performs in operations which checks for multple matchs with the coresponding field
        """
        
        def callback(headers, data):
            if isinstance(conditions, ResultantSet):
                return data[headers[self._name]] in [ obj[0] for obj in conditions.data]
            
            return data[headers[self._name]] in conditions
        
        return callback
    
    def sw_(self, value:Any, start_idx = None, end_idx=None, ilike = False):

        """
            takes field and value and performs in operations which checks for multple matchs with the coresponding field
        """
        
        def callback(headers, data):
            compare = data[headers[self._name]]
            value_inp = value
            if ilike:
                value_inp = value_inp.lower()
                compare = compare.lower()
                
            if start_idx is not None and end_idx is not None:
                return compare.startswith(value_inp, start_idx, end_idx)
            
            return compare.startswith(value_inp)
        
        return callback


    def ew_(self, value, start_idx = None, end_idx=None, ilike = False):

        """
            takes field and value and performs in operations which checks for multiple matches with the corresponding field
        """
        
        def callback(headers, data):
            compare = data[headers[self._name]]
            value_inp = value
            if ilike:
                value_inp = value_inp.lower()
                compare = compare.lower()
                
            if start_idx is not None and end_idx is not None:
                return compare.endswith(value_inp, start_idx, end_idx)
            
            return compare.endswith(value_inp)
        
        return callback


    def reg_(self, value, ilike=False):
        """
            takes field and value and performs in operations which checks for multiple matches with the corresponding field
        """
        
        def callback(headers, data):
            if ilike:
                return re.search(value.lower(), data[headers[self._name]].lower())
            
            return re.search(value, data[headers[self._name]])
        
        return callback



    def btw_(self, start, end):
        """
            takes field and value and performs in operations which checks for multiple matches with the corresponding field
        """

        
        def callback(headers, data):
            return data[headers[self._name]] in range(start, end)

        return callback