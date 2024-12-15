
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
    
    def get_name(self):
        return self.__name

    def get_value(self):
        return self.__default

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