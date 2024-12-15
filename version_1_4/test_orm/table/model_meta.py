from .fields import Field

class TableMeta(type):
    
    def __new__(cls, name, bases, dct):
        cls_fields = {k: v for k, v in dct.items()}
        
        for field_name, field_value in cls_fields.items():
            if isinstance(field_value, Field):
                dct[field_name] = field_value
                field_value.set_name(name, f'{name}.{field_name}')
                
        dct['_id'] = Field(type=str, pk = True)
        dct['_id'].set_name(name, f'{name}._id')
                
        cls_instance = super().__new__(cls, name, bases, dct)
        
        if name!='Model':
            cls_instance.__classname__ = name
            
        return cls_instance
        