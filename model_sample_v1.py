import json
import datetime
from typing import Any , Optional, List, Dict

class Types:
    # Basic data types
    INTIGER = 'int'
    FLOAT = 'float'
    STRING = 'str'
    BOOL = 'bool'
    
    # Collection types
    LIST = 'list'
    DICT = 'dict'
    
    # Date and time types
    DATE = 'date'
    TIME = 'time'
    DATETIME = 'datetime'
    TIMEDELTA = 'timedelta'
    TZINFO = 'tzinfo'
    
    # Other built-in types
    COMPLEX = 'complex'
    NONE = 'none'
    NOTIMPLEMENTED = 'notimplemented'
    ELLIPSIS = 'ellipsis'


class Field:
    
    def __init__(self, type: Types, default_value: Any = None, pk: bool = False, nullable: bool = False, unique: bool = False):
        self.type:Types = type
        self.default_value = default_value if not callable(default_value) else None
        self.pk:bool = pk
        self.nullable:bool = nullable
        self.unique:bool = unique
    
    def get_default(self):
        if self.default_value_callback:
            return self.default_value_callback()
        return self.default_value


class TableMeta(type):
    
    def __new__(cls, name, bases, dct) -> Any:
        cls_fields = {k: v for k, v in dct.items()}
        
        for field_name, field_value in cls_fields.items():
            if isinstance(field_value, Field):
                dct[field_name] = field_value
                
        cls_instance = super().__new__(cls, name, bases, dct)
        
        if name!='Model':
            MetaData = cls_fields['Meta'].__dict__
            cls_instance.__tablename__ = MetaData.get('__tablename__').lower()
            cls_instance.__tabledesc__ = MetaData.get('__tabledesc__').lower()
            cls_instance.__version__ = MetaData.get('__version__')
            cls_instance.__last_modified__ = datetime.datetime.now()
            cls_instance.__owner__ = None
        return cls_instance
    
class Model(metaclass = TableMeta):

    def __init__(self) -> None:
        self.fields = {}
        
    def __init__(self, **kwargs: dict[str, Any]) -> None:
        for field_name, field_value in self.__class__.__dict__.items():
            if isinstance(field_value, Field):
                field_copy = Field(**field_value.__dict__)
                field_copy.set_value(kwargs.get(field_name, field_copy.get_default()))
                setattr(self, field_name, field_copy)

class User(Model):
    
    book_id: int = Field(type=Types.INTIGER, default_value="", pk=True)
    name: Optional[str] = Field(type=Types.STRING, default_value="", nullable=True)
    description: Optional[str] = Field(type=Types.STRING, default_value="", nullable=True)
    category: str = Field(type=Types.STRING, default_value="")
    tags: List[str] = Field(type=Types.LIST, default_value=list)
    status: Optional[str] = Field(type=Types.STRING, default_value="available")
    purchase_date: datetime.datetime = Field(type=Types.DATETIME, default_value=datetime.datetime.now)

    class Meta:
        __tablename__ = "dept_users"
        __tabledesc__ = "hi this is user table"
        __version__ = 1.0
        


class Database:
    
    def __init__(self) -> None:
        self.db = {
            "name": "library",
            "user" : {
                "name":"dummy",
                "password":"dummy",
            },
            "tables":{
                
            }
        }
    
    def serialize_field(self, field: Field) -> Dict[str, Any]:
        return {
            'type': field.type,
            'default_value': field.default_value,
            'pk': field.pk,
            'nullable': field.nullable,
            'unique': field.unique,
        }
    
    def model_init(self, Model):
        table = {}
        tableProperties = Model.__dict__
        table['__tablename__'] = tableProperties.get('__tablename__')
        table['__tabledesc__'] = tableProperties.get('__tabledesc__')
        table['__version__'] = tableProperties.get('__version__')

        properties = {}
        for field_name, field_value in tableProperties.items():
            if isinstance(field_value, Field):
                properties[field_name] = self.serialize_field(field_value)

        table['schema'] = properties
        self.db['tables'][table['__tablename__']] = table
        
    
    def getDb(self):
        return json.dumps(self.db, indent=4)
    
    
if __name__ == '__main__':
    db = Database()
    db.model_init(User)
    print(db.getDb())