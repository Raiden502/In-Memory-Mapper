import re
from typing import List, Any, Dict

class Database:
    
    __instance__:'Database' = None

    def __new__(cls) -> 'Database':
        
        if Database.__instance__ is None:
            Database.__instance__ = super(Database, cls).__new__(cls)
            Database.__instance__.database :Dict[Any, Any] = {}
            
        return Database.__instance__


class SelectOrm:
    
    def __init__(self, model = None):
        self.__model = model
        self.__field :List[Any] = []
        self.__conditions :List[Any] = []
        self.__order_by_cond :List[Any] = []
        self.__group_by_cond :List[Any] = []
        self.__filtered :List[Any] = []
        self.__model2 = None
        self.onJoin = [] 
        self.__db :Database = Database()

    def select(self, *field) -> 'SelectOrm':
        self.__field = field
        return self
    
    def table(self, model = None):
        self.__model = model
        return self
        
    def where(self, conditions: List[Any] = []) -> 'SelectOrm':
        self.__conditions = conditions
        return self
    
    def order_by(self, conditions: List[Any] = []):
        self.__order_by_cond = conditions
        return self
    
    def group_by(self, conditions: List[Any] = []) -> 'SelectOrm':
        self.__group_by_cond = conditions
        return self
    
    def join(self,  j1 = None, j2 = None):
        self.__model = j1
        self.__model2 = j2
        return self
        
    def on(self, conditions=[]):
        self.onJoin = conditions
        return self

    def execute(self):
        if self.__model and self.__model2 and self.onJoin:
            for obj in self.__db.database[self.__model.__tablename__]:
                for obj2 in self.__db.database[self.__model2.__tablename__]:
                    check = all([callback(obj, obj2) for callback in self.onJoin])
                    if check:
                        print(obj, obj2)
                
        
        if self.__conditions:                                                               # where conditions
            for obj in self.__db.database[self.__model.__tablename__]:
                check = all([callback(obj=obj) for callback in self.__conditions])
                if(check):
                    self.__filtered.append(obj)
        else:
            self.__filtered = self.__db.database[self.__model.__tablename__]
            
        if(self.__order_by_cond):                                                           # order by
            self.__filtered.sort(
                key=lambda obj: tuple(key(obj) for key, _ in self.__order_by_cond),
                reverse=any(reverse for _, reverse in self.__order_by_cond)
            )
        
        if (self.__group_by_cond):
            self.__filtered = sorted(self.__filtered, key = lambda obj: tuple(getattr(obj, field.name).get_value() for field in self.__group_by_cond))
            
        temp_select_data = []
        for obj in self.__filtered:
            temp = obj.__dict__
            field_data = tuple()
            
            if self.__field:
                for field in self.__field:
                    if field.name in temp:
                        field_data += (temp[field.name].get_value(), )
            else:
                for value in temp.values():
                    field_data += (value.get_value(), )
            temp_select_data.append(field_data)
            
        return temp_select_data

class InsertOrm:
    
    def __init__(self, model = None) -> None:
        self.__model = model
        self.__rows : Dict[str, Any] = {}
        self.__db : Database = Database()
    
    def insert(self, model = None):
        if model:
            self.__model = model
            
            if self.__model.__tablename__ not in self.__db.database:
                self.__db.database[self.__model.__tablename__] = []
        
        return self

    def fields(self, rows : Dict[str, Any] = {}) ->  'InsertOrm':
        self.__rows = rows
        return self
    
    def execute(self):
        for row in self.__rows:
            obj : Any = self.__model(**row)
            self.__db.database[self.__model.__tablename__].append(obj)

def _eq(field: Any, value: Any):
    '''
        takes field and value and performs equal operations with the coresponding field
    '''
    def callback(obj:Any= None, obj2 = None):
        compare_value = value
        if isinstance(value, Field):
            compare_value = getattr(obj2, value.name).get_value()
        return getattr(obj, field.name).get_value() == compare_value
    
    return callback

def _or(conditions:List[Any] = []):
    '''
        takes mutliple conditions and performs or operations with the coresponding callbacks
    '''
    def callback(obj:Any= None, obj2 = None):
        if isinstance(value, Field):
            value = getattr(obj2, value.name)
            
        return any([callback(obj) for callback in conditions])
    
    return callback


def _ne(field:Any, value:Any):
    '''
        takes field and value and performs not equal operations with the coresponding field
    '''
    def callback(obj:Any= None, obj2 = None):
        if isinstance(value, Field):
            value = getattr(obj2, value.name)
            
        return getattr(obj, field.name).get_value() != value
    
    return callback

def _lt(field:Any, value:Any):
    '''
        takes field and value and performs lesser than operations with the coresponding field
    '''

    def callback(obj:Any= None, obj2 = None):
        if isinstance(value, Field):
            value = getattr(obj2, value.name)
            
        return getattr(obj, field.name).get_value() < value
    
    return callback

def _le(field:Any, value:Any):
    '''
        takes field and value and performs lesser than equals operations with the coresponding field
    '''

    def callback(obj:Any= None, obj2 = None):
        if isinstance(value, Field):
            value = getattr(obj2, value.name)
            
        return getattr(obj, field.name).get_value() <= value
    
    return callback

def _gt(field:Any, value:Any):
    '''
        takes field and value and performs greater than operations with the coresponding field
    '''

    def callback(obj:Any= None, obj2 = None):
        if isinstance(value, Field):
            value = getattr(obj2, value.name)
            
        return getattr(obj, field.name).get_value() > value
    
    return callback

def _ge(field:Any, value:Any):
    '''
        takes field and value and performs greater than equals operations with the coresponding field
    '''

    def callback(obj:Any= None, obj2 = None):
        if isinstance(value, Field):
            value = getattr(obj2, value.name)
            
        return getattr(obj, field.name).get_value() >= value
    
    return callback

def _in(field:Any, conditions:List[Any]=[]):
    '''
        takes field and value and performs in operations which checks for multple matchs with the coresponding field
    '''

    def callback(obj:Any= None, obj2 = None):
        if isinstance(value, Field):
            value = getattr(obj2, value.name)
            
        return getattr(obj, field.name).get_value() in conditions
    
    return callback

def _sw(field:Any, value:Any, startIdx = None, endIdx=None, ilike = False):
    
    '''
        takes field and value and performs in operations which checks for multple matchs with the coresponding field
    '''

    def callback(obj:Any= None, obj2 = None):
        if isinstance(value, Field):
            value = getattr(obj2, value.name)
            
        if startIdx!=None and endIdx!=None:
            if ilike:
                return getattr(obj, field.name).get_value().lower().startswith(value.lower(), startIdx, endIdx)

            return getattr(obj, field.name).get_value().startswith(value, startIdx, endIdx)
        
        if ilike:
            return getattr(obj, field.name).get_value().lower().startswith(value)
        
        return getattr(obj, field.name).get_value().startswith(value)
    
    return callback


def _ew(field, value, startIdx = None, endIdx=None, ilike = False):

    '''
        takes field and value and performs in operations which checks for multple matchs with the coresponding field
    '''

    def callback(obj:Any= None, obj2 = None):
        if isinstance(value, Field):
            value = getattr(obj2, value.name)
            
        if startIdx!=None and endIdx!=None:
            if ilike:
                return getattr(obj, field.name).get_value().lower().endswith(value.lower(), startIdx, endIdx)

            return getattr(obj, field.name).get_value().endswith(value, startIdx, endIdx)
        
        if ilike:
            return getattr(obj, field.name).get_value().lower().endswith(value)
        
        return getattr(obj, field.name).get_value().endswith(value)
    
    return callback


def _reg(field, value, ilike=False):
    '''
        takes field and value and performs in operations which checks for multple matchs with the coresponding field
    '''
    def callback(obj:Any= None, obj2 = None):
        if isinstance(value, Field):
            value = getattr(obj2, value.name)
            
        if ilike:
            return re.search(value.lower(), getattr(obj, field.name).get_value().lower())
        
        return re.search(value, getattr(obj, field.name).get_value())
    
    return callback



def _btw(field, start, end):
    '''
        takes field and value and performs in operations which checks for multple matchs with the coresponding field
    '''
    def callback(obj:Any= None, obj2 = None):
        if isinstance(value, Field):
            value = getattr(obj2, value.name)
            
        return getattr(obj, field.name).get_value() in range(start, end)

    return callback


def _asc(field):
    return (lambda obj: getattr(obj, field.name).get_value(), False)

def _desc(field):
    return (lambda obj: getattr(obj, field.name).get_value(), True)


class Field:
    def __init__(self, type,
        name = None, 
        default_value = None, pk = False, 
        nullable = False, unique = False
    ):
        self.name = None
        self.type = type
        self.default_value = default_value
        self.pk = pk
        self.nullable = nullable
        self.unique = unique
    
    def set_value(self, value):
        self.default_value = value
    
    def set_name(self, value):
        self.name = value

    def get_value(self):
        return self.default_value

class TableMeta(type):
    
    def __new__(cls, name, bases, dct):
        cls_fields = {k: v for k, v in dct.items()}
        
        for field_name, field_value in cls_fields.items():
            if isinstance(field_value, Field):
                dct[field_name] = field_value
                field_value.set_name(field_name)
                
        cls_instance = super().__new__(cls, name, bases, dct)
        
        if name!='Model':
            MetaData = cls_fields['Meta'].__dict__
            cls_instance.__tablename__ = MetaData.get('__tablename__').lower()
            cls_instance.__tabledesc__ = MetaData.get('__tabledesc__').lower()
            cls_instance.__version__ = MetaData.get('__version__')
            cls_instance.__last_modified__ = ""
            cls_instance.__owner__ = None
        return cls_instance
        
class Model(metaclass = TableMeta):

    def __init__(self):
        self.fields = {}
        
    def __init__(self, **kwargs):
        for field_name, field_value in self.__class__.__dict__.items():
            if isinstance(field_value, Field):
                field_copy = Field(**field_value.__dict__)
                field_copy.set_value(kwargs.get(field_name, field_copy.get_value()))
                setattr(self, field_name, field_copy)

class User(Model):
    
    id = Field(type=2, default_value="", pk=True)
    name = Field(type=1, default_value="")
    data = Field(type=2, default_value="",)
    
    
    def __repr__(self) -> str:
        return f"Users: {self.id.get_value()}->{self.name.get_value()}"
    
    class Meta:
        __tablename__ = "user"
        __tabledesc__ = "this is dept table"
        __version__ = 1.0


class Orders(Model):
    
    id =  Field(type=1, default_value="", pk=True)
    name = Field(type=1, default_value="")
    userid = Field(type=2, default_value="",)
    
    def __repr__(self) -> str:
        return f"Orders: {self.id.get_value()}->{self.name.get_value()}"
    
    
    class Meta:
        __tablename__ = "orders"
        __tabledesc__ = "this is dept table"
        __version__ = 1.0


# insert query
insertQuery = InsertOrm()
insertQuery.insert(User).fields([
    {"name": "C", "data": 4, "id":1},
    {"name": "B", "data": 2, "id":2},
    {"name": "C", "data": 3, "id":3},
    {"name": "C", "data": 4, "id":4},
    {"name": "D", "data": 4, "id":5},
]).execute()

insertQuery.insert(Orders).fields([
    {"name": "C", "id": 1, "userid":1},
    {"name": "B", "id": 2, "userid":2},
    {"name": "C", "id": 3, "userid":3},
    {"name": "C", "id": 4, "userid":3},
    {"name": "D", "id": 5, "userid":4},
]).execute()

# select Query
query = SelectOrm()
res = query.select(User.id, User.name, User.data).table(User).where([_eq(User.name, "B")]).order_by([_asc(User.name)])
print(res.execute())

res = query.select(Orders.id, Orders.name,Orders.userid).table(Orders).where([]).order_by([_asc(User.name)])
print(res.execute())


res = query.select(User.id, User.name, User.data).join(User, Orders).on([_eq(User.id, Orders.userid)]).execute()