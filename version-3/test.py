import re
class Database:
    
    __instance__ = None

    def __new__(cls):
        if Database.__instance__ is None:
            Database.__instance__ = super(Database, cls).__new__(cls)
            Database.__instance__.database = []
        return Database.__instance__


class Orm:
    
    def __init__(self, model = None):
        self.__model = model
        self.__field = []
        self.__conditions = None
        self.__order_by_cond = []
        self.__group_by_cond = []
        self.__filtered = []
        self.__db = Database()

    def select(self, model = None,  *field):
        if model:
            self.__model = model
            
        self.__field = field
        return self
        
    def where(self, conditions):
        self.__conditions = " and ".join(conditions)
        return self
    
    def order_by(self, conditions):
        self.__order_by_cond = [(lambda obj: getattr(obj, field.name).get_value(), reverse) for field, reverse in conditions]
        return self
    
    def group_by(self, conditions):
        self.__group_by_cond = conditions
        return self

    def execute(self):
        if self.__conditions:
            for obj in self.__db.database:
                if(eval(self.__conditions)):
                    self.__filtered.append(obj)
        else:
            self.__filtered = self.__db.database
            
        if(self.__order_by_cond):
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
        self.__rows = {}
        self.__db = Database()
    
    def insert(self, model = None):
        if model:
            self.__model = model
        
        return self

    def fields(self, rows):
        self.__rows = rows
        return self
    
    def execute(self):
        for row in self.__rows:
            obj = self.__model(**row)
            self.__db.database.append(obj)

def _eq(field, value):
    val = f"'{value}'" if isinstance(value, str) else value
    return f"obj.{field.name}.get_value() == {val}"

def _or(conditions):
    return f"( {' or '.join(conditions)} )"

def _ne(field, value):
    val = f"'{value}'" if isinstance(value, str) else value
    return f"obj.{field.name}.get_value() != {val}"


def _lt(field, value):
    val = f"'{value}'" if isinstance(value, str) else value
    return f"obj.{field.name}.get_value() < {val}"

def _le(field, value):
    val = f"'{value}'" if isinstance(value, str) else value
    return f"obj.{field.name}.get_value() <= {val}"

def _gt(field, value):
    val = f"'{value}'" if isinstance(value, str) else value
    return f"obj.{field.name}.get_value() > {val}"

def _ge(field, value):
    val = f"'{value}'" if isinstance(value, str) else value
    return f"obj.{field.name}.get_value() >= {val}"

def _in(field, conditions):
    val = [f"'{value}'" if isinstance(value, str) else value for value in conditions ]
    return f"obj.{field.name}.get_value() in {val}"

def _sw(field, value, startIdx = None, endIdx=None, ilike = False):
    
    if startIdx!=None and endIdx!=None:
        if ilike:
            return f"obj.{field.name}.get_value().lower().startswith('{value}'.lower(), {startIdx}, {endIdx})"
        
        return f"obj.{field.name}.get_value().startswith('{value}', {startIdx}, {endIdx})"
    
    if ilike:
        return f"obj.{field.name}.get_value().lower().startswith('{value}'.lower())"
    
    return f"obj.{field.name}.get_value().startswith('{value}')"

def _ew(field, value, startIdx = None, endIdx=None, ilike = False):
    
    if startIdx!=None and endIdx!=None:
        if ilike:
            return f"obj.{field.name}.get_value().lower().endswith('{value}'.lower(), {startIdx}, {endIdx})"
        
        return f"obj.{field.name}.get_value().endswith('{value}', {startIdx}, {endIdx})"
    
    if ilike:
        return f"obj.{field.name}.get_value().lower().endswith('{value}'.lower())"
    
    return f"obj.{field.name}.get_value().endswith('{value}')"

def _reg(field, value, ilike=False):
    if ilike:
        return f"re.search('{value}'.lower(), obj.{field.name}.get_value().lower())"
        
    return f"re.search('{value}', obj.{field.name}.get_value())"

def _btw(field, start, end):
    return f"obj.{field.name}.get_value() in range({start}, {end})"

def _asc(field):
    return (field, False)

def _desc(field):
    return (field, True)

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
    
    name = Field(type=1, default_value="", pk=True)
    data = Field(type=2, default_value="",)
    num = Field(type=2, default_value="",)
    
    class Meta:
        __tablename__ = "department"
        __tabledesc__ = "this is dept table"
        __version__ = 1.0


# insert query

insertQuery = InsertOrm()
insertQuery.insert(User).fields([
    {"name": "A", "data": 4, "num":2},
    {"name": "B", "data": 2, "num":3},
    {"name": "C", "data": 3, "num":2},
    {"name": "D", "data": 4, "num":2},
]).execute()

# select Query
query = Orm()
res = query.select(User, User.name, User.data, User.num).group_by([User.data, User.num])
print(res.execute())