from in_memory_db.models.fields import Field
from in_memory_db.models.models import Model
from in_memory_db.db.database import DBEngine
from in_memory_db.orm.insert import insert
from in_memory_db.orm.select import select
from datetime import datetime



def current_date():
    return datetime.now()

class Organization(Model):
    org_name = Field(type=str, nullable=True)
    email = Field(type=str, unique=True)
    priority = Field(type=int, default=0)

class User(Model):
    user_name = Field(type=str)
    cost = Field(type=int, default=0)
    verification = Field(type=str, default='PENDING')
    created_at = Field(type=datetime, default=current_date)

class Address(Model):
    address = Field(type=str)
    delivery_cost = Field(type=int, default=0)
    user_id = Field(type=str)
    


engine = DBEngine()
engine.migrate(Organization)
engine.migrate(User)
engine.migrate(Address)

# -------------------------------Insert------------------------------

insert(User).add_all([
    {'user_name':'Jhon', 'cost':1}, {'user_name':'don', 'cost':2}
]).execute()

insert(Organization).add_all(
    [
        {'org_name' : 'Jhon', 'email':'ara@gmail.com', 'priority':1}, 
        {'org_name' : 'jean', 'email':'bara@gmail.com', 'priority':3}, 
        {'org_name' : 'jean', 'email':'avi@gmail.com',}, 
    ]
).execute()

user_id = select(User._id).model(User).filter(User.user_name=='Jhon').execute().data[0][0]

insert(Address).add_all(
    [{'address':'12, #a town', "user_id":user_id, 'delivery_cost':300}, {'address':'16, #b town', "user_id":user_id, 'delivery_cost':130}]
).execute()







