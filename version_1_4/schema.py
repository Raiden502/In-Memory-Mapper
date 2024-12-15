from test_orm.table.models import Model
from test_orm.table.fields import Field
from test_orm.db.database import DBEngine
from test_orm.orm.insert import insert
from test_orm.orm.select import select
from test_orm.orm.update import update
from test_orm.orm.delete import delete
from test_orm.orm.operators import on_, or_

from datetime import datetime



def current_date():
    return datetime.now()

class Organization(Model):
    org_name = Field(type=str, nullable=True)
    email = Field(type=str, unique=True)
    priority = Field(type=int, default=0)

class User(Model):
    user_name = Field(type=str)
    age = Field(type=str, nullable=True)
    verification = Field(type=str, default='PENDING')
    created_at = Field(type=datetime, default=current_date)

class Address(Model):
    address = Field(type=str)
    user_id = Field(type=str)
    


engine = DBEngine()
engine.migrate(Organization)
engine.migrate(User)
engine.migrate(Address)

# -------------------------------Insert------------------------------

insert(User).add_all([
    {'user_name':'Jhon'}, {'user_name':'don'}
]).execute()

insert(Organization).add_all(
    [
        {'org_name' : 'Jhon', 'email':'ara@gmail.com', 'priority':1}, 
        {'org_name' : 'jean', 'email':'bara@gmail.com', 'priority':3}, 
        {'org_name' : 'jean', 'email':'avi@gmail.com',}, 
    ]
).execute()

user_id = select(User._id).model(User).filter(User.user_name=='Jhon').execute().data[0][0]

insert(Address).add_all([{'address':'12, #a town', "user_id":user_id}, {'address':'16, #b town', "user_id":user_id}]).execute()







