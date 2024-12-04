from testorm import Field, Database, Insert, Select, Model, _lt, _gt, _ge, _le, _in, _or, _sw, _ew, _eq, _asc, _desc, _on, _ne

class Organization(Model):
    orgId = Field(type=2, default="")
    orgName = Field(type=1, default="")
    
    class Meta:
        __version__ = 1.0

class User(Model):
    userId = Field(type=2, default="")
    userName = Field(type=1, default="")
    orgId = Field(type=2, default="1",)
    
    class Meta:
        __version__ = 1.0


class Orders(Model):
    
    id =  Field(type=1, default="")
    name = Field(type=1, default="")
    userId = Field(type=2, default="",)
    
    class Meta:
        __version__ = 1.0


db = Database()
db.migrate(Organization)
db.migrate(User)
db.migrate(Orders)

# insert query
insertQuery = Insert()

insertQuery.insert(Organization).fields([
    {"orgId":1, "orgName":"Testing"},
    {"orgId":2, "orgName":"Testing2"},
]).execute()

insertQuery.insert(User).fields([
    {"userId":1, "userName":"user1", "orgId":1},
    {"userId":2, "userName":"user2", "orgId":1},
    {"userId":3, "userName":"user3",},
    {"userId":4, "userName":"user4",},
]).execute()


insertQuery.insert(Orders).fields([
    {"id":1, "name":"Product1", "userId":1},
    {"id":2, "name":"Product2", "userId":2},
    {"id":3, "name":"Product3", "userId":3},
    {"id":4, "name":"Product4", "userId":1},
    {"id":5, "name":"Product5", "userId":1},
]).execute()

# select Query
query = Select()

orgQuery = query.select(Organization.orgId).model(Organization).filter(_eq(Organization.orgId, 1)).execute()
print(orgQuery.data)

userRes = query.select(
            User.userId, User.userName, User.orgId, 
            Orders.id, Orders.name, Orders.userId
        ).model(User).join(Orders, _on(User.userId, Orders.userId, "="), type=query.RIGHT)\
        .filter(_eq(Orders.userId, 1)).execute()

print(userRes.data)