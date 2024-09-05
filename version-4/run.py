from testorm import Field, Database, InsertOrm, SelectOrm, Model, _lt, _gt, _ge, _le, _in, _or, _sw, _ew, _eq, _asc, _desc

class User(Model):
    
    id = Field(type=2, default_value="", pk=True)
    name = Field(type=1, default_value="")
    data = Field(type=2, default_value="",)
    
    class Meta:
        __version__ = 1.0


class Orders(Model):
    
    id =  Field(type=1, default_value="", pk=True)
    name = Field(type=1, default_value="")
    userid = Field(type=2, default_value="",)
    
    class Meta:
        __version__ = 1.0


db = Database()
db.migrate(User)
db.migrate(Orders)

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
    {"name": "OD", "id": 1, "userid":4},
    {"name": "OC", "id": 5, "userid":1},
    {"name": "OB", "id": 2, "userid":2},
    {"name": "OC", "id": 3, "userid":3},
    {"name": "OG", "id": 4, "userid":3},
]).execute()

# select Query
query = SelectOrm()

userRes = query.select(User.id).table(User).where([ _eq(User.id, 2)]).execute_new()
print(userRes.data)
print(userRes.__meta__)

ordersRes = query.select(Orders.id, Orders.name, Orders.userid).table(Orders).where([_ge(Orders.userid, userRes), _eq(Orders.userid, 3)]).group_by([Orders.name]).execute_new()

print(ordersRes.data)
print(ordersRes.__meta__)
