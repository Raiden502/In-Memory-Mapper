from testorm import Field, Database, InsertOrm, SelectOrm, Model, _lt, _gt, _ge, _le, _in, _or, _sw, _ew, _eq, _asc, _desc, _on, _ne

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
    {"name": "Surya", "data": 1, "id":1},
    {"name": "Avi", "data": 2, "id":2},
    {"name": "Anu", "data": 3, "id":3},
    {"name": "Ajith", "data": 4, "id":4},
    {"name": "Hita", "data": 5, "id":5},
]).execute()


insertQuery.insert(Orders).fields([
    {"name": "OD", "id": 1, "userid":3},
    {"name": "OC", "id": 5, "userid":1},
    {"name": "OB", "id": 2, "userid":2},
    {"name": "OC", "id": 3, "userid":3},
    {"name": "OG", "id": 4, "userid":3},
    {"name": "OE", "id": 6, "userid":None},
]).execute()

# select Query
query = SelectOrm()


orderQuery = query.select(Orders.id).table(Orders).where(_eq(Orders.id, 2)).execute()

userRes = query.select(User.id, User.name, User.data, Orders.id, Orders.name, Orders.userid)\
    .table(User).rightJoin(Orders, _on(User.id, Orders.userid, "="))\
        .where(_ne(Orders.userid, None), _eq(Orders.id, orderQuery)).execute()
        
print(userRes.data)