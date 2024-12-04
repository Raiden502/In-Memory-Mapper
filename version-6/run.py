from testorm import Field, Database, insert, select, Model, on_, or_

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

insert(Organization).fields([
    {"orgId":1, "orgName":"Testing"},
    {"orgId":2, "orgName":"Testing2"},
]).execute()

insert(User).fields([
    {"userId":1, "userName":"user1", "orgId":1},
    {"userId":2, "userName":"user2", "orgId":1},
    {"userId":3, "userName":"user3",},
    {"userId":4, "userName":"user4",},
]).execute()


insert(Orders).fields([
    {"id":1, "name":"Product1", "userId":1},
    {"id":2, "name":"Product2", "userId":2},
    {"id":3, "name":"Product3", "userId":3},
    {"id":4, "name":"Product4", "userId":1},
    {"id":5, "name":"Test5", "userId":1},
]).execute()


# select Query

orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgId == 1).execute()
print("equals", orgQuery.data)

orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgId != 1).execute()
print("not equals",orgQuery.data)

orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgId < 1).execute()
print("less than", orgQuery.data)

orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgId <= 1).execute()
print("less than equals",orgQuery.data)

orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgId >= 1).execute()
print("greater than equals", orgQuery.data)

orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgId > 1).execute()
print("greater than", orgQuery.data)

orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgId.in_([1,2])).execute()
print("in condition", orgQuery.data)

orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgId.btw_(1,4)).execute()
print("between condition", orgQuery.data)

orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgName.sw_("Tes")).execute()
print("starts with", orgQuery.data)

orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgName.sw_("tes", ilike=True)).execute()
print("starts with ilike", orgQuery.data)


orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgName.ew_("ing2")).execute()
print("ends with", orgQuery.data)

orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgName.ew_("ING", ilike=True)).execute()
print("ends with ilike", orgQuery.data)

orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgName.reg_("IN", ilike=True)).execute()
print("regex", orgQuery.data)


orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgId.in_([1,2])).sort(Organization.orgId.asc).execute()
print("ascending", orgQuery.data)

orgQuery = select(Organization.orgId).model(Organization).filter(Organization.orgId.in_([1,2])).sort(Organization.orgId.desc).execute()
print("descending", orgQuery.data)


userRes = select(
            User.userId, User.userName, User.orgId, 
            Orders.id, Orders.name, Orders.userId
        ).model(User).join(Orders, on_(User.userId, Orders.userId, "="), type=select.RIGHT)\
        .filter(Orders.userId ==1).execute()
print("join", userRes.data)

res = select(Orders.id, Orders.name).model(Orders).filter(
        Orders.userId == select(User.userId).model(User).filter(User.userId == 3).execute()
    ).execute()
print("subquery", res.data)

res = select(Orders.id, Orders.name).model(Orders).filter(
        or_(Orders.userId == select(User.userId).model(User).filter(User.userId == 3).execute(), Orders.id==1)
    ).execute()
print("or condition filters", res.data)

res = select(Orders.id, Orders.name).model(Orders).filter(
        Orders.userId==1, Orders.id==1
    ).execute()
print("multiple filters (and condition)", res.data)

res = select(Orders.id, Orders.name).model(Orders).filter(
        or_(Orders.userId==1, Orders.id==1), Orders.name.sw_("prod", ilike=True)
    ).execute()

print("multiple filters (and/or condition)", res.data)