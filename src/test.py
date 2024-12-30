from in_memory_db.orm.select import select
from in_memory_db.orm.operators import alias, expr, on_
from schema import User, Address



# orgQuery = select(
#         alias(Organization._id, 'org_id'),
#         Organization.org_name, 
#         Organization.priority,
#         alias('sujanix', 'office_name'),
#         alias(expr('Organization.priority * 10'), 'score'),
# ).model(Organization).execute()

# print(orgQuery.columns)
# print(orgQuery.data)


userRes = select(
            User._id, 
            User.user_name, 
            Address.address,
            User.cost,
            Address.delivery_cost,
            alias(expr('((User.cost * Address.delivery_cost)/100) - 2'), 'estimation'),
            alias((User.cost + Address.delivery_cost * 2), 'addon'),
            alias(User.cost  == 1, 'compare'),
            alias(User.cost  == Address.delivery_cost, 'compare2'),
        ).model(User).join(Address, on_(User._id, Address.user_id, "="), type=select.RIGHT).execute()


print(userRes.columns)
print(userRes.data)


userRes = select(User.user_name, 'compare').model(userRes).execute()
print(userRes.columns)
print(userRes.data)