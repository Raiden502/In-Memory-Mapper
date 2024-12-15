
from test_orm.orm.select import select
from test_orm.orm.update import update
from test_orm.orm.delete import delete
from test_orm.orm.operators import on_, or_
from schema import Organization, User, Address
# --------------------------Select--------------------------------------

orgQuery = select(Organization.org_name).model(Organization).filter(Organization.org_name == 'Jhon').execute()
print("equals", orgQuery.data)

orgQuery = select(Organization.org_name).model(Organization).filter(Organization.org_name != 'Jhon').execute()
print("not equals",orgQuery.data)

orgQuery = select(Organization.org_name).model(Organization).filter(Organization.priority < 1).execute()
print("less than", orgQuery.data)

orgQuery = select(Organization.org_name).model(Organization).filter(Organization.priority <= 1).execute()
print("less than equals",orgQuery.data)

orgQuery = select(Organization.org_name).model(Organization).filter(Organization.priority >= 1).execute()
print("greater than equals", orgQuery.data)

orgQuery = select(Organization.org_name).model(Organization).filter(Organization.priority > 1).execute()
print("greater than", orgQuery.data)

orgQuery = select(Organization.org_name).model(Organization).filter(Organization.priority.in_([1,2])).execute()
print("in condition", orgQuery.data)

orgQuery = select(Organization.org_name).model(Organization).filter(Organization.priority.btw_(1,4)).execute()
print("between condition", orgQuery.data)

orgQuery = select(Organization.org_name).model(Organization).filter(Organization.org_name.sw_("j")).execute()
print("starts with", orgQuery.data)

orgQuery = select(Organization.org_name).model(Organization).filter(Organization.org_name.sw_("J", ilike=True)).execute()
print("starts with ilike", orgQuery.data)


orgQuery = select(Organization.org_name).model(Organization).filter(Organization.org_name.ew_("an")).execute()
print("ends with", orgQuery.data)

orgQuery = select(Organization.org_name).model(Organization).filter(Organization.org_name.ew_("AN", ilike=True)).execute()
print("ends with ilike", orgQuery.data)

orgQuery = select(Organization.org_name).model(Organization).filter(Organization.org_name.reg_("EA", ilike=True)).execute()
print("regex", orgQuery.data)


orgQuery = select(Organization.org_name).model(Organization).filter(Organization.priority.in_([1,2])).sort(Organization.org_name.asc).execute()
print("ascending", orgQuery.data)

orgQuery = select(Organization.org_name).model(Organization).filter(Organization.priority.in_([1,2])).sort(Organization.org_name.desc).execute()
print("descending", orgQuery.data)

orgQuery = select(Organization._id, Organization.org_name).model(Organization).cluster(Organization.org_name).execute()
print("cluster", orgQuery.data)

orgQuery = select(Organization._id, Organization.org_name).model(Organization).cluster(Organization.org_name).filter_cluster(Organization.org_name =='jean').execute()
print("cluster with filters", orgQuery.data)

userRes = select(
            User._id, User.user_name, Address._id, Address.address, Address.user_id
        ).model(User).join(Address, on_(User._id, Address.user_id, "="), type=select.RIGHT).execute()
print("join", userRes.data)

res = select(Address._id, Address.address).model(Address).filter(
        Address.user_id == select(User._id).model(User).filter(User.user_name=='Jhon').execute()
    ).execute()
print("subquery", res.data)

res = select(Address._id, Address.address).model(Address).filter(
        or_(Address.user_id == select(User._id).model(User).filter(User.user_name=='Jhon').execute(), Address._id=='1')
    ).execute()
print("or condition filters", res.data)

res = select(Address._id, Address.address).model(Address).filter(
        Address.address.sw_('12'), Address._id=='0be87548-3e1f-4193-83c8-d781c71af23f'
    ).execute()
print("multiple filters (and condition)", res.data)

res = select(Address._id, Address.address).model(Address).filter(
        or_(Address.user_id!='1', Address._id=='1'), Address.address.sw_("12", ilike=True)
    ).execute()

print("multiple filters (and/or condition)", res.data)


# --------------------------- updates -------------------------


update(Organization).set(org_name='Ara').filter(Organization.org_name=='jean').execute()

data = select(Organization.email, Organization.org_name, Organization._id).model(Organization).execute()
print("update", data.data)

# update(Organization).set(org_name='Ara', email='avi@gmail.com').filter(Organization.org_name=='Ara').execute()

# data = select(Organization.email, Organization.org_name, Organization._id).model(Organization).execute()
# print("update", data.data)


# ------------------------------- delete -------------------------------------------
delete(Organization).filter(Organization.org_name == 'Ara').execute()

data = select(Organization.email, Organization.org_name, Organization._id).model(Organization).execute()
print(data.data)


