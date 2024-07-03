import random
from dataclasses import dataclass , field, asdict
from typing import List, Callable, Any, Tuple

@dataclass
class BaseModel:
    
    @classmethod
    def autoPk(cls) -> int:
        return random.randint(0, 100)
    
    @classmethod
    def load(cls, data):
        return cls(**data)
    

class Orm:
    def __init__(self):
        self.selected_fields = None
        self.result = []
    
    def select(self, fields):
        self.selected_fields = fields
        self.result = []
        return self
    
    def _filter_fields(self, item):
        fields = self.selected_fields
        
        if item is None:
            return None

        item_dict = asdict(item)
        return {field: item_dict[field] for field in fields if field in item_dict}
    
    def join(self, list1, list2, condition: Callable[[Any, Any], bool]):
        for item1 in list1:
            for item2 in list2:
                if condition(item1, item2):
                    if self.selected_fields:
                        self.result.append((self._filter_fields(item1), self._filter_fields(item2)))
        return self
    
    def rightJoin(self, list1, list2, condition: Callable[[Any, Any], bool]):
        for item2 in list2:
            for item1 in list1:
                if condition(item1, item2):
                    if self.selected_fields:
                        self.result.append(self._filter_fields(item2))
        return self
        
    def leftJoin(self, list1, list2, condition: Callable[[Any, Any], bool]):
        for item1 in list1:
            for item2 in list2:
                if condition(item1, item2):
                    if self.selected_fields:
                        self.result.append(self._filter_fields(item1))
        return self
    
    def fetchall(self):
        return self.result
    
    def limit(self, value):
        return self.result[:value]


@dataclass
class User(BaseModel):
    user_id: int = field(default = 0, metadata={"required": True})
    user_name: str = field(default = "")
    

@dataclass
class Department(BaseModel):
    department_id : int = field(default = 0, metadata={"required": True})
    department_name : str = field(default="",  metadata={"required": True})
    users : List[int] = field(default_factory = list)
    

def condition(user:BaseModel, department:BaseModel) -> bool:
    return user.user_id in department.users

if __name__=='__main__':
    query:Orm = Orm()
    users : List[User] = [User.load({"user_id":i, "user_name": f"a{i}"}) for i in range(5)]
    departments : List[Department] =[Department.load({"department_id":i, "department_name": f"b{i}"}) for i in range(5)]
    
    departments[0].users.append(users[0].user_id)
    departments[2].users.append(users[0].user_id)
    departments[3].users.append(users[0].user_id)
    
    
    innerJoin: List[Any] = query.select(
                    ['user_id', 'department_id', 'user_name', 'department_name']
                ).join(users, departments, condition).fetchall()
    
    rightJoin: List[Any] = query.select(
                    ['department_id', 'department_name']
                ).rightJoin(users, departments, condition).fetchall()
    
    leftJoin: List[Any] = query.select(
                    ['user_id', 'user_name']
                ).leftJoin(users, departments, condition).limit(1)
    
    
    print("Join:\n",innerJoin, end="\n\n")
    print("right join:\n",rightJoin, end="\n\n")
    print("left join:\n",leftJoin, end="\n\n")
