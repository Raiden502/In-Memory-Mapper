from replq.tables import Model, Field
from typing import Optional, List
from datetime import datetime


class UserInfo(Model):

    user_id: int = Field(type=int, value=Model.default_uniqueId, pk=True)
    name: Optional[str] = Field(type=str, value="", nullable=True)
    description: Optional[str] = Field(type=str, value="", nullable=True)
    mobile_num: str = Field(type=str, value="")
    status: Optional[str] = Field(type=str, value="active")
    tags: List[str] = Field(type=List[str], value=list)
    date: datetime = Field(type=datetime, value=datetime.now)
