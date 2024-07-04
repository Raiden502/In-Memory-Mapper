from datetime import datetime

from repliq.tables import Model, Field, Types
from typing import Any , Optional, List, Dict


class Department(Model):
    
    dept_id: int = Field(type=Types.INTIGER, default_value="", pk=True)
    name: Optional[str] = Field(type=Types.STRING, default_value="", nullable=True)
    description: Optional[str] = Field(type=Types.STRING, default_value="", nullable=True)
    category: str = Field(type=Types.STRING, default_value="")
    employees: list = Field(type=Types.LIST,)
    estd_date: datetime = Field(type=Types.DATETIME, default_value=datetime.now)
    

    class Meta:
        __tablename__ = "department"
        __tabledesc__ = "this is dept table"
        __version__ = 1.0
        