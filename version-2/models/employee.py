from datetime import datetime

from repliq.tables import Model, Field, Types
from typing import Any , Optional, List, Dict


class Employee(Model):
    
    emp_id: int = Field(type=Types.INTIGER, default_value="", pk=True)
    name: Optional[str] = Field(type=Types.STRING, default_value="", nullable=True)
    description: Optional[str] = Field(type=Types.STRING, default_value="", nullable=True)
    category: str = Field(type=Types.STRING, default_value="")
    status: Optional[str] = Field(type=Types.STRING, default_value="available")
    onboarding_date: datetime = Field(type=Types.DATETIME, default_value=datetime.now)

    class Meta:
        __tablename__ = "dept_emply"
        __tabledesc__ = "this is employee table"
        __version__ = 1.0
        