from replq.tables import Model, Field
from typing import Optional, List
from datetime import datetime


class LibraryInfo(Model):
    
    book_id: int = Field(type=int, value=Model.default_uniqueId, pk=True)
    name: Optional[str] = Field(type=str, value="", nullable=True)
    description: Optional[str] = Field(type=str, value="", nullable=True)
    category: str = Field(type=str, value="")
    tags: List[str] = Field(type=List[str], value=list)
    status: Optional[str] = Field(type=str, value="available")
    purchase_date: datetime = Field(type=datetime, value=datetime.now)
