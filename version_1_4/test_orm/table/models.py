from .model_meta import TableMeta
from .fields import Field
import uuid


class Model(metaclass = TableMeta):

    _id = Field(type=str, pk=True)
    
    def __init__(self, **kwargs):
        print("working", kwargs)