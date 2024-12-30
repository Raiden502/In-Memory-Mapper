from typing import List, Dict, Any

class Table:
    
    def __init__(self):
        self.name:str = ""
        self.data:List[int] = []
        self.columns:Dict[str, int] = {}
        self.properties:Dict[str, Any] = {
            'constraints': {},
            'headers':{}
        }
        self.unique_hashes={}
        self.lookup = {}
            
    def set_column(self, name, index):
        self.columns[name] = index
    
    def set_constraints(self, name, properties):
        self.properties['constraints'][name] = properties
    
    def set_headers(self, key, value):
        self.properties['headers'][key] = value
    
    def set_hashes(self, name):
        self.unique_hashes[name] = set()
    
    def set_name(self, name):
        self.name = name
    
    def add_row(self, data):
        self.data.append(data)
    
    