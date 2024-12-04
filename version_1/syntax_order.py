
class Execute:
    def __init__(self, instance):
        self.sharedInst = instance
    
    def compute(self):
        print("compute", self.sharedInst.fields, self.sharedInst.where )

class WhereBranch:
    
    def __init__(self, instance):
        self.sharedInst = instance
    
    def groupby(self, conditions):
        self.sharedInst.groupby = conditions
        return Execute(self.sharedInst).compute
    
    def execute(self):
        return Execute(self.sharedInst).compute

class Where:
    
    def __init__(self, instance):
        self.sharedInst = instance
    
    def where(self, conditions):
        self.sharedInst.where = conditions
        return WhereBranch(self.sharedInst)
    
    def groupby(self, conditions):
        self.sharedInst.groupby = conditions
        return Execute(self.sharedInst).compute
    
    def execute(self):
        return Execute(self.sharedInst).compute


class Table:
    
    def __init__(self, instance):
        self.sharedInst = instance
        
    def table(self, Model):
        self.sharedInst.table = Model
        return Where(self.sharedInst)

class Orm:
    
    def __init__(self):
        self.fields = []
        self.where = []
        self.groupby = []
        self.table = None
    
    def select(self, fields):
        self.fields = fields
        return Table(self)
        

obj  = Orm()
obj.select([1, 3]).table("User").groupby([])()
