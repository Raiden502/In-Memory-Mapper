class MoreRowsException(Exception):
    '''
        data must be one record only
    '''
    pass

class MoreColException(Exception):
    '''
        data must have one column only
    '''
    pass

class ModelNotFound(Exception):
    '''
        model not found
    '''
    pass

class UniqueConstraint(Exception):
    '''
        model not found
    '''
    pass

class NoUpdateFields(Exception):
    pass