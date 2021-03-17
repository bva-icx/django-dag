
class NodeNotReachableException (Exception):
    """Exception for node distance and path"""
    pass

class InvalidNodeMove (Exception):
    """Invalid Node Movement"""
    pass

class InvalidNodeInsert (Exception):
    """Invalid Node Insert"""
    pass

class NodeSequenceExhaustion (Exception):
    """The gap between to node is to small to generate an intermediate value"""
    pass
