import datetime

def datetimeFromIso( s ):
    """parse (a subset of ) valid ISO 8601 dates"""
    
    s = s.strip()
    try:
        return datetime.datetime.strptime( s, '%Y-%m-%dT%H:%M:%S.%f' )
    except ValueError:
        return datetime.datetime.strptime( s, '%Y-%m-%dT%H:%M:%S' )
        
    
    
