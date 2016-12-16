import datetime, os

def datetimeFromIso( s ):
    """parse (a subset of ) valid ISO 8601 dates"""
    
    s = s.strip()
    try:
        return datetime.datetime.strptime( s, '%Y-%m-%dT%H:%M:%S.%f' )
    except ValueError:
        return datetime.datetime.strptime( s, '%Y-%m-%dT%H:%M:%S' )
        

def allFilePaths(path):
    """Find all of the files below path"""
    result = []
    for root, dirs, files in os.walk(path):
        for file in files:
            result.append(os.path.normpath(os.path.join(os.path.relpath(root,path), file)))
    return result

def removeEmptyDirectories(path, removeRoot):
  """Remove empty directories recursively"""

  if not os.path.isdir(path):
    return

  files = os.listdir(path)
  if len(files):
    for f in files:
      fullpath = os.path.join(path, f)
      if os.path.isdir(fullpath):
        removeEmptyDirectories(fullpath, True)

  files = os.listdir(path)
  if len(files) == 0 and removeRoot:
    os.rmdir(path)        
    
