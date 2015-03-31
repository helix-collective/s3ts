import json, os

class FileStore(object):

    def exists( self, path ):
        """Returns true if a file with the given file exists"""
        raise RuntimeError, "Not implemented"
        

    def get( self, path ):
        """Get the value associated with path

        Raises a KeyError if the path doesn't exist

        """
        raise RuntimeError, "Not implemented"

    def put( self, path, body ):
        """Store a value associated with path

        Overwrites any existing value
        """
        raise RuntimeError, "Not implemented"

    def list( self, pathPrefix ):
        """Return all paths having the specified path prefix"""
        raise RuntimeError, "Not implemented"

    def getFromJson( self, path, jscoder ):
        return jscoder.fromJson( json.loads( self.get(path) ) )
        
    def putToJson( self, path, v, jscoder ):
        return self.put( path, json.dumps( jscoder.toJson(v) ) )

    
class LocalFileStore(FileStore):
    """implements the FileStore interface using the local file system"""

    def __init__( self, root ):
        self.root = root

    def __path( self, path ):
        return os.path.join( self.root, path )

    def exists( self, path ):
        return os.path.exists( self.__path(path)  )

    def get( self, path ):
        try:
            with open( self.__path(path), 'rb' ) as f:
                return f.read()
        except IOError, e:
            raise KeyError(e)

    def put( self, path, body ):
        path = self.__path(path)
        dir = os.path.dirname( path )
        if not os.path.isdir( dir ):
            os.makedirs( dir )
        with open( path, 'wb' ) as f:
            f.write(body)

    def list( self, pathPrefix ):
        results = []
        for dir0, dirs, files in os.walk(self.__path(pathPrefix)):
            for file in files:
                path = os.path.join( dir0, file)
                rpath = os.path.relpath( path, os.path.join( self.root, pathPrefix ) )
                results.append( rpath )
        return results
            
        

    
