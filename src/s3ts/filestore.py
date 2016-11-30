import json, os

from s3ts import filewriter

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

    def remove( self, path ):
        """Remove a value with the path, if it exists
        """
        raise RuntimeError, "Not implemented"

    def list( self, pathPrefix ):
        """Return all paths having the specified path prefix"""
        raise RuntimeError, "Not implemented"

    def getFromJson( self, path, jscoder ):
        return jscoder.fromJson( json.loads( self.get(path) ) )

    def putToJson( self, path, v, jscoder ):
        return self.put( path, json.dumps( jscoder.toJson(v) ) )

    def url( self, path, expiresInSecs ):
        raise RuntimeError, "Not implemented"

    def joinPath( self, *elements):
        raise RuntimeError, "Not implemented"

    def splitPath( self, path):
        raise RuntimeError, "Not implemented"

    def getMetadata( self, path):
        raise RuntimeError, "Not implemented"

class FileMetaData:
    def __init__(self,size,lastModified):
        self.size = size
        self.lastModified = lastModified
    def __repr__(self):
        return self.__str__()
    def __str__(self):
        return "size:" + str(self.size) + " mtime:" + str(self.lastModified) 

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

        # Do our best to be atomic in our updates here, in case
        # another process is simultaneously updating the file
        with filewriter.atomicFileWriter(path) as f:
            f.write(body)

    def remove( self, path ):
        path = self.__path(path)
        if os.path.exists( path ):
            os.unlink(path)

    def list( self, pathPrefix ):
        results = []
        for dir0, dirs, files in os.walk(self.__path(pathPrefix)):
            for file in files:
                path = os.path.join( dir0, file)
                rpath = os.path.relpath( path, os.path.join( self.root, pathPrefix ) )
                results.append( rpath )
        return results
    
    def joinPath( self, *elements):
        return os.path.join(*elements)

    def splitPath( self, path):
        return path.split(os.sep)

    def getMetadata(self, path):
        """Returns the size and update time for the given path
           Raises a KeyError if the path doesn't exist
        """
        statinfo = os.stat(self.__path(path))
        return FileMetaData(statinfo.st_size, statinfo.st_mtime)



