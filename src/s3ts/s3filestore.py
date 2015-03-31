import os

from s3ts.filestore import FileStore

from boto.s3.key import Key
from boto.exception import S3ResponseError

class S3FileStore(FileStore):
    def __init__( self, bucket ):
        self.bucket = bucket

    def exists( self, path ):
        k = Key(self.bucket,path)
        return k.exists()

    def get( self, path ):
        k = Key(self.bucket,path)
        try:
            return k.get_contents_as_string()
        except S3ResponseError, e:
            if e.status == 404:
                raise KeyError(e)
            raise

    def put( self, path, body ):
        k = Key(self.bucket,path)
        k.set_contents_from_string( body )

    def list( self, pathPrefix ):
        return [os.path.relpath(key.name,pathPrefix) for key in self.bucket.list(prefix=pathPrefix)]

        
        
        

        
        
        
