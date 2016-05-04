import os

from s3ts.filestore import FileStore

from boto.s3.key import Key
from boto.exception import S3ResponseError

class S3FileStore(FileStore):
    def __init__( self, bucket, pathPrefix=None ):
        self.bucket = bucket
        self.pathPrefix = pathPrefix

    def exists( self, path ):
        k = self._key(path)
        return k.exists()

    def get( self, path ):
        k = self._key(path)
        try:
            return k.get_contents_as_string()
        except S3ResponseError, e:
            if e.status == 404:
                raise KeyError(e)
            raise

    def put( self, path, body ):
        k = self._key(path)
        k.set_contents_from_string( body )

    def list( self, pathPrefix ):
        return [os.path.relpath(key.name,pathPrefix) for key in self.bucket.list(prefix=pathPrefix)]

    def remove( self, path ):
        k = self._key(path)
        k.delete()

    def url( self, path, expiresInSecs ):
        k = self._key(path)
        return k.generate_url(expiresInSecs)

    def joinPath( self, *elements):
        return '/'.join(elements)

    def splitPath(self, path):
        return path.split('/')

    def _key(self,path):
        if self.pathPrefix:
            path = self.joinPath( self.pathPrefix, path )
        return Key(self.bucket,path)
