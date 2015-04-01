import os, hashlib, zlib, tempfile
import requests

from s3ts.config import TreeStoreConfig, TreeStoreConfigJS
from s3ts import package

CONFIG_PATH = 'config'
TREES_PATH = 'trees'

class TreeStore(object):
    """implements a directory tree store

    This class implements a simple directory tree storage mechanism. It supports 
    uploading and downloading of multiple directory trees, with data chunking,
    and de-duplication. A local cache is kept for download optimisation.
    """

    @classmethod
    def create( cls, pkgStore, localCache, config ):
        """creates a new treestore"""
        try:
            pkgStore.get( CONFIG_PATH )
            raise RuntimeError, "treestore is already initialised"
        except KeyError:
            pkgStore.putToJson( CONFIG_PATH, config, TreeStoreConfigJS() )
            return cls( pkgStore, localCache, config )

    @classmethod
    def open( cls, pkgStore, localCache ):
        """opens a existing treestore"""
        return cls( pkgStore, localCache, pkgStore.getFromJson( CONFIG_PATH, TreeStoreConfigJS() ) )
    
    def __init__( self, pkgStore, localCache, config ):
        self.pkgStore = pkgStore
        self.localCache = localCache
        self.config = config

    def upload( self, treeName, localPath ):
        packageFiles = self.__storeFiles( self.pkgStore, localPath )
        pkg = package.Package( treeName, packageFiles )
        self.pkgStore.putToJson( self.__treeNamePath( treeName ), pkg, package.PackageJS() )
        return pkg

    def find( self, treeName ):
        """Return the package definition for the given name"""
        return self.pkgStore.getFromJson( self.__treeNamePath( treeName ), package.PackageJS() )

    def list( self ):
        """Returns the available packages names"""
        return self.pkgStore.list( TREES_PATH )

    def verify( self, pkg ):
        """confirms that all data for the given package is present in the store"""
        for pf in pkg.files:
            for chunk in pf.chunks:
                cpath = self.__chunkPath( chunk.sha1, chunk.encoding ) 
                if not self.pkgStore.exists( cpath ):
                    raise RuntimeError, "{0} not found".format(cpath)

    def verifyLocal( self, pkg ):
        """confirms that all data for the given package is present in the local cache"""
        for pf in pkg.files:
            for chunk in pf.chunks:
                cpath = self.__chunkPath( chunk.sha1, chunk.encoding ) 
                if not self.localCache.exists( cpath ):
                    raise RuntimeError, "{0} not found".format(cpath)

    def download( self,  pkg, progressCB ):
        """downloads all data not already present to the local cache

        progressCB will be called with parameters (nBytes) as the download progresses

        """
        for pf in pkg.files:
            for chunk in pf.chunks:
                cpath = self.__chunkPath( chunk.sha1, chunk.encoding )
                if not self.localCache.exists( cpath ):
                    buf = self.pkgStore.get( cpath )
                    self.__checkSha1( self.__decompress( buf, chunk.encoding ), chunk.sha1, cpath )
                    self.localCache.put( cpath, buf )
                progressCB( chunk.size )

    def downloadHttp( self, pkg, progressCB ):
        """downloads all data not already present to the local cache, using http.

        This requires that pkg already has embedded urls, created with the addUrls method
        progressCB will be called with parameters (nBytes) as the download progresses

        """
        for pf in pkg.files:
            for chunk in pf.chunks:
                cpath = self.__chunkPath( chunk.sha1, chunk.encoding )
                if not self.localCache.exists( cpath ):
                    resp = requests.get( chunk.url )
                    resp.raise_for_status()
                    buf = resp.content
                    self.__checkSha1( self.__decompress( buf, chunk.encoding ), chunk.sha1, cpath )
                    self.localCache.put( cpath, buf )
                progressCB( chunk.size )

    def install( self, pkg, localPath, progressCB ):
        """installs the given package into the local path

        progressCB will be called with parameters (nBytes) as the installation progresses

        """
        for pf in pkg.files:
            filesha1 = hashlib.sha1()
            with tempfile.NamedTemporaryFile(delete=False) as f:
                for chunk in pf.chunks:
                    cpath = self.__chunkPath( chunk.sha1, chunk.encoding )
                    buf = self.localCache.get( cpath )
                    buf = self.__decompress( buf, chunk.encoding )
                    filesha1.update( buf )
                    self.__checkSha1( buf, chunk.sha1, cpath )
                    f.write( buf )
                    progressCB( len(buf) )
            if filesha1.hexdigest() != pf.sha1:
                raise RuntimeError, "sha1 for {0} doesn't match".format(pf.path)
            targetPath = os.path.join( localPath, pf.path )
            targetDir = os.path.dirname(targetPath)
            if not os.path.exists( targetDir ):
                os.makedirs( targetDir )
            os.rename( f.name, targetPath )

    def addUrls( self, pkg, expiresInSecs ):
        """Update the given package so that it can be accessed directly via pre-signed http urls"""
        for pf in pkg.files:
            for chunk in pf.chunks:
                cpath = self.__chunkPath( chunk.sha1, chunk.encoding )
                chunk.url = self.pkgStore.url( cpath, expiresInSecs )

    def prime( self, localPath ):
        """Walk a local directory tree and ensure that all chunks of all files are present in the local cache"""
        self.__storeFiles( self.localCache, localPath )
        
    def __storeFiles( self, store, localPath ):
        if not os.path.isdir( localPath ):
            raise IOError( "directory {0} doesn't exist".format( localPath ) )
        packageFiles = []
        for root, dirs, files in os.walk(localPath):
            for file in files:
                rpath = os.path.relpath( os.path.join(root, file), localPath )
                packageFiles.append( self.__storeFile( store, localPath, rpath ) )
        return packageFiles

    def __storeFile( self, store, root, rpath ):
        filesha1 = hashlib.sha1()
        chunks = []
        with open( os.path.join( root, rpath ), 'rb' ) as f:
            while True:
                buf = f.read( self.config.chunkSize )
                if not buf:
                    break
                chunksha1 = hashlib.sha1()
                chunksha1.update( buf )
                filesha1.update( buf )
                size = len(buf)
                encoding = package.ENCODING_RAW
                if self.config.useCompression:
                    buf,encoding = self.__compress( buf )
                chunks.append( self.__storeChunk( store, chunksha1.hexdigest(), encoding, buf, size ) )

        return package.PackageFile( filesha1.hexdigest(), rpath, chunks )

    def __storeChunk( self, store, sha1, encoding, buf, size ):
        cpath = self.__chunkPath( sha1, encoding )
        if not store.exists( cpath ):
            store.put( cpath, buf )
        return package.FileChunk( sha1, size, encoding, None )

    def __treeNamePath( self, treeName ):
        return os.path.join( TREES_PATH, treeName )
            
    def __chunkPath( self, sha1, encoding ):
        enc = {
            package.ENCODING_RAW : 'raw',
            package.ENCODING_ZLIB : 'zlib',
        }[encoding]
        return os.path.join( 'chunks', enc, sha1[:2], sha1[2:] )

    def __compress( self, buf ):
        bufz = zlib.compress( buf )
        if len( bufz ) < len( buf ):
            return bufz,package.ENCODING_ZLIB
        else:
            return buf,package.ENCODING_RAW

    def __decompress( self, buf, encoding ):
        if encoding == package.ENCODING_ZLIB:
            return zlib.decompress( buf )
        else:
            return buf

    def __checkSha1( self, buf, sha1, cpath ):
        csha1 = hashlib.sha1()
        csha1.update( buf )
        if csha1.hexdigest() != sha1:
            raise RuntimeError, "sha1 for {0} doesn't match".format( cpath )
        
