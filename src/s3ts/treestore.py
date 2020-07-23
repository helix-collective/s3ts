import os, hashlib, zlib, tempfile, datetime, time
import requests

from s3ts.config import TreeStoreConfig, TreeStoreConfigJS, InstallProperties, writeInstallProperties, S3TS_PROPERTIES
from s3ts import package

CONFIG_PATH = 'config'
TREES_PATH = 'trees'
CHUNKS_PATH = 'chunks'
RAW_PATH = 'raw'
ZLIB_PATH = 'zlib'


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

    @classmethod
    def forHttpOnly( cls, localCache ):
        """create a treestore that only supports the downloadHttp method"""
        return cls( None, localCache, None )

    def __init__( self, pkgStore, localCache, config ):
        self.pkgStore = pkgStore
        self.localCache = localCache
        self.config = config
        self.dryRun = False
        self.outVerbose = lambda *args : None

    def setDryRun( self, dryRun ):
        """Set the dryRun flag.

        Uploads will not push any data to S3. Downloads will
        retrieve package files, but no check data.
        """
        self.dryRun = dryRun

    def setOutVerbose( self, outVerbose ):
        """Set the function to generate verbose output

        The supplied function must take a format string, with additional
        format arguments.
        """
        self.outVerbose = outVerbose

    def upload( self, treeName, creationTime, localPath, progressCB ):
        """Creates a package for the content of localPath.

        This uploads the package definition and any file chunks not already uploaded.
        progressCB will be called with parameters (bytesUploaded,bytesCached) as the upload progresses

        """
        packageFiles = self.__storeFiles( self.pkgStore, localPath, progressCB )
        pkg = package.Package( treeName, creationTime, packageFiles )
        if not self.dryRun:
            self.outVerbose( "Uploading package definition for {}", treeName )
            self.pkgStore.putToJson( self.__treeNamePath( self.pkgStore, treeName ), pkg, package.PackageJS() )
        return pkg

    def uploadMany( self, treeName, creationTime, commonLocalPath, variantsLocalPath, progressCB ):
        """Creates multiple package for the content of commonPath including variantsLocalPath files
        for each directory found in the variantsLocalPath.

        This uploads the package definition and any file chunks not already uploaded.
        progressCB will be called with parameters (bytesUploaded,bytesCached) as the upload progresses

        """
        packageFiles = self.__storeFiles( self.pkgStore, commonLocalPath, progressCB )

        for variantPath in os.listdir(variantsLocalPath):
            if os.path.isdir(os.path.join(variantsLocalPath, variantPath )):
                variantTreeName = treeName + ":" + variantPath
                indivisualPackageFiles = self.__storeFiles( self.pkgStore, os.path.join( variantsLocalPath, variantPath ), progressCB )
                # merge the common + indivisual package
                mergedPackageFiles = packageFiles + indivisualPackageFiles
                pkg = package.Package( variantTreeName, creationTime, mergedPackageFiles)
                self.pkgStore.putToJson( self.__treeNamePath( self.pkgStore, variantTreeName ), pkg, package.PackageJS() )

    def find( self, treeName ):
        """Return the package definition for the given name"""
        return self.pkgStore.getFromJson( self.__treeNamePath( self.pkgStore, treeName ), package.PackageJS() )

    def list( self ):
        """Returns the available packages names"""
        return self.pkgStore.list( TREES_PATH )

    def remove( self, treeName ):
        """Removes a tree from the store"""
        return self.pkgStore.remove( self.__treeNamePath( self.pkgStore, treeName ) )

    def rename( self, fromTreeName, toTreeName ):
        """Renames a tree in the store"""
        fromPath = self.__treeNamePath( self.pkgStore, fromTreeName )
        toPath = self.__treeNamePath( self.pkgStore, toTreeName )
        pkg = self.pkgStore.getFromJson( fromPath, package.PackageJS() )
        pkg.name = toTreeName
        self.pkgStore.putToJson( toPath, pkg, package.PackageJS() )
        self.pkgStore.remove( fromPath )

    def verify( self, pkg ):
        """confirms that all data for the given package is present in the store"""
        self.__verifyStore( self.pkgStore, pkg )

    def verifyLocal( self, pkg ):
        """confirms that all data for the given package is present in the local cache"""
        self.__verifyStore( self.localCache, pkg )

    def download( self,  pkg, progressCB ):
        """downloads all data not already present to the local cache

        progressCB will be called with parameters (bytesDownloaded,bytesFromCache) as the download progresses

        """
        for pf in pkg.files:
            for chunk in pf.chunks:
                cpath = self.__chunkPath( self.pkgStore, chunk.sha1, chunk.encoding )
                lpath = self.__chunkPath( self.localCache, chunk.sha1, chunk.encoding )

                if self.localCache.exists( lpath ):
                    progressCB( 0, chunk.size )
                else:
                    if not self.dryRun:
                        self.outVerbose( "Fetching chunk {} to local cache", chunk.sha1 )
                        buf = self.pkgStore.get( cpath )
                        self.__checkSha1( self.__decompress( buf, chunk.encoding ), chunk.sha1, cpath )
                        self.localCache.put( lpath, buf )
                    progressCB( chunk.size, 0 )

    def downloadHttp( self, pkg, progressCB ):
        """downloads all data not already present to the local cache, using http.

        This requires that pkg already has embedded urls, created with the addUrls method
        progressCB will be called with parameters (bytesDownloaded,bytesFromCache) as the download progresses

        """
        for pf in pkg.files:
            for chunk in pf.chunks:
                lpath = self.__chunkPath( self.localCache, chunk.sha1, chunk.encoding )

                if self.localCache.exists( lpath ):
                    progressCB( 0, chunk.size )
                else:
                    resp = requests.get( chunk.url )
                    resp.raise_for_status()
                    buf = resp.content
                    self.__checkSha1( self.__decompress( buf, chunk.encoding ), chunk.sha1, lpath )
                    self.localCache.put( lpath, buf )
                    progressCB( chunk.size, 0 )

    def install( self, pkg, localPath, progressCB ):
        """installs the given package into the local path

        progressCB will be called with parameters (nBytes) as the installation progresses,

        """
        installTime = datetime.datetime.now()

        for pf in pkg.files:
            targetPath = os.path.join( localPath, pf.path )
            targetDir = os.path.dirname(targetPath)

            if not os.path.exists( targetDir ):
                os.makedirs( targetDir )

            f = None
            try:
                filesha1 = hashlib.sha1()
                with tempfile.NamedTemporaryFile(delete=False,dir=targetDir) as f:
                    for chunk in pf.chunks:
                        cpath = self.__chunkPath( self.localCache, chunk.sha1, chunk.encoding )
                        buf = self.localCache.get( cpath )
                        buf = self.__decompress( buf, chunk.encoding )
                        filesha1.update( buf )
                        self.__checkSha1( buf, chunk.sha1, cpath )
                        f.write( buf )
                        progressCB( len(buf) )

                    # Need to flush both at libc and kernel layers here to
                    # ensure that the os.rename() below works correctly under
                    # windows
                    f.flush()
                    os.fsync(f.fileno())

                if filesha1.hexdigest() != pf.sha1:
                    raise RuntimeError, "sha1 for {0} doesn't match".format(pf.path)

                self.outVerbose( "Installing {}", targetPath )
                os.rename( f.name, targetPath )
            except:
                if f: os.unlink( f.name )
                raise

            # write details of the install
            writeInstallProperties( localPath, InstallProperties( pkg.name, installTime ) )

    def compareInstall( self, pkg, localPath ):
        """
        Compares the package against the files installed in the given directory.
        Returns an object with 3 fields:

               result.missing - paths of files present in the package but missing on disk
               result.extra   - paths of files present on disk, but missing in the package
               result.diffs   - paths with different content
        """
        
        # 1) check that the list of files installed matches the list in the package
        installedFiles = []
        for root, dirs, files in os.walk(localPath):
            for file in files:
                installedFiles.append(os.path.relpath( os.path.join(root, file), localPath ))
        installedFiles = set(installedFiles)
        if S3TS_PROPERTIES in installedFiles:
            installedFiles.remove( S3TS_PROPERTIES )

        packageFiles = [f.path for f in pkg.files]
        packageFiles = set(packageFiles)

        class Result: pass

        result = Result()
        result.missing = packageFiles.difference(installedFiles)
        result.extra = installedFiles.difference(packageFiles)
        result.diffs = set()

        # 2) verify the contents of each file
        for pf in pkg.files:
            if pf.path in installedFiles:
                path = os.path.join(localPath, pf.path)
                filesha1 = hashlib.sha1()
                i = 0
                with open( path, 'rb' ) as f:
                    for chunk in pf.chunks:
                        buf = f.read( self.config.chunkSize )
                        chunksha1 = hashlib.sha1()
                        chunksha1.update(buf)
                        filesha1.update(buf)
                        if chunksha1.hexdigest() != chunk.sha1:
                            result.diffs.add( pf.path )
                        i += len(buf)
                if filesha1.hexdigest() != pf.sha1:
                    result.diffs.add( pf.path )

        return result

    def addUrls( self, pkg, expiresInSecs ):
        """Update the given package so that it can be accessed directly via pre-signed http urls"""
        for pf in pkg.files:
            for chunk in pf.chunks:
                cpath = self.__chunkPath( self.pkgStore, chunk.sha1, chunk.encoding )
                chunk.url = self.pkgStore.url( cpath, expiresInSecs )

    def prime( self, localPath, progressCB ):
        """Walk a local directory tree and ensure that all chunks of all files are present in the local cache"""
        self.__storeFiles( self.localCache, localPath, progressCB )

    def validateLocalCache(self):
        return self.__validateStore( self.localCache )

    def validateStore(self):
        return self.__validateStore( self.store )

    def flushLocalCache(self, packageNames ):
        """
        Remove all chunks from the local cache that are not
        referenced by the named packages. Returns the chunks removed.
        """
        packages = [ self.find(pname) for pname in packageNames]
        if len(packages) == 0:
            raise RuntimeError, "flushLocalCache refuses to remove all cached chunks"
        return self.__flushStore( self.localCache, packages )

    def flushStore(self):
        """
        Remote any "dangling" chunks from the store that are not referenced by packages.
        This will happen when packages are deleted Returns the chunks removed.
        """
        packageNames = self.pkgStore.list(TREES_PATH)
        packages = [ self.find(packageName) for packageName in packageNames ]
        return self.__flushStore( self.pkgStore, packages )

    def __validateStore( self, fileStore ):
        """Walk a fileStore and ensure that all chunks are valid sha1 """
        fileList = fileStore.list("")
        corruptedFiles = []
        for fileName in fileList:
            token = fileName.rsplit('/', 2)
            sha1 = token[1] + token[2]
            encoding = package.ENCODING_RAW
            if "zlib" in fileName:
                encoding = package.ENCODING_ZLIB

            buf = fileStore.get(fileName)
            try:
                self.__checkSha1(self.__decompress(buf, encoding), sha1, fileName)
            except:
                corruptedFiles.append({fileName, fileStore.getMetadata(fileName)})
        return corruptedFiles

    def __verifyStore( self, fileStore, pkg ):
        """Walk a fileStore and ensure that all chunks for the given package are present"""
        for pf in pkg.files:
            for chunk in pf.chunks:
                cpath = self.__chunkPath( fileStore, chunk.sha1, chunk.encoding )
                if not fileStore.exists( cpath ):
                    raise RuntimeError, "{0} not found".format(cpath)

    def __flushStore( self, fileStore, packages ):
        """Remove all keys from the store except those referenced by the given packages"""

        # Generate the keys for all chunks in those packages
        keysToKeep = set()
        for pkg in packages:
            for pf in pkg.files:
                for chunk in pf.chunks:
                    keysToKeep.add( (chunk.encoding,chunk.sha1) )

        # Generate the set of all keys currently in the store
        allKeys = set()
        for path in fileStore.list( CHUNKS_PATH ):
            encoding,s1,s2 = fileStore.splitPath(path)
            allKeys.add( (encoding,s1+s2))

        # Work out the keys to remove
        keysToRemove = allKeys.difference( keysToKeep )

        self.outVerbose( "{} packages reference {} chunks...".format(len(packages),len(keysToKeep)))
        self.outVerbose( "The store contains {} chunks, removing {}".format(len(allKeys),len(keysToRemove)))

        # remove them
        if not self.dryRun:
            for encoding,sha1 in keysToRemove:
                fileStore.remove( self.__chunkPath(fileStore, sha1, encoding) )
        return keysToRemove
            
    def __storeFiles( self, store, localPath, progressCB ):
        if not os.path.isdir( localPath ):
            raise IOError( "directory {0} doesn't exist".format( localPath ) )
        packageFiles = []
        for root, dirs, files in os.walk(localPath):
            for file in files:
                rpath = os.path.relpath( os.path.join(root, file), localPath )
                if rpath == S3TS_PROPERTIES:
                  continue
                packageFiles.append( self.__storeFile( store, localPath, rpath, progressCB ) )
        return packageFiles

    def __storeFile( self, store, root, rpath, progressCB ):
        filesha1 = hashlib.sha1()
        chunks = []
        self.outVerbose( "Processing file {}", rpath )
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
                chunks.append( self.__storeChunk( store, chunksha1.hexdigest(), encoding, buf, size, progressCB ) )
        self.outVerbose( "file {} has hash {}", rpath, filesha1.hexdigest() )
        #make the package path consistent and have only forward slash
        rpath = rpath.replace("\\", "/")
        return package.PackageFile( filesha1.hexdigest(), rpath, chunks )

    def __storeChunk( self, store, sha1, encoding, buf, size, progressCB ):
        cpath = self.__chunkPath( store, sha1, encoding )

        if store.exists( cpath ):
            progressCB( 0, size )
        else:
            if not self.dryRun:
                self.outVerbose( "Uploading {} chunk with hash {}", encoding, sha1  )
                store.put( cpath, buf )
            progressCB( size, 0 )
        return package.FileChunk( sha1, size, encoding, None )

    def __treeNamePath( self, store, treeName ):
        return store.joinPath( TREES_PATH, treeName )

    def __chunkPath( self, store, sha1, encoding ):
        enc = {
            package.ENCODING_RAW : RAW_PATH,
            package.ENCODING_ZLIB : ZLIB_PATH,
        }[encoding]
        return store.joinPath( CHUNKS_PATH, enc, sha1[:2], sha1[2:] )

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
