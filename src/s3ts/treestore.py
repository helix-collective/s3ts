import os, hashlib, zlib, tempfile, datetime, time, shutil
import requests
            
from s3ts.config import TreeStoreConfig, TreeStoreConfigJS, InstallProperties, writeInstallProperties, S3TS_PROPERTIES
from s3ts import package, filewriter, utils, metapackage

CONFIG_PATH = 'config'
TREES_PATH = 'trees'
META_TREES_PATH = 'meta'
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

    def upload( self, treeName, description, creationTime, localPath, progressCB ):
        """Creates a package for the content of localPath.

        This uploads the package definition and any file chunks not already uploaded.
        progressCB will be called with parameters (bytesUploaded,bytesCached) as the upload progresses

        """
        packageFiles = self.__storeFiles( self.pkgStore, localPath, progressCB )
        pkg = package.Package( treeName, description, creationTime, packageFiles )
        if not self.dryRun:
            self.outVerbose( "Uploading package definition for {}", treeName )
            self.pkgStore.putToJson( self.__treeNamePath( self.pkgStore, treeName ), pkg, package.PackageJS() )
        return pkg

    def uploadMetaPackage(self, meta):
        """ Upload a meta package to the store.
        """
        self.pkgStore.putToJson( self.__metaTreeNamePath( self.pkgStore, meta.name ), meta, metapackage.MetaPackageJS() )

    def uploadMany( self, treeName, description, creationTime, commonLocalPath, variantsLocalPath, progressCB ):
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
                pkg = package.Package( variantTreeName, description, creationTime, mergedPackageFiles)
                self.pkgStore.putToJson( self.__treeNamePath( self.pkgStore, variantTreeName ), pkg, package.PackageJS() )

    def createMerged( self, treeName, creationTime, packageMap ):
        """Create a new package by merging together existing packages.

        `packageMap` is a dictionary with subdirectories as keys, and
        package names as values.
        """
        files = []
        description = "merged package:"
        
        for subdir,subTreeName in packageMap.items():
            subpackage = self.findPackage( subTreeName )
            for file in subpackage.files:
                path = package.pathFromFileSystem( os.path.normpath( os.path.join(subdir, file.path) ) )
                files.append( package.PackageFile( file.sha1, path, file.chunks ) )
            description += "\n    {} : {} (created {})".format( subdir, subTreeName, subpackage.creationTime.isoformat() )

        pkg = package.Package( treeName, description, creationTime, files )
        if not self.dryRun:
            self.outVerbose( "Uploading package definition for {}", treeName )
            self.pkgStore.putToJson( self.__treeNamePath( self.pkgStore, treeName ), pkg, package.PackageJS() )
        return pkg
        
    def find( self, treeName, metadata ):
        """
        Return the package definition for the given name.

        Metapackages will be detected, and the equivalent
        regular package will be returned.
        """
        try:
            return self.findMetaPackage(treeName).package(self, metadata)
        except KeyError:
            return self.findPackage(treeName)

    def findPackage( self, treeName ):
        """Return the package definition for the given name"""
        return self.pkgStore.getFromJson( self.__treeNamePath( self.pkgStore, treeName ), package.PackageJS() )

    def findMetaPackage( self, metaTreeName ):
        """Return the meta package definition for the given name"""
        return self.pkgStore.getFromJson( self.__metaTreeNamePath( self.pkgStore, metaTreeName ), metapackage.MetaPackageJS() )

    def listPackages( self ):
        """Returns the available packages names"""
        return self.pkgStore.list( TREES_PATH )

    def listMetaPackages( self ):
        """Returns the available meta packages names"""
        return self.pkgStore.list( META_TREES_PATH )

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


    def sync( self, pkg, localPath, progressCB ):
        """synchronise the content of localpath with the given package,
        reusing existing files where possible.

        progressCB will be called with parameters (nBytes) as the installation progresses,
        """
        try:
            existingPkg = package.readInstallPackage(localPath)
        except IOError:
            existingPkg = None

        if not existingPkg:
            # Start from scratch
            syncPkg,pathsToRemove = pkg,[]
            if os.path.exists( localPath ):
                shutil.rmtree( localPath )
            os.makedirs( localPath )
        else:
            # Synchronise the existing content. To ensure the
            # drectory is clean, we remove every exising file that
            # is not present in the new pkg
            syncPkg,_ = package.packageDiff( existingPkg, pkg )
            localPaths = set( utils.allFilePaths(localPath) )

            # normpath is required here to turn a package path (delimited by '/')
            # into a local filesystem path (delimited by '\' on windows)
            targetPaths = set( [os.path.normpath(f.path) for f in pkg.files] )
            pathsToRemove = localPaths.difference(targetPaths)
            pathsToRemove.discard(package.S3TS_PACKAGEFILE)
            
            # Remove the existing package from disk, so that if anything
            # fails during the sync, we start from scratch next time
            os.unlink( os.path.join( localPath, package.S3TS_PACKAGEFILE ) )

        installTime = datetime.datetime.now()
        # Remove existing files first, and any empty directories
        # to ensure that if we replace a directory with a file,
        # it's gone by the time we install it
        for path in pathsToRemove:
            path = os.path.join( localPath, path )
            self.outVerbose( "removing {}", path )
            os.unlink( path )
        utils.removeEmptyDirectories(localPath, removeRoot=False)
        self.__install( syncPkg, localPath, progressCB )
        package.writeInstallPackage( localPath, pkg )
        writeInstallProperties( localPath, InstallProperties( pkg.name, installTime ) )
            
    def install( self, pkg, localPath, progressCB ):
        """installs the given package into the local path

        progressCB will be called with parameters (nBytes) as the installation progresses,
        """
        installTime = datetime.datetime.now()
        self.__install( pkg, localPath, progressCB )
        writeInstallProperties( localPath, InstallProperties( pkg.name, installTime ) )

    def __install( self, pkg, localPath, progressCB ):

        for pf in pkg.files:
            targetPath = os.path.join( localPath, pf.path )
            targetDir = os.path.dirname(targetPath)

            if not os.path.exists( targetDir ):
                os.makedirs( targetDir )

            filesha1 = hashlib.sha1()
            # We can update the file in place, because we never install
            # to a directory tree that is in use.
            with filewriter.InPlaceFileWriter(targetPath) as f:
                for chunk in pf.chunks:
                    cpath = self.__chunkPath( self.localCache, chunk.sha1, chunk.encoding )
                    buf = self.localCache.get( cpath )
                    buf = self.__decompress( buf, chunk.encoding )
                    filesha1.update( buf )
                    f.write( buf )
                    progressCB( len(buf) )

            if filesha1.hexdigest() != pf.sha1:
                raise RuntimeError, "sha1 for {0} doesn't match".format(pf.path)

            self.outVerbose( "Wrote {}", targetPath )

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
        installedFiles.discard( S3TS_PROPERTIES )
        installedFiles.discard( package.S3TS_PACKAGEFILE )

        packageFiles = [os.path.normpath(f.path) for f in pkg.files]
        packageFiles = set(packageFiles)

        class Result: pass

        result = Result()
        result.missing = packageFiles.difference(installedFiles)
        result.extra = installedFiles.difference(packageFiles)
        result.diffs = set()

        # 2) verify the contents of each file
        for pf in pkg.files:
            ppath = os.path.normpath(pf.path)
            if ppath in installedFiles:
                path = os.path.join(localPath, ppath)
                filesha1 = hashlib.sha1()
                i = 0
                with open( path, 'rb' ) as f:
                    for chunk in pf.chunks:
                        buf = f.read( self.config.chunkSize )
                        chunksha1 = hashlib.sha1()
                        chunksha1.update(buf)
                        filesha1.update(buf)
                        if chunksha1.hexdigest() != chunk.sha1:
                            result.diffs.add( ppath )
                        i += len(buf)
                if filesha1.hexdigest() != pf.sha1:
                    result.diffs.add( ppath )

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
        packages = [ self.findPackage(pname) for pname in packageNames]
        if len(packages) == 0:
            raise RuntimeError, "flushLocalCache refuses to remove all cached chunks"
        return self.__flushStore( self.localCache, packages )

    def flushStore(self):
        """
        Remote any "dangling" chunks from the store that are not referenced by packages.
        This will happen when packages are deleted Returns the chunks removed.
        """
        packageNames = self.pkgStore.list(TREES_PATH)
        packages = [ self.findPackage(packageName) for packageName in packageNames ]
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
                chunks.append( self.__storeChunk( store, chunksha1.hexdigest(), buf, progressCB ) )
        self.outVerbose( "file {} has hash {}", rpath, filesha1.hexdigest() )
        rpath = package.pathFromFileSystem( rpath )
        return package.PackageFile( filesha1.hexdigest(), rpath, chunks )

    def __storeChunk( self, store, sha1, buf, progressCB ):
        size = len(buf)
        if store.exists( self.__chunkPath( store, sha1, package.ENCODING_RAW ) ):
            progressCB( 0, size )
            return package.FileChunk( sha1, size, package.ENCODING_RAW, None )
        elif store.exists( self.__chunkPath( store, sha1, package.ENCODING_ZLIB ) ):
            progressCB( 0, size )
            return package.FileChunk( sha1, size, package.ENCODING_ZLIB, None )
        else:
            encoding = package.ENCODING_RAW
            if self.config.useCompression:
                buf,encoding = self.__compress( buf )
            if not self.dryRun:
                self.outVerbose( "Uploading {} chunk with hash {}", encoding, sha1  )
                store.put( self.__chunkPath( store, sha1, encoding ), buf )
            progressCB( size, 0 )
            return package.FileChunk( sha1, size, encoding, None )

    def __treeNamePath( self, store, treeName ):
        return store.joinPath( TREES_PATH, treeName )

    def __metaTreeNamePath( self, store, metaTreeName ):
        return store.joinPath( META_TREES_PATH, metaTreeName )

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
