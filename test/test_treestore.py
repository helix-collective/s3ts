import os, tempfile, unittest, shutil, subprocess, datetime, time

from s3ts.filestore import LocalFileStore
from s3ts.s3filestore import S3FileStore
from s3ts.config import TreeStoreConfig, readInstallProperties, S3TS_PROPERTIES
from s3ts.treestore import TreeStore
from s3ts.utils import datetimeFromIso
from s3ts.package import PackageJS, S3TS_PACKAGEFILE

import boto
import logging

# boto.set_stream_logger('boto')

class CaptureDownloadProgress:
    def __init__( self ):
        self.recorded = []

    def __call__( self, bytesDownloaded, bytesFromCache ):
        self.recorded.append( bytesDownloaded + bytesFromCache )

CaptureUploadProgress = CaptureDownloadProgress

class CaptureInstallProgress:
    def __init__( self ):
        self.recorded = []

    def __call__( self, nBytes ):
        self.recorded.append( nBytes )


class EmptyS3Bucket:
    def __init__( self, bucket ):
        self.bucket = bucket

    def __enter__(self):
        # Ensure the bucket starts empty
        assert len(list(self.bucket.list()))==0, "S3 bucket is not empty"

    def __exit__(self, type, value, traceback):
        # Clean the bucket (ok, as we know it started empty)
        self.bucket.delete_keys( self.bucket.list() )


class TestTreeStore(unittest.TestCase):

    def setUp(self):
        self.workdir = tempfile.mkdtemp()
        if os.path.exists( self.workdir ):
            shutil.rmtree( self.workdir )
        os.makedirs( self.workdir )

        self.FILE1 = '#!/bin/env python\n def main(): print "hello"\n'
        self.FILE2 = '#!/bin/env python\n def main(): print "goodbye"\n'
        self.FILE2_A = '#!/bin/env python\n def main(): print "goodbye foreever"\n'
        self.FILE3 = '#!/bin/env python\n def main(): print "goodbye foreever"\n'
        self.FILE4 = '#!/bin/env python\n def main(): print "what now"\n' 
        self.FILE5 = 'Just text' 
        self.CAR01 = (
            'Some big and complicated data structure goes here, hopefully big enough that it requires chunking and compression.\n'
            'sydney london paris port moresby okinawa st petersburg salt lake city  new york whitehorse mawson woy woy st louis\n'
            )
        

        # Create some test input data
        self.srcTree = makeEmptyDir( os.path.join( self.workdir, 'src-1' ) )
        fs = LocalFileStore( self.srcTree )
        fs.put( 'code/file1.py', self.FILE1)
        fs.put( 'code/file2.py', self.FILE2)
        fs.put( 'assets/car-01.db', self.CAR01)

        self.srcTree2 = makeEmptyDir( os.path.join( self.workdir, 'src-2' ) )
        fs = LocalFileStore( self.srcTree2 )
        fs.put( 'code/file1.py', self.FILE1 )
        fs.put( 'code/file3.py', self.FILE3 )
        fs.put( 'code/file4.py', self.FILE4)
        fs.put( 'assets/car-01.db', self.CAR01 )

        self.srcTree3 = makeEmptyDir( os.path.join( self.workdir, 'src-3' ) )
        fs = LocalFileStore( self.srcTree3 )
        fs.put( 'code/file1.py', self.FILE1 )
        fs.put( 'code/file2.py', self.FILE2_A )
        fs.put( 'code/file4.py', self.FILE4 )
        fs.put( 'text/text', self.FILE5 )

        self.srcTree4 = makeEmptyDir( os.path.join( self.workdir, 'src-4' ) )
        fs = LocalFileStore( self.srcTree4 )
        fs.put( 'file1.py', self.FILE1 )
        fs.put( 'code/file2.py', self.FILE2_A )
        fs.put( 'code/file4.py', self.FILE4 )
        fs.put( 'text', self.FILE5 )

        self.srcVariant = makeEmptyDir( os.path.join( self.workdir, 'src1-kiosk' ) )
        fs = LocalFileStore( self.srcVariant )
        fs.put( 'kiosk-01/key', 'this is the key src1:kiosk-01' )
        fs.put( 'kiosk-02/key', 'this is the key src1:kiosk-02' )

    def tearDown(self):
        shutil.rmtree( self.workdir )

    def test_fs_treestore(self):
        # Create a file system backed treestore
        fileStore = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'fs' ) ) )
        localCache = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'cache' ) ) )
        treestore = TreeStore.create( fileStore, localCache, TreeStoreConfig( 100, True ) )

        # Upload 2 trees
        creationTime = datetimeFromIso( '2015-01-01T00:00:00.0' )
        treestore.upload( 'v1.0', creationTime, self.srcTree, CaptureUploadProgress() )
        pkg = treestore.find( 'v1.0' )

        # Confirm it's in the index
        self.assertEquals( treestore.list(), ['v1.0'] )

        # Verify it
        treestore.verify( pkg )

        # Test the cache priming function
        treestore.prime( self.srcTree2, CaptureUploadProgress() )

        # Test whether the verifyCache works
        corruptedFiles = treestore.validateLocalCache()
        self.assertEquals( len(corruptedFiles), 0) 
        
        # Download it, checking we get expected progress callbacks
        # The order of the callbacks will depend on the order of the
        # chunks in the package definition, which will depend on the
        # iteration order of the file system when the package was created.
        # So check independently of ordering.
        cb = CaptureDownloadProgress()
        treestore.download( pkg, cb )
        self.assertEquals( sorted(cb.recorded), [30, 45, 47, 100, 100] )

        # Verify it locally
        treestore.verifyLocal( pkg )

        # Install it
        destTree = os.path.join( self.workdir, 'dest-1' )
        treestore.install( pkg, destTree, CaptureInstallProgress() )

        # Check that the installed tree is the same as the source tree
        self.assertEquals( subprocess.call( 'diff -r -x {0} {1} {2}'.format(S3TS_PROPERTIES,self.srcTree,destTree), shell=True ), 0 )

        # Rename the tree, and check that installing that is the same
        treestore.rename( 'v1.0', 'v1.0x' )
        pkg = treestore.find( 'v1.0x' )
        treestore.download( pkg, CaptureDownloadProgress() )
        destTree = os.path.join( self.workdir, 'dest-2' )
        treestore.install( pkg, destTree, CaptureInstallProgress() )
        self.assertEquals( subprocess.call( 'diff -r -x {0} {1} {2}'.format(S3TS_PROPERTIES,self.srcTree,destTree), shell=True ), 0 )

        # Test the flushStore function has nothing to remove)
        treestore.upload( 'extra', creationTime, self.srcTree2, CaptureUploadProgress() )
        removed = treestore.flushStore()
        self.assertEquals(len(removed), 0)

        # Remove a tree
        treestore.remove( 'v1.0x' )

        # Test the store now has dangling chunks when can be removed
        removed = treestore.flushStore()
        self.assertTrue(len(removed) > 0)

        treestore.upload( 'v1.0', creationTime, self.srcTree, CaptureUploadProgress() )

        # Initially the local cache should contain chunks for v1.0 and extra. Empty
        # the local cache by successive flush operations
        removed = treestore.flushLocalCache(['extra'])
        self.assertTrue(len(removed) > 0)
        removed = treestore.flushLocalCache(['v1.0'])
        self.assertTrue(len(removed) > 0)

        # Confirm that removing everything from the local cache is refused
        with self.assertRaises(RuntimeError):
            treestore.flushLocalCache([])

    def test_sync(self):            
        # Create a file system backed treestore
        fileStore = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'fs' ) ) )
        localCache = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'cache' ) ) )
        treestore = TreeStore.create( fileStore, localCache, TreeStoreConfig( 10, True ) )

        creationTime = datetimeFromIso( '2015-01-01T00:00:00.0' )
        treestore.upload( 'v1.0', creationTime, self.srcTree, CaptureUploadProgress() )
        treestore.upload( 'v1.3', creationTime, self.srcTree3, CaptureUploadProgress() )
        treestore.upload( 'v1.4', creationTime, self.srcTree4, CaptureUploadProgress() )

        testdir = makeEmptyDir( os.path.join( self.workdir, 'test' ) )

        def assertExists( path ):
            self.assertTrue( os.path.exists( os.path.join(testdir, path) ) )

        def assertContains( path, text ):
            self.assertEquals( open( os.path.join(testdir, path) ).read(), text )

        def assertDoesntExist( path ):
            self.assertFalse( os.path.exists( os.path.join(testdir, path) ) ) 
        
        # sync a package to an empty directory
        pkg = treestore.find('v1.0')
        treestore.download( pkg, CaptureDownloadProgress() ) 
        treestore.sync( pkg, testdir, CaptureInstallProgress() )
        assertContains( "code/file1.py", self.FILE1 )
        assertContains( "code/file2.py", self.FILE2 )
        assertContains( "assets/car-01.db", self.CAR01 )
        assertExists( S3TS_PACKAGEFILE )

        # Re-sync the same package
        pkg = treestore.find('v1.0')
        treestore.download( pkg, CaptureDownloadProgress() ) 
        treestore.sync( pkg, testdir, CaptureInstallProgress() )
        assertContains( "code/file1.py", self.FILE1 )
        assertContains( "code/file2.py", self.FILE2 )
        assertContains( "assets/car-01.db", self.CAR01 )
        assertExists( S3TS_PACKAGEFILE )

        # Sync to a different package
        pkg = treestore.find('v1.3')
        treestore.download( pkg, CaptureDownloadProgress() ) 
        treestore.sync( pkg, testdir, CaptureInstallProgress() )
        assertContains( "code/file1.py", self.FILE1 )
        assertContains( "code/file2.py", self.FILE2_A )
        assertDoesntExist( "assets/car-01.db" )
        assertContains( "code/file4.py", self.FILE4 )
        assertContains( "text/text", self.FILE5 )
        assertExists( S3TS_PACKAGEFILE )

        # Sync back to the first package
        pkg = treestore.find('v1.0')
        treestore.download( pkg, CaptureDownloadProgress() ) 
        treestore.sync( pkg, testdir, CaptureInstallProgress() )
        assertContains( "code/file1.py", self.FILE1 )
        assertContains( "code/file2.py", self.FILE2 )
        assertContains( "assets/car-01.db", self.CAR01 )
        assertDoesntExist( "code/file4.py" )
        assertExists( S3TS_PACKAGEFILE )

        # Remove the package file, and sync the second package again
        os.unlink( os.path.join( testdir, S3TS_PACKAGEFILE ) )
        pkg = treestore.find('v1.3')
        treestore.download( pkg, CaptureDownloadProgress() ) 
        treestore.sync( pkg, testdir, CaptureInstallProgress() )
        assertContains( "code/file1.py", self.FILE1 )
        assertContains( "code/file2.py", self.FILE2_A )
        assertDoesntExist( "assets/car-01.db" )
        assertContains( "code/file4.py", self.FILE4 )
        assertExists( S3TS_PACKAGEFILE )

        # Sync to test replacing a directory with a file
        pkg = treestore.find('v1.4')
        treestore.download( pkg, CaptureDownloadProgress() ) 
        treestore.sync( pkg, testdir, CaptureInstallProgress() )
        assertContains( "text", self.FILE5 )

    def test_s3_treestore(self):
        # Create an s3 backed treestore
        # Requires these environment variables set
        #
        #   AWS_ACCESS_KEY_ID
        #   AWS_SECRET_ACCESS_KEY
        #   S3TS_BUCKET
        #
        # NB: **this will only work if the bucket is empty

        s3c = boto.connect_s3()
        bucket = s3c.get_bucket( os.environ['S3TS_BUCKET'] )

        with EmptyS3Bucket(bucket):
            fileStore = S3FileStore( bucket )
            localCache = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'cache' ) ) )
            treestore = TreeStore.create( fileStore, localCache, TreeStoreConfig( 100, True ) )

            # Upload it as a tree
            creationTime = datetimeFromIso( '2015-01-01T00:00:00.0' )
            treestore.upload( 'v1.0', creationTime, self.srcTree, CaptureUploadProgress() )
            pkg = treestore.find( 'v1.0' )

            # Confirm it's in the index
            self.assertEquals( treestore.list(), ['v1.0'] )

            # Verify it
            treestore.verify( pkg )

            # Download it, checking we get expected progress callbacks
            cb = CaptureDownloadProgress()
            treestore.download( pkg, cb )
            self.assertEquals( sorted(cb.recorded), [30, 45, 47, 100, 100] )

            # Verify it locally
            treestore.verifyLocal( pkg )

            # Install it
            destTree = os.path.join( self.workdir, 'dest-1' )
            treestore.install( pkg, destTree, CaptureInstallProgress() )

            # Check that the installed tree is the same as the source tree
            self.assertEquals( subprocess.call( 'diff -r -x {0} {1} {2}'.format(S3TS_PROPERTIES,self.srcTree,destTree), shell=True ), 0 )
            self.assertEquals( readInstallProperties(destTree).treeName, 'v1.0' )

            # Use the compareInstall function to confirm the installed package is ok, and
            # then check that modifying the files show up in the comparison
            result = treestore.compareInstall( pkg, destTree )
            self.assertEquals( len(result.missing), 0 )
            self.assertEquals( len(result.extra), 0 )
            self.assertEquals( len(result.diffs), 0 )

            with open( os.path.join(destTree,"code/file1.py"), "w" ) as f:
                f.write("x")
            with open( os.path.join(destTree,"code/file3.py"), "w" ) as f:
                f.write("y")
            os.unlink(os.path.join(destTree,'assets/car-01.db'))
            
            result = treestore.compareInstall( pkg, destTree )
            self.assertEquals( result.missing, set(['assets/car-01.db']) )
            self.assertEquals( result.extra, set(['code/file3.py']) )
            self.assertEquals( result.diffs, set(['code/file1.py']) )

            # Reinstall to fix directory content
            shutil.rmtree( destTree )
            treestore.install( pkg, destTree, CaptureInstallProgress() )
            result = treestore.compareInstall( pkg, destTree )
            self.assertEquals( len(result.missing), 0 )
            self.assertEquals( len(result.extra), 0 )
            self.assertEquals( len(result.diffs), 0 )

            # Now create a pre-signed version of the package
            pkg = treestore.find( 'v1.0' )
            treestore.addUrls( pkg, 3600 )
            self.assertEquals( len(result.missing), 0 )
            self.assertEquals( len(result.extra), 0 )
            self.assertEquals( len(result.diffs), 0 )

            # And download it directly via http. Create a new local cache
            # to ensure that we actually redownload each chunk
            localCache = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'cache' ) ) )
            treestore2 = TreeStore.forHttpOnly( localCache )
            cb = CaptureDownloadProgress()
            treestore2.downloadHttp( pkg, cb )
            self.assertEquals( sorted(cb.recorded), [30, 45, 47, 100, 100] )

            # Install it
            destTree2 = os.path.join( self.workdir, 'dest-2' )
            treestore2.install( pkg, destTree2, CaptureInstallProgress() )

            # Check that the new installed tree is the same as the source tree
            self.assertEquals( subprocess.call( 'diff -r -x {0} {1} {2}'.format(S3TS_PROPERTIES,self.srcTree,destTree2), shell=True ), 0 )

            # Rename the tree, and check that installing that is the same
            treestore.rename( 'v1.0', 'v1.0x' )
            pkg = treestore.find( 'v1.0x' )
            treestore.download( pkg, CaptureDownloadProgress() )
            destTree = os.path.join( self.workdir, 'dest-3' )
            treestore.install( pkg, destTree, CaptureInstallProgress() )
            self.assertEquals( subprocess.call( 'diff -r -x {0} {1} {2}'.format(S3TS_PROPERTIES,self.srcTree,destTree), shell=True ), 0 )

            # Remove the tree
            treestore.remove( 'v1.0x' )
            
    def test_s3_prefixes(self):
        # Requires these environment variables set
        #
        #   AWS_ACCESS_KEY_ID
        #   AWS_SECRET_ACCESS_KEY
        #   S3TS_BUCKET
        #
        # NB: **this will only work if the bucket is empty

        s3c = boto.connect_s3()
        bucket = s3c.get_bucket( os.environ['S3TS_BUCKET'] )

        with EmptyS3Bucket(bucket):
            localCache = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'cache' ) ) )
            treestore1 = TreeStore.create( S3FileStore( bucket, "prefix1" ), localCache, TreeStoreConfig( 100, True ) )
            treestore2 = TreeStore.create( S3FileStore( bucket, "prefix2" ), localCache, TreeStoreConfig( 100, True ) )

            # Confirm we can write the different values to the same path in both treestores,
            # and the different prefix keeps them separate independent
            creationTime = datetimeFromIso( '2015-01-01T00:00:00.0' )
            treestore1.upload( 'release', creationTime, self.srcTree, CaptureUploadProgress() )
            treestore2.upload( 'release', creationTime, self.srcTree2, CaptureUploadProgress() )
            pkg1 = treestore1.find( 'release' )
            pkg2 = treestore2.find( 'release' )
            self.assertEquals(len(pkg1.files),3)
            self.assertEquals(len(pkg2.files),4)

    def test_s3_many_treestore(self):
        # Create an s3 backed treestore
        # Requires these environment variables set
        #
        #   AWS_ACCESS_KEY_ID
        #   AWS_SECRET_ACCESS_KEY
        #   S3TS_BUCKET
        #
        # NB: **this will only work if the bucket is empty

        s3c = boto.connect_s3()
        bucket = s3c.get_bucket( os.environ['S3TS_BUCKET'] )

        with EmptyS3Bucket(bucket):
            fileStore = S3FileStore( bucket )
            localCache = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'cache' ) ) )
            treestore = TreeStore.create( fileStore, localCache, TreeStoreConfig( 100, True ) )

            # Upload it as a tree
            creationTime = datetimeFromIso( '2015-01-01T00:00:00.0' )
            treestore.uploadMany( 'v1.0', creationTime, self.srcTree, self.srcVariant, CaptureUploadProgress() )
            pkg = treestore.find( 'v1.0:kiosk-01' )

            # Confirm it's in the index
            self.assertEquals( treestore.list(), ['v1.0:kiosk-01', 'v1.0:kiosk-02'] )

            # Verify it
            treestore.verify( pkg )

            # Download it, checking we get expected progress callbacks
            cb = CaptureDownloadProgress()
            treestore.download( pkg, cb )
            self.assertEquals( sorted(cb.recorded), [29, 30, 45, 47, 100, 100] )

            # Verify it locally
            treestore.verifyLocal( pkg )

            # Install it
            destTree = os.path.join( self.workdir, 'dest-1' )
            treestore.install( pkg, destTree, CaptureInstallProgress() )

            # Check that the installed tree is the same as the source tree
            self.assertEquals( subprocess.call( 'diff -r -x {0} {1} {2}'.format(S3TS_PROPERTIES,self.srcTree + '/assets',destTree + '/assets'), shell=True ), 0 )
            self.assertEquals( subprocess.call( 'diff -r -x {0} {1} {2}'.format(S3TS_PROPERTIES,self.srcTree + '/code',destTree + '/code'), shell=True ), 0 )


            self.assertEquals( readInstallProperties(destTree).treeName, 'v1.0:kiosk-01' )



def makeEmptyDir( path ):
    if os.path.exists( path ):
        shutil.rmtree( path )
    os.makedirs( path )
    return path



if __name__ == '__main__':
    unittest.main()
