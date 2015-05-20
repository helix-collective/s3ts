import os, tempfile, unittest, shutil, subprocess, datetime

from s3ts.filestore import LocalFileStore
from s3ts.s3filestore import S3FileStore
from s3ts.config import TreeStoreConfig, readInstallProperties, S3TS_PROPERTIES
from s3ts.treestore import TreeStore
from s3ts.utils import datetimeFromIso

import boto
import logging

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
        # self.workdir = tempfile.mkdtemp()
        self.workdir = "/tmp/test"
        if os.path.exists( self.workdir ):
            shutil.rmtree( self.workdir )
        os.makedirs( self.workdir )

        # Create some test input data
        self.srcTree = makeEmptyDir( os.path.join( self.workdir, 'src-1' ) )
        fs = LocalFileStore( self.srcTree )
        fs.put( 'code/file1.py', '#!/bin/env python\n def main(): print "hello"\n' )
        fs.put( 'code/file2.py', '#!/bin/env python\n def main(): print "goodbye"\n' )
        fs.put( 'assets/car-01.db',
                'Some big and complicated data structure goes here, hopefully big enough that it requires chunking and compression.\n'
                'sydney london paris port moresby okinawa st petersburg salt lake city  new york whitehorse mawson woy woy st louis\n'
        )

        self.srcTree2 = makeEmptyDir( os.path.join( self.workdir, 'src-2' ) )
        fs = LocalFileStore( self.srcTree2 )
        fs.put( 'code/file1.py', '#!/bin/env python\n def main(): print "hello!"\n' )
        fs.put( 'code/file3.py', '#!/bin/env python\n def main(): print "goodbye foreever"\n' )
        fs.put( 'assets/car-01.db',
                'Some big and complicated data structure goes here, hopefully big enough that it requires chunking and compression.\n'
                'sydney london paris port moresby okinawa st petersburg salt lake city  new york whitehorse mawson woy woy st louis\n'
        )

        self.srcVariant = makeEmptyDir( os.path.join( self.workdir, 'src1-kiosk' ) )
        fs = LocalFileStore( self.srcVariant )
        fs.put( 'kiosk-01/key', 'this is the key src1:kiosk-01' )
        fs.put( 'kiosk-02/key', 'this is the key src1:kiosk-02' )

    def tearDown(self):
        # shutil.rmtree( self.workdir )
        pass

    def test_fs_treestore(self):
        # Create a file system backed treestore
        fileStore = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'fs' ) ) )
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

        # Test the cache priming function
        treestore.prime( self.srcTree2, CaptureUploadProgress() )

        # Download it, checking we get expected progress callbacks
        cb = CaptureDownloadProgress()
        treestore.download( pkg, cb )
        self.assertEquals( cb.recorded, [100, 100, 30, 45, 47] )

        # Verify it locally
        treestore.verifyLocal( pkg )

        # Install it
        destTree = os.path.join( self.workdir, 'dest-1' )
        treestore.install( pkg, destTree, CaptureInstallProgress() )

        # Check that the installed tree is the same as the source tree
        self.assertEquals( subprocess.call( 'diff -r -x {0} {1} {2}'.format(S3TS_PROPERTIES,self.srcTree,destTree), shell=True ), 0 )

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
            self.assertEquals( cb.recorded, [100, 100, 30, 45, 47] )

            # Verify it locally
            treestore.verifyLocal( pkg )

            # Install it
            destTree = os.path.join( self.workdir, 'dest-1' )
            treestore.install( pkg, destTree, CaptureInstallProgress() )

            # Check that the installed tree is the same as the source tree
            self.assertEquals( subprocess.call( 'diff -r -x {0} {1} {2}'.format(S3TS_PROPERTIES,self.srcTree,destTree), shell=True ), 0 )

            self.assertEquals( readInstallProperties(destTree).treeName, 'v1.0' )

            # Now create a pre-signed version of the package
            pkg = treestore.find( 'v1.0' )
            treestore.addUrls( pkg, 3600 )

            # And download it directly via http. Create a new local cache
            # to ensure that we actually redownload each chunk
            localCache = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'cache' ) ) )
            treestore = TreeStore.forHttpOnly( localCache )
            cb = CaptureDownloadProgress()
            treestore.downloadHttp( pkg, cb )
            self.assertEquals( cb.recorded, [100, 100, 30, 45, 47] )

            # Install it
            destTree2 = os.path.join( self.workdir, 'dest-2' )
            treestore.install( pkg, destTree2, CaptureInstallProgress() )

            # Check that the new installed tree is the same as the source tree
            self.assertEquals( subprocess.call( 'diff -r -x {0} {1} {2}'.format(S3TS_PROPERTIES,self.srcTree,destTree2), shell=True ), 0 )
        
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
            print treestore.list()
            pkg = treestore.find( 'v1.0:kiosk-01' )

            # Confirm it's in the index
            self.assertEquals( treestore.list(), ['v1.0:kiosk-01', 'v1.0:kiosk-02'] )

            # Verify it
            treestore.verify( pkg )

            # Download it, checking we get expected progress callbacks
            cb = CaptureDownloadProgress()
            treestore.download( pkg, cb )
            self.assertEquals( cb.recorded, [100, 100, 30, 45, 47, 29] )

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
