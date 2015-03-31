import os, tempfile, unittest, shutil, subprocess

from s3ts.filestore import LocalFileStore
from s3ts.s3filestore import S3FileStore
from s3ts.config import TreeStoreConfig
from s3ts.treestore import TreeStore

import boto
import logging

class CaptureProgress:
    def __init__( self ):
        self.recorded = []

    def __call__( self, nBytes ):
        self.recorded.append( nBytes )

        
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

    def tearDown(self):
        # shutil.rmtree( self.workdir )
        pass

    def test_fs_treestore(self):
        # Create a file system backed treestore
        fileStore = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'fs' ) ) )
        localCache = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'cache' ) ) )
        treestore = TreeStore.create( fileStore, localCache, TreeStoreConfig( 100, True ) )
        
        # Upload it as a tree
        treestore.upload( 'v1.0', self.srcTree )
        pkg = treestore.find( 'v1.0' )

        # Confirm it's in the index
        self.assertEquals( treestore.list(), ['v1.0'] )

        # Verify it
        treestore.verify( pkg )

        # Download it, checking we get expected progress callbacks
        cb = CaptureProgress()
        treestore.download( pkg, cb )
        self.assertEquals( cb.recorded, [100, 100, 30, 45, 47] )

        # Install it
        destTree = os.path.join( self.workdir, 'dest-1' )
        treestore.install( pkg, destTree )

        # Check that the installed tree is the same as the source tree
        self.assertEquals( subprocess.call( 'diff -r {0} {1}'.format(self.srcTree,destTree), shell=True ), 0 )

    def test_s3_treestore(self):
        # Create an s3 backed treestore
        # Requires these environment variables set
        #
        #   AWS_ACCESS_KEY_ID
        #   AWS_SECRET_ACCESS_KEY
        #   TS_TEST_S3_BUCKET
        #
        # NB: **this will delete all keys in the test bucket**

        s3c = boto.connect_s3()
        bucket = s3c.get_bucket( os.environ['TS_TEST_S3_BUCKET'] )
        bucket.delete_keys( bucket.list() )

#        boto.set_stream_logger('boto')
        
        fileStore = S3FileStore( bucket )
        localCache = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'cache' ) ) )
        treestore = TreeStore.create( fileStore, localCache, TreeStoreConfig( 100, True ) )
        
        # Upload it as a tree
        treestore.upload( 'v1.0', self.srcTree )
        pkg = treestore.find( 'v1.0' )

        # Confirm it's in the index
        self.assertEquals( treestore.list(), ['v1.0'] )

        # Verify it
        treestore.verify( pkg )

        # Download it, checking we get expected progress callbacks
        cb = CaptureProgress()
        treestore.download( pkg, cb )
        self.assertEquals( cb.recorded, [100, 100, 30, 45, 47] )

        # Install it
        destTree = os.path.join( self.workdir, 'dest-1' )
        treestore.install( pkg, destTree )

        # Check that the installed tree is the same as the source tree
        self.assertEquals( subprocess.call( 'diff -r {0} {1}'.format(self.srcTree,destTree), shell=True ), 0 )

        # Now create a pre-signed version of the package
        pkg = treestore.find( 'v1.0' )
        treestore.addUrls( pkg, 3600 )

        # And download it directly via http. Create a new local cache
        # to ensure that we actually redownload each chunk
        localCache = LocalFileStore( makeEmptyDir( os.path.join( self.workdir, 'cache' ) ) )
        treestore = TreeStore.open( fileStore, localCache )
        cb = CaptureProgress()
        treestore.downloadHttp( pkg, cb )
        self.assertEquals( cb.recorded, [100, 100, 30, 45, 47] )
        
        # Install it
        destTree2 = os.path.join( self.workdir, 'dest-2' )
        treestore.install( pkg, destTree2 )
        
        # Check that the new installed tree is the same as the source tree
        self.assertEquals( subprocess.call( 'diff -r {0} {1}'.format(self.srcTree,destTree2), shell=True ), 0 )
        

def makeEmptyDir( path ):
    if os.path.exists( path ):
        shutil.rmtree( path )
    os.makedirs( path )
    return path
                       

        
if __name__ == '__main__':
    unittest.main()
