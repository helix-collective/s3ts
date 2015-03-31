import os, tempfile, unittest, shutil, subprocess

from s3ts.filestore import LocalFileStore
from s3ts.s3filestore import S3FileStore
from s3ts.config import TreeStoreConfig
from s3ts.treestore import TreeStore

import boto
import logging

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

        # Confirm it's in the index
        self.assertEquals( treestore.list(), ['v1.0'] )

        # Verify it
        treestore.verify( 'v1.0' )

        # Download it
        treestore.download( 'v1.0' )

        # Install it
        destTree = os.path.join( self.workdir, 'dest-1' )
        treestore.install( 'v1.0', destTree )

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

        # Confirm it's in the index
        self.assertEquals( treestore.list(), ['v1.0'] )

        # Verify it
        treestore.verify( 'v1.0' )

        # Download it
        treestore.download( 'v1.0' )

        # Install it
        destTree = os.path.join( self.workdir, 'dest-1' )
        treestore.install( 'v1.0', destTree )

        # Check that the installed tree is the same as the source tree
        self.assertEquals( subprocess.call( 'diff -r {0} {1}'.format(self.srcTree,destTree), shell=True ), 0 )

def makeEmptyDir( path ):
    if os.path.exists( path ):
        shutil.rmtree( path )
    os.makedirs( path )
    return path
                       

        
if __name__ == '__main__':
    unittest.main()
