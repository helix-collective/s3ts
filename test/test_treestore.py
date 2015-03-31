import os, tempfile, unittest, shutil, subprocess

from s3ts.filestore import FSFileStore
from s3ts.config import TreeStoreConfig
from s3ts.treestore import TreeStore

class TestTreeStore(unittest.TestCase):

    def setUp(self):
        # self.workdir = tempfile.mkdtemp()
        self.workdir = "/tmp/test"
        if os.path.exists( self.workdir ):
            shutil.rmtree( self.workdir )
        os.makedirs( self.workdir )
        
        fileStoreDir = os.path.join( self.workdir, 'fs' )
        localCacheDir = os.path.join( self.workdir, 'cache' )
        fileStore = FSFileStore( fileStoreDir )
        localCache = FSFileStore( localCacheDir )
        self.treeStore = TreeStore.create( fileStore, localCache, TreeStoreConfig( 100, True ) )
        
    def tearDown(self):
        # shutil.rmtree( self.workdir )
        pass

    def test_basic(self):
        # Create some test input data
        srcTree = os.path.join( self.workdir, 'src-1' )
        os.makedirs( srcTree )
        fs = FSFileStore( srcTree )
        fs.put( 'code/file1.py', '#!/bin/env python\n def main(): print "hello"\n' )
        fs.put( 'code/file2.py', '#!/bin/env python\n def main(): print "goodbye"\n' )
        fs.put( 'assets/car-01.db',
                'Some big and complicated data structure goes here, hopefully big enough that it requires chunking and compression.\n'
                'sydney london paris port moresby okinawa st petersburg salt lake city  new york whitehorse mawson woy woy st louis\n'
        )

        # Upload it as a tree
        self.treeStore.upload( 'v1.0', srcTree )

        # Verify it
        self.treeStore.verify( 'v1.0' )

        # Download it
        self.treeStore.download( 'v1.0' )

        # Install it
        destTree = os.path.join( self.workdir, 'dest-1' )
        self.treeStore.install( 'v1.0', destTree )

        # Check that the installed tree is the same as the source tree
        self.assertEquals( subprocess.call( 'diff -r {0} {1}'.format(srcTree,destTree), shell=True ), 0 )

        
if __name__ == '__main__':
    unittest.main()
