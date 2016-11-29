import tempfile, shutil, os, datetime, resource, sys, time

from s3ts.treestore import TreeStore
from s3ts.filestore import LocalFileStore
from s3ts.config import TreeStoreConfig

class UploadStats:
    def __init__(self):
        self.bytesUploaded = 0
        self.bytesCached = 0
        self.rusage0 = resource.getrusage(resource.RUSAGE_SELF)
        self.time0 = time.time()
        
    def progress(self, bytesUploaded, bytesCached):
        self.bytesUploaded += bytesUploaded
        self.bytesCached += bytesCached

    def done(self):
        self.rusage1 = resource.getrusage(resource.RUSAGE_SELF)
        self.time1 = time.time()
        print "%d bytes uploaded" % self.bytesUploaded
        print "%d bytes cached" % self.bytesCached
        print "%f seconds elapsed time" % (self.time1 - self.time0)
        print "%f seconds user time" % (self.rusage1.ru_utime - self.rusage0.ru_utime)
        print "%f seconds system time" % (self.rusage1.ru_stime - self.rusage0.ru_stime)
        

def runtest(testContent):
    print "----------------------------------------------------------------------"
    print "* path", testContent

    dir = tempfile.mkdtemp()
    try:
        store = LocalFileStore(os.path.join(dir, 'store'))
        cache = LocalFileStore(os.path.join(dir, 'cache'))
        config = TreeStoreConfig(1000000, True)
        ts = TreeStore.create(store, cache, config)

        creationTime = datetime.datetime.now()
        print "* Initial upload"
        stats = UploadStats()
        ts.upload( "test", creationTime, testContent, stats.progress)
        stats.done()
        print
        print "* Repeat upload"
        stats = UploadStats()
        ts.upload( "test", creationTime, testContent, stats.progress)
        stats.done()

    finally:
        shutil.rmtree(dir)

def main():
    for testContent in sys.argv[1:]:
        runtest(testContent)
        
if __name__ == '__main__':
    main()
