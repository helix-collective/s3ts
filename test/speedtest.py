import tempfile, shutil, os, datetime, resource, sys, time

from s3ts.treestore import TreeStore
from s3ts.filestore import LocalFileStore
from s3ts.config import TreeStoreConfig

class TransferStats:
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
        print "{:,} bytes transferred".format(self.bytesUploaded)
        print "{:,} bytes cached".format(self.bytesCached)
        print "{} seconds elapsed time".format((self.time1 - self.time0))
        print "{} seconds user time".format((self.rusage1.ru_utime - self.rusage0.ru_utime))
        print "{} seconds system time".format((self.rusage1.ru_stime - self.rusage0.ru_stime))

class InstallStats:
    def __init__(self):
        self.bytesInstalled = 0
        self.rusage0 = resource.getrusage(resource.RUSAGE_SELF)
        self.time0 = time.time()
        
    def progress(self, bytesInstalled):
        self.bytesInstalled += bytesInstalled

    def done(self):
        self.rusage1 = resource.getrusage(resource.RUSAGE_SELF)
        self.time1 = time.time()
        print "{:,} bytes copied".format(self.bytesInstalled)
        print "{} seconds elapsed time".format((self.time1 - self.time0))
        print "{:,} MB per second".format(int((self.bytesInstalled/(self.time1 - self.time0))/1e6))
        print "{} seconds user time".format((self.rusage1.ru_utime - self.rusage0.ru_utime))
        print "{} seconds system time".format((self.rusage1.ru_stime - self.rusage0.ru_stime))


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
        stats = TransferStats()
        ts.upload( "test", creationTime, testContent, stats.progress)
        stats.done()
        print
        print "* Repeat upload"
        stats = TransferStats()
        ts.upload( "test", creationTime, testContent, stats.progress)
        stats.done()

        print
        print "* Download"
        stats = TransferStats()
        pkg = ts.find("test")
        ts.download(pkg, stats.progress)
        stats.done()

        print
        print "* Clean installation"
        installDir = os.path.join(dir,"install")
        os.makedirs(installDir)
        stats = InstallStats()
        pkg = ts.find("test")
        ts.install(pkg, installDir, stats.progress)
        stats.done()
            
    finally:
        shutil.rmtree(dir)

def main():
    for testContent in sys.argv[1:]:
        runtest(testContent)
        
if __name__ == '__main__':
    main()

