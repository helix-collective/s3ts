import os, sys, argparse, json, datetime

import boto

from s3ts.treestore import TreeStore, TreeStoreConfig
from s3ts.filestore import FileStore, LocalFileStore
from s3ts.s3filestore import S3FileStore
from s3ts.package import PackageJS

def getEnv( name, desc ):
    try:
        return os.environ[name]
    except KeyError:
        sys.stderr.write( 'The environment variable {0} must be set to {1}\n'.format( name, desc ) )
        sys.exit(1)

class UploadProgress(object):
    def __init__( self ):
        self.cumTransferred = 0
        self.cumCached = 0

    def __call__( self, nDownloaded, nCached ):
        self.cumTransferred += nDownloaded
        self.cumCached += nCached
        sys.stdout.write( "\r{0} transferred + {1} cached".format( self.cumTransferred, self.cumCached ) )
        sys.stdout.flush()

class DownloadProgress(object):
    def __init__( self, pkg ):
        self.size = pkg.size()
        self.cumTransferred = 0
        self.cumCached = 0

    def __call__( self, nDownloaded, nCached ):
        self.cumTransferred += nDownloaded
        self.cumCached += nCached
        sys.stdout.write( "\r{0} transferred + {1} cached / {2} total".format( self.cumTransferred, self.cumCached, self.size ) )
        sys.stdout.flush()

class InstallProgress(object):
    def __init__( self, pkg ):
        self.size = pkg.size()
        self.cumSize = 0

    def __call__( self, nbytes ):
        self.cumSize += nbytes
        sys.stdout.write( "\r{0} / {1} installed".format( self.cumSize, self.size ) )
        sys.stdout.flush()

def outVerbose( formatStr, *args ):
    sys.stdout.write( "\r" + formatStr.format(*args) + "\n" )
    sys.stdout.flush()

def connectToBucket():
    awsAccessKeyId = getEnv( 'AWS_ACCESS_KEY_ID', 'the AWS access key id' )
    awsSecretAccessKey = getEnv( 'AWS_SECRET_ACCESS_KEY', 'the AWS secret access key' )
    bucketName = getEnv( 'S3TS_BUCKET', 'the AWS S3 bucket used for tree storage'  )
    s3c = boto.connect_s3(awsAccessKeyId,awsSecretAccessKey)
    return s3c.get_bucket( bucketName )

def createTreeStore(chunksize):
    localCacheDir = getEnv( 'S3TS_LOCALCACHE', 'the local directory used for caching'  )
    bucket = connectToBucket()
    config = TreeStoreConfig( chunksize, True )
    return TreeStore.create( S3FileStore(bucket), LocalFileStore(localCacheDir), config )

def openTreeStore(dryRun=False,verbose=False):
    localCacheDir = getEnv( 'S3TS_LOCALCACHE', 'the local directory used for caching'  )
    bucket = connectToBucket()
    treeStore = TreeStore.open( S3FileStore(bucket), LocalFileStore(localCacheDir) )
    treeStore.setDryRun(dryRun)
    if verbose:
        treeStore.setOutVerbose( outVerbose )
    return treeStore

def nonS3TreeStore():
    # Don't use or require S3 - some operations won't be available
    localCacheDir = getEnv( 'S3TS_LOCALCACHE', 'the local directory used for caching'  )
    return TreeStore( FileStore(), LocalFileStore(localCacheDir), None )

def readPackageFile( packageFile ):
    with open( packageFile, 'r' ) as f:
        return PackageJS().fromJson( json.loads( f.read() ) )

def init( chunksize ):
    treeStore = createTreeStore(chunksize)

def list():
    treeStore = openTreeStore()
    for treeName in treeStore.list():
        print treeName

def remove( treename, confirmed ):
    if not confirmed:
        s = raw_input( "Really remove tree '{}' (Y/N) ? [N] ".format(treename) )
        confirmed = s =='Y'
    if confirmed:
        print "Removing {}".format( treename )
        treestore = openTreeStore()
        treestore.remove( treename )
    else:
        print "Cancelled"

def rename( fromtreename, totreename ):
    treestore = openTreeStore()
    treestore.rename( fromtreename, totreename )

def info( treename ):
    treeStore = openTreeStore()
    pkg = treeStore.find( treename )
    print 'Package:', treename
    print 'Created At:', pkg.creationTime.isoformat()
    print 'Total Size: {0} bytes'.format(pkg.size())
    print 'Files:'
    for pf in pkg.files:
        print '    {0} ({1} chunks, {2} bytes)'.format( pf.path, len(pf.chunks), pf.size() )

def upload( treename, localdir, dryRun, verbose ):
    creationTime = datetime.datetime.now()
    treeStore = openTreeStore(dryRun=dryRun,verbose=verbose)
    treeStore.upload( treename, creationTime, localdir, UploadProgress() )
    print

def uploadMany( treename, localdir, kioskDir ):
    creationTime = datetime.datetime.now()
    treeStore = openTreeStore()
    treeStore.uploadMany(treename, creationTime, localdir, kioskDir, UploadProgress())
    print

def download( treename, dryRun, verbose ):
    treeStore = openTreeStore(dryRun=dryRun,verbose=verbose)
    pkg = treeStore.find( treename )
    treeStore.download( pkg, DownloadProgress(pkg) )
    print

def install( treename, localdir, verbose ):
    treeStore = openTreeStore(verbose=verbose)
    pkg = treeStore.find( treename )
    treeStore.download( pkg, DownloadProgress(pkg) )
    print
    treeStore.verifyLocal( pkg )
    treeStore.install( pkg, localdir, InstallProgress(pkg) )
    print

def presign( treename, expirySecs ):
    treeStore = openTreeStore()
    pkg = treeStore.find( treename )
    treeStore.addUrls( pkg, expirySecs )
    print json.dumps( PackageJS().toJson(pkg), sort_keys=True, indent=2, separators=(',', ': ') )

def downloadHttp( packageFile ):
    treeStore = nonS3TreeStore()
    pkg = readPackageFile( packageFile )
    treeStore.downloadHttp( pkg, DownloadProgress(pkg) )
    print

def installHttp( packageFile, localdir ):
    treeStore = nonS3TreeStore()
    pkg = readPackageFile( packageFile )
    treeStore.verifyLocal( pkg )
    treeStore.install( pkg, localdir, InstallProgress(pkg) )
    print

def primeCache( localdir ):
    treeStore = openTreeStore()
    treeStore.prime( localdir, UploadProgress() )

parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(help='commands',dest='commandName')

p = subparsers.add_parser('init', help='Initialise a new store')
p.add_argument('--chunksize', action='store', default=10000000, type=int,
               help='The maximum number of bytes to be stored in each chunk')

p = subparsers.add_parser('list', help='List trees available in the store')

p = subparsers.add_parser( 'remove', help='Remove a tree from the store')
p.add_argument( "--yes", action='store_true', help='Dont ask for confirmation' )
p.add_argument('treename', action='store', help='The name of the tree')

p = subparsers.add_parser( 'rename', help='Rename an existing tree in the sore')
p.add_argument('fromtreename', action='store', help='The name of the src tree')
p.add_argument('totreename', action='store', help='The name of the target tree')

p = subparsers.add_parser('info', help='Show information about a tree')
p.add_argument('treename', action='store', help='The name of the tree')

p = subparsers.add_parser('upload', help='Upload a tree from the local filesystem')
p.set_defaults(dryRun=False,verbose=False)
p.add_argument('--dry-run', dest='dryRun', action='store_true')
p.add_argument('--verbose', dest='verbose', action='store_true')
p.add_argument('treename', action='store', help='The name of the tree')
p.add_argument('localdir', action='store', help='The local directory path')

p = subparsers.add_parser('download', help='Download a tree to the local cache')
p.set_defaults(dryRun=False,verbose=False)
p.add_argument('--dry-run', dest='dryRun', action='store_true')
p.add_argument('--verbose', dest='verbose', action='store_true')
p.add_argument('treename', action='store', help='The name of the tree')

p = subparsers.add_parser('install', help='Download/Install a tree into the filesystem')
p.set_defaults(verbose=False)
p.add_argument('--verbose', dest='verbose', action='store_true')
p.add_argument('treename', action='store', help='The name of the tree')
p.add_argument('localdir', action='store', help='The local directory path')

p = subparsers.add_parser('presign', help='Generate a package definition containing presigned urls ')
p.add_argument('treename', action='store', help='The name of the tree')
p.add_argument('--expirySecs', action='store', default=3600, type=int,
                            help='Validity of the presigned URLs in seconds')

p = subparsers.add_parser('download-http', help='Download a tree to the local cache using a presigned package file')
p.add_argument('pkgfile', action='store', help='The file containing the package definition')

p = subparsers.add_parser('install-http', help='Install a tree from local cache using a presigned package file')
p.add_argument('pkgfile', action='store', help='The file containing the package definition')
p.add_argument('localdir', action='store', help='The local directory path')

p = subparsers.add_parser('prime-cache', help='Prime the local cache with the contents of a local directory')
p.add_argument('localdir', action='store', help='The local directory path')

p = subparsers.add_parser('upload-many', help='Upload multiple trees from the local filesystem')
p.add_argument('treename', action='store', help='The name of the tree')
p.add_argument('localdir', action='store', help='The local directory path')
p.add_argument('local_variant_dir', action='store', help='The local variant path')

def main():
    args = parser.parse_args()
    if args.commandName == 'init':
        init( args.chunksize )
    elif args.commandName == 'list':
        list()
    elif args.commandName == 'remove':
        remove( args.treename, args.yes )
    elif args.commandName == 'rename':
        rename( args.fromtreename, args.totreename )
    elif args.commandName == 'info':
        info( args.treename )
    elif args.commandName == 'upload':
        upload( args.treename, args.localdir, args.dryRun, args.verbose )
    elif args.commandName == 'download':
        download( args.treename, args.dryRun, args.verbose )
    elif args.commandName == 'install':
        install( args.treename, args.localdir, args.verbose )
    elif args.commandName == 'presign':
        presign( args.treename, args.expirySecs )
    elif args.commandName == 'download-http':
        downloadHttp( args.pkgfile )
    elif args.commandName == 'install-http':
        installHttp( args.pkgfile, args.localdir )
    elif args.commandName == 'prime-cache':
        primeCache( args.localdir )
    elif args.commandName == 'upload-many':
        uploadMany(args.treename, args.localdir, args.local_variant_dir)

if __name__ == '__main__':
    main()
