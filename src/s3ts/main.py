import os, sys, argparse, json

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

def openTreeStore():
    localCacheDir = getEnv( 'S3TS_LOCALCACHE', 'the local directory used for caching'  )
    bucket = connectToBucket()
    return TreeStore.open( S3FileStore(bucket), LocalFileStore(localCacheDir) )

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

def info( treename ):
    treeStore = openTreeStore()
    pkg = treeStore.find( treename )
    print 'Package:', treename
    print 'Total Size: {0} bytes'.format(pkg.size())
    print 'Files:'
    for pf in pkg.files:
        print '    {0} ({1} chunks, {2} bytes)'.format( pf.path, len(pf.chunks), pf.size() )

def upload( treename, localdir ):
    treeStore = openTreeStore()
    treeStore.upload( treename, localdir )

class PackageProgress(object):
    def __init__( self, action, pkg ):
        self.action = action
        self.size = pkg.size()
        self.cumSize = 0

    def __call__( self, nbytes ):
        self.cumSize += nbytes
        print "\r{0}/{1} bytes {2}".format( self.cumSize, self.size, self.action ),
        
def download( treename ):
    treeStore = openTreeStore()
    pkg = treeStore.find( treename )
    treeStore.download( pkg, PackageProgress("downloaded", pkg) )
    print

def install( treename, localdir ):
    treeStore = openTreeStore()
    pkg = treeStore.find( treename )
    treeStore.verifyLocal( pkg )
    treeStore.install( pkg, localdir, PackageProgress("installed", pkg) )
    print

def presign( treename, expirySecs ):
    treeStore = openTreeStore()
    pkg = treeStore.find( treename )
    treeStore.addUrls( pkg, expirySecs )
    print json.dumps( PackageJS().toJson(pkg), sort_keys=True, indent=2, separators=(',', ': ') )
    
def downloadHttp( packageFile ):
    treeStore = nonS3TreeStore()
    pkg = readPackageFile( packageFile )
    treeStore.downloadHttp( pkg, PackageProgress("downloaded", pkg) )
    print

def installHttp( packageFile, localdir ):
    treeStore = nonS3TreeStore()
    pkg = readPackageFile( packageFile )
    treeStore.verifyLocal( pkg )
    treeStore.install( pkg, localdir, PackageProgress("installed", pkg) )
    print

parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(help='commands',dest='commandName')

init_parser = subparsers.add_parser('init', help='Initialise a new store')
init_parser.add_argument('--chunksize', action='store', default=10000000, type=int,
                         help='The maximum number of bytes to be stored in each chunk')

list_parser = subparsers.add_parser('list', help='List trees available in the store')

info_parser = subparsers.add_parser('info', help='Show information about a tree')
info_parser.add_argument('treename', action='store', help='The name of the tree')

upload_parser = subparsers.add_parser('upload', help='Upload a tree from the local filesystem')
upload_parser.add_argument('treename', action='store', help='The name of the tree')
upload_parser.add_argument('localdir', action='store', help='The local path of the file')

download_parser = subparsers.add_parser('download', help='Download a tree to the local cache')
download_parser.add_argument('treename', action='store', help='The name of the tree')

install_parser = subparsers.add_parser('install', help='Install a tree into the filesystem')
install_parser.add_argument('treename', action='store', help='The name of the tree')
install_parser.add_argument('localdir', action='store', help='The local path of the file')

presign_parser = subparsers.add_parser('presign', help='Generate a package definition containing presigned urls ')
presign_parser.add_argument('treename', action='store', help='The name of the tree')
presign_parser.add_argument('--expirySecs', action='store', default=3600, type=int,
                            help='Validity of the presigned URLs in seconds')

downloadhttp_parser = subparsers.add_parser('download-http', help='Download a tree to the local cache using a presigned package file')
downloadhttp_parser.add_argument('pkgfile', action='store', help='The file containing the package definition')

installhttp_parser = subparsers.add_parser('install-http', help='Install a tree from local cache using a presigned package file')
installhttp_parser.add_argument('pkgfile', action='store', help='The file containing the package definition')
installhttp_parser.add_argument('localdir', action='store', help='The local path of the file')

def main():
    args = parser.parse_args()
    if args.commandName == 'init':
        init( args.chunksize )
    elif args.commandName == 'list':
        list()
    elif args.commandName == 'info':
        info( args.treename )
    elif args.commandName == 'upload':
        upload( args.treename, args.localdir )
    elif args.commandName == 'download':
        download( args.treename )
    elif args.commandName == 'install':
        install( args.treename, args.localdir )
    elif args.commandName == 'presign':
        presign( args.treename, args.expirySecs )
    elif args.commandName == 'download-http':
        downloadHttp( args.pkgfile )
    elif args.commandName == 'install-http':
        installHttp( args.pkgfile, args.localdir )

if __name__ == '__main__':
    main()
