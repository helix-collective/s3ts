import os, sys, argparse, json, datetime, re

import boto

from s3ts.treestore import TreeStore, TreeStoreConfig
from s3ts.filestore import FileStore, LocalFileStore
from s3ts.s3filestore import S3FileStore
from s3ts.package import PackageJS, packageDiff, packageFilter
from s3ts.metapackage import MetaPackage, SubPackage, MetaPackageJS

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
    s3PathPrefix = os.environ.get( 'S3TS_S3PREFIX' )
    s3c = boto.connect_s3(awsAccessKeyId,awsSecretAccessKey)
    return s3c.get_bucket( bucketName ),s3PathPrefix

def createTreeStore(chunksize):
    localCacheDir = getEnv( 'S3TS_LOCALCACHE', 'the local directory used for caching'  )
    bucket,s3PathPrefix = connectToBucket()
    config = TreeStoreConfig( chunksize, True )
    return TreeStore.create( S3FileStore(bucket,s3PathPrefix), LocalFileStore(localCacheDir), config )

def openTreeStore(dryRun=False,verbose=False):
    localCacheDir = getEnv( 'S3TS_LOCALCACHE', 'the local directory used for caching'  )
    bucket,s3PathPrefix = connectToBucket()
    treeStore = TreeStore.open( S3FileStore(bucket,s3PathPrefix), LocalFileStore(localCacheDir) )
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
    for treeName in treeStore.listPackages():
        print treeName
    for treeName in treeStore.listMetaPackages():
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

def info( treename, pathRegex ):
    treeStore = openTreeStore()
    try:
        pkg = treeStore.findMetaPackage(treename)
        infofn = printMetaPackageInfo
    except KeyError:
        pkg = treeStore.findPackage(treename)
        pkg = packageFilter(pkg,pathRegex)
        infofn = printPackageInfo
    infofn(treename, pkg)

def printPackageInfo(name, pkg):
    print 'Package:', name
    print 'Created At:', pkg.creationTime.isoformat()
    print 'Total Size: {0} bytes'.format(pkg.size())
    print 'Files:'
    for pf in pkg.files:
        print '    {0} ({1} chunks, {2} bytes)'.format( pf.path, len(pf.chunks), pf.size() )

def printMetaPackageInfo(name, pkg):
    print 'Package:', name
    print 'Created At:', pkg.creationTime.isoformat()
    print 'Components:'
    for component in pkg.components:
        print '    ', component.info()

def upload( treename, description, localdir, dryRun, verbose ):
    creationTime = datetime.datetime.now()
    treeStore = openTreeStore(dryRun=dryRun,verbose=verbose)
    treeStore.upload( treename, description, creationTime, localdir, UploadProgress() )
    print

def uploadMany( treename, description, localdir, kioskDir ):
    creationTime = datetime.datetime.now()
    treeStore = openTreeStore()
    treeStore.uploadMany(treename, description, creationTime, localdir, kioskDir, UploadProgress())
    print

def createMerged( treename, packageArgs, dryRun, verbose ):
    creationTime = datetime.datetime.now()
    treeStore = openTreeStore(dryRun=dryRun,verbose=verbose)
    packageMap = {}

    # Convert the package command line arguments from form
    #    [ "subdir1:pname1", "subdir2:pname2", ... ]
    # to
    #    { "subdir1":"pname1", "subdir2":"pname2", ... }
    
    for s in packageArgs:
        fields = s.split( ':', 2 )
        if len(fields) != 2:
            sys.stderr.write( 'create-merge arguments must be of form SUBDIR:TREENAME' )
            sys.exit(1)
        packageMap[fields[0]] = fields[1]
    
    treeStore.createMerged( treename, creationTime, packageMap)
    print                

def download( treename, dryRun, verbose, metadata ):
    treeStore = openTreeStore(dryRun=dryRun,verbose=verbose)
    pkg = treeStore.find( treename, metadata )
    treeStore.download( pkg, DownloadProgress(pkg) )
    print

def flush( dryRun, verbose ):
    treeStore = openTreeStore(dryRun=dryRun,verbose=verbose)
    treeStore.flushStore()
    print

def flushCache( dryRun, verbose, packageNames ):
    treeStore = openTreeStore(dryRun=dryRun,verbose=verbose)
    treeStore.flushLocalCache(packageNames)
    print
    
def install( treename, localdir, verbose, pathRegex, metadata ):
    treeStore = openTreeStore(verbose=verbose)
    pkg = treeStore.find( treename, metadata )
    pkg = packageFilter(pkg,pathRegex)
    treeStore.download( pkg, DownloadProgress(pkg) )
    print
    treeStore.verifyLocal( pkg )
    treeStore.install( pkg, localdir, InstallProgress(pkg) )
    print

def verifyInstall( treename, localdir, verbose, metadata ):
    treeStore = openTreeStore(verbose=verbose)
    pkg = treeStore.find( treename, metadata )
    result = treeStore.compareInstall( pkg, localdir )
    for path in result.missing:
        print "{} is missing".format(path)
    for path in result.diffs:
        print "{} is different".format(path)
    if len(result.missing) == 0 and len(result.diffs) == 0:
        print "Package {} verified ok at {}".format(treename,localdir)
    else:
        sys.exit(1)

def presign( treename, expirySecs, metadata ):
    treeStore = openTreeStore()
    pkg = treeStore.find( treename, metadata )
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

def validateCache():
    treeStore = openTreeStore()
    print treeStore.validateLocalCache()

def comparePackages( packageName1, packageName2, metadata ):
    treeStore = openTreeStore()
    print "Fetching {}...".format(packageName1)
    package1 = treeStore.find(packageName1, metadata)
    print "Fetching {}...".format(packageName2)
    package2 = treeStore.find(packageName2, metadata)
    print "---"

    size1 = 0
    size2 = 0
    diffSize = 0

    for f in package1.files:
        size1 += f.size()
    for f in package2.files:
        size2 += f.size()
    diffPackage,removedPaths = packageDiff(package1,package2)
    for p in removedPaths:
        print "Removed", p
    for f in diffPackage.files:
        size = f.size()
        print "Updated {} (size {:,})".format(f.path,size)
        diffSize += size

    print
    print "{} size = {:,}".format(package1.name,size1)
    print "{} size = {:,}".format(package2.name,size2)
    print "update size = {:,}".format(diffSize)

TEMPLATE_METAPACKAGE_NAME =  "METANAME-VERSION"

def newMetaPackage(metapackagefile):
    template = MetaPackage(
      name = TEMPLATE_METAPACKAGE_NAME,
      description = "",
      creationTime = datetime.datetime.now(),
      components = [
        SubPackage("SUBDIR1", "PACKAGE1-VERSION"),
        SubPackage("SUBDIR2", "PACKAGE2-VERSION")
      ]
    )
    with open(metapackagefile, 'w') as f:
        f.write(json.dumps(MetaPackageJS().toJson(template), indent=2))
        f.write('\n')
    print "metapackage template written to {}".format(metapackagefile)

def uploadMetaPackage(metapackagefile):
    treeStore = openTreeStore()
    with open(metapackagefile, 'r') as f:
        metapackage = MetaPackageJS().fromJson(json.load(f))
    if metapackage.name == TEMPLATE_METAPACKAGE_NAME:
        raise RuntimeError, "Edit the metapackage template before uploading it"
    metapackage.verify(treeStore,{})
    treeStore.uploadMetaPackage(metapackage)

def downloadMetaPackage(metapackagename,metapackagefile):
    treeStore = openTreeStore()
    metapackage = treeStore.findMetaPackage(metapackagename)
    with open(metapackagefile, 'w') as f:
        f.write(json.dumps(MetaPackageJS().toJson(metapackage), indent=2))
        f.write('\n')
    
def metaDataDictionary(vals):
    """
    Turn list of colon separated pairs (from the command line) into a
    dictionary
    """
    if vals == None:
        return {}
    else:
        # 
        return dict( [v.split(":",1) for v in vals] )

def pathRegex(arg):
    if arg == None:
        return None
    else:
        return re.compile(arg)

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
p.add_argument('--path-regex', dest='pathRegex', action='store')

p = subparsers.add_parser('upload', help='Upload a tree from the local filesystem')
p.set_defaults(dryRun=False,verbose=False,description='')
p.add_argument('--dry-run', dest='dryRun', action='store_true')
p.add_argument('--verbose', dest='verbose', action='store_true')
p.add_argument('--description', dest='description', action='store')
p.add_argument('treename', action='store', help='The name of the tree')
p.add_argument('localdir', action='store', help='The local directory path')

p = subparsers.add_parser('download', help='Download a tree to the local cache')
p.set_defaults(dryRun=False,verbose=False)
p.add_argument('--dry-run', dest='dryRun', action='store_true')
p.add_argument('--verbose', dest='verbose', action='store_true')
p.add_argument('--meta', dest='meta', action='append')
p.add_argument('treename', action='store', help='The name of the tree')

p = subparsers.add_parser('flush', help='Flush chunks from the store that are no longer referenced')
p.set_defaults(dryRun=False,verbose=False)
p.add_argument('--dry-run', dest='dryRun', action='store_true')
p.add_argument('--verbose', dest='verbose', action='store_true')

p = subparsers.add_parser('flush-cache', help='Flush chunks from the local cache that are not referenced by the specified packages')
p.set_defaults(dryRun=False,verbose=False)
p.add_argument('--dry-run', dest='dryRun', action='store_true')
p.add_argument('--verbose', dest='verbose', action='store_true')
p.add_argument('packagenames', nargs='+' )

p = subparsers.add_parser('install', help='Download/Install a tree into the filesystem')
p.set_defaults(verbose=False)
p.add_argument('--verbose', dest='verbose', action='store_true')
p.add_argument('--meta', dest='meta', action='append')
p.add_argument('--path-regex', dest='pathRegex', action='store')
p.add_argument('treename', action='store', help='The name of the tree')
p.add_argument('localdir', action='store', help='The local directory path')

p = subparsers.add_parser('verify-install', help='Confirm a tree has been correctly installed')
p.add_argument('--verbose', dest='verbose', action='store_true')
p.add_argument('--meta', dest='meta', action='append')
p.add_argument('treename', action='store', help='The name of the tree')
p.add_argument('localdir', action='store', help='The local directory path')

p = subparsers.add_parser('presign', help='Generate a package definition containing presigned urls ')
p.add_argument('treename', action='store', help='The name of the tree')
p.add_argument('--expirySecs', action='store', default=3600, type=int,
                            help='Validity of the presigned URLs in seconds')
p.add_argument('--meta', dest='meta', action='append')

p = subparsers.add_parser('download-http', help='Download a tree to the local cache using a presigned package file')
p.add_argument('pkgfile', action='store', help='The file containing the package definition')

p = subparsers.add_parser('install-http', help='Install a tree from local cache using a presigned package file')
p.add_argument('pkgfile', action='store', help='The file containing the package definition')
p.add_argument('localdir', action='store', help='The local directory path')

p = subparsers.add_parser('prime-cache', help='Prime the local cache with the contents of a local directory')
p.add_argument('localdir', action='store', help='The local directory path')

p = subparsers.add_parser('upload-many', help='Upload multiple trees from the local filesystem')
p.set_defaults(description='')
p.add_argument('--description', dest='description', action='store')
p.add_argument('treename', action='store', help='The name of the tree')
p.add_argument('localdir', action='store', help='The local directory path')
p.add_argument('local_variant_dir', action='store', help='The local variant path')

p = subparsers.add_parser('create-merged', help='Create a new package by merging existing packages')
p.set_defaults(dryRun=False,verbose=False)
p.add_argument('--dry-run', dest='dryRun', action='store_true')
p.add_argument('--verbose', dest='verbose', action='store_true')
p.add_argument('treename', action='store', help='The name of the merged tree')
p.add_argument('package_args', nargs='+', metavar='DIR:RNAME')

p = subparsers.add_parser('new-metapackage', help='Write a template for a metapackage to a local file')
p.add_argument('metapackagefile', action='store', help='The metapackage file to which the template is to be written')

p = subparsers.add_parser('upload-metapackage', help='Upload a metapackage from a local file')
p.add_argument('metapackagefile', action='store', help='The metapackage file to be uploaded')

p = subparsers.add_parser('download-metapackage', help='Download an existing metapackage to a local file')
p.add_argument('metapackagename', action='store', help='The name of the metapackage to be downloaded')
p.add_argument('metapackagefile', action='store', help='The metapackage file to be uploaded')

p = subparsers.add_parser('compare-packages', help='Compare two packages')
p.add_argument('--meta', dest='meta', action='append')
p.add_argument('package1', action='store', help='The first package')
p.add_argument('package2', action='store', help='The second package')

validate_local_cache_parser = subparsers.add_parser('validate-local-cache', help='Validates the local cache')

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
        info( args.treename, pathRegex(args.pathRegex) )
    elif args.commandName == 'upload':
        upload( args.treename, args.description, args.localdir, args.dryRun, args.verbose )
    elif args.commandName == 'download':
        download( args.treename, args.dryRun, args.verbose, metaDataDictionary(args.meta) )
    elif args.commandName == 'flush':
        flush( args.dryRun, args.verbose )
    elif args.commandName == 'flush-cache':
        flushCache( args.dryRun, args.verbose, args.packagenames )
    elif args.commandName == 'install':
        install( args.treename, args.localdir, args.verbose, pathRegex(args.pathRegex), metaDataDictionary(args.meta) )
    elif args.commandName == 'verify-install':
        verifyInstall( args.treename, args.localdir, args.verbose, metaDataDictionary(args.meta) )
    elif args.commandName == 'presign':
        presign( args.treename, args.expirySecs, metaDataDictionary(args.meta) )
    elif args.commandName == 'download-http':
        downloadHttp( args.pkgfile )
    elif args.commandName == 'install-http':
        installHttp( args.pkgfile, args.localdir )
    elif args.commandName == 'prime-cache':
        primeCache( args.localdir )
    elif args.commandName == 'upload-many':
        uploadMany(args.treename, args.description, args.localdir, args.local_variant_dir)
    elif args.commandName == 'create-merged':
        createMerged(args.treename, args.package_args, args.dryRun, args.verbose)
    elif args.commandName == 'validate-local-cache':
        validateCache()
    elif args.commandName == 'compare-packages':
        comparePackages(args.package1, args.package2, metaDataDictionary(args.meta))
    elif args.commandName == 'new-metapackage':
        newMetaPackage(args.metapackagefile)
    elif args.commandName == 'upload-metapackage':
        uploadMetaPackage(args.metapackagefile)
    elif args.commandName == 'download-metapackage':
        downloadMetaPackage(args.metapackagename, args.metapackagefile)

if __name__ == '__main__':
    main()
