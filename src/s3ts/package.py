import json, datetime, os

from s3ts.utils import datetimeFromIso

ENCODING_RAW = 'raw'
ENCODING_ZLIB = 'zlib'

class Package(object):
    """represents a collection of files to be downloaded."""
    
    def __init__( self, name, description, creationTime, files ):
        self.name = name
        self.description = description
        self.creationTime = creationTime
        self.files = files

    def size(self):
        return sum( [pf.size() for pf in self.files] )

class PackageFile(object):
    """represents a single file to be downloaded."""
    def __init__( self, sha1, path, chunks ):
        self.sha1 = sha1
        self.path = path
        self.chunks = chunks

    def size(self):
        return sum( [c.size for c in self.chunks] )

class FileChunk(object):
    """represents a chunk of a file to be downloaded"""
    def __init__( self, sha1, size, encoding, url ):
        self.sha1 = sha1
        self.size = size
        self.encoding = encoding
        self.url = url

class PackageJS(object):
    """A json de/serialiser for Package objects"""
    def __init__( self ):
        self.packageFileJS = PackageFileJS()

    def fromJson( self, jv ):
        return Package(
            jv['name'],
            jv.get('description', ''),
            datetimeFromIso( jv['creationTime'] ),
            [self.packageFileJS.fromJson(jv1) for jv1 in jv['files']]
        )
                        
    def toJson( self, v ):
        return {
            'name' : v.name,
            'description' : v.description,
            'creationTime' : v.creationTime.isoformat(),
            'files' : [ self.packageFileJS.toJson(f) for f in v.files ],
        }

class PackageFileJS(object):
    "A json de/serialiser for PackageFile objects"
    def __init__( self ):
        self.chunkJS = FileChunkJS()

    def toJson( self, v ):
        return {
            'sha1' : v.sha1,
            'path' : v.path,
            'chunks' : [ self.chunkJS.toJson(v) for v in v.chunks ]
        }

    def fromJson( self, jv ):
        return PackageFile( jv['sha1'], jv['path'], [self.chunkJS.fromJson(jv1) for jv1 in jv['chunks']] )


class FileChunkJS(object): 
    "A json de/serialiser for FileChunk objects"

    def toJson( self, v ):
        jv = {
            'sha1' : v.sha1,
            'size' : v.size,
            'encoding' : v.encoding
        }
        if v.url != None:
            jv['url'] = v.url
        return jv

    def fromJson( self, jv ):
        return FileChunk( jv['sha1'], jv['size'], jv['encoding'], jv.get('url') )

def packageFilter(package, pathRegex ):
    """
    Return a new package that only includes paths from the
    input package that match the given regexp

    """
    result = Package(
        name=package.name,
        description=package.description,
        creationTime=package.creationTime,
        files=[]
    )
    for file in package.files:
        if not pathRegex or pathRegex.match(file.path):
            result.files.append(file)
    return result

def pathFromFileSystem(fspath):
    """
    Turn a local filesystem path into a package file path.
    (Inside the package metadata, we always store paths in unix format)
    """
    return fspath.replace("\\", "/")

def packageDiff(package1, package2):
    """
    Compute a package that reflects the changes required to turn package1
    into package2. The set of file path that must be deleted is also returned.
    """

    def filesByPath(package):
        dict = {}
        for f in package.files:
            dict[f.path] = f
        return dict

    files1 = filesByPath(package1)
    files2 = filesByPath(package2)
    paths1 = set(files1.keys())
    paths2 = set(files2.keys())

    removedPaths = paths1.difference(paths2)
    commonPaths = paths1.intersection(paths2)
    addedPaths = paths2.difference(paths1)
    
    diffPackage = Package(
        name="%s->%s" % (package1.name,package2.name),
        description='',
        creationTime=package2.creationTime,
        files=[]
        )

    for p in addedPaths:
        diffPackage.files.append(files2[p])

    for p in commonPaths:
        if files1[p].sha1 != files2[p].sha1:
            diffPackage.files.append(files2[p])

    return diffPackage,removedPaths
            
S3TS_PACKAGEFILE = '.s3ts.package' 

def writeInstallPackage( installDir, pkg ):
    with open( os.path.join( installDir, S3TS_PACKAGEFILE ), 'w' ) as f:
        f.write( json.dumps( PackageJS().toJson( pkg ) ) )

def readInstallPackage( installDir ):
    with open( os.path.join( installDir, S3TS_PACKAGEFILE ), 'r' ) as f:
        return PackageJS().fromJson( json.loads( f.read() ) )
