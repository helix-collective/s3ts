import json

ENCODING_RAW = 'raw'
ENCODING_ZLIB = 'zlib'

class Package(object):
    """represents a collection of files to be downloaded."""
    
    def __init__( self, name, files ):
        self.name = name
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
        return Package( jv['name'], [self.packageFileJS.fromJson(jv1) for jv1 in jv['files']] )
                        
    def toJson( self, v ):
        return {
            'name' : v.name,
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
