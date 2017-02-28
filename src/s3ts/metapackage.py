import json, datetime, os
from collections import OrderedDict

from s3ts.utils import datetimeFromIso
from s3ts import package

class MetaPackage(object):
    """
    A metapackage specified how multiple packages can be
    arranged into a single directory tree.
    """

    def __init__( self, name, description, creationTime, components ):
        self.name = name
        self.description = description
        self.creationTime = creationTime
        self.components = components

    def verify(self, treestore, metadata):
        assert self.name
        for component in self.components:
            component.verify(treestore, metadata)

    def package(self, treestore, metadata):
        files = []
        for component in self.components:
            files += component.packageFiles(treestore, metadata)
        return package.Package( self.name, self.description, self.creationTime, files )
        
class SubPackage(object):
    """
    A sub package component of a MetaPackage.
    """
    def __init__( self, installPath, packageName):
        self.installPath = installPath
        self.packageName = packageName

    def verify(self, treestore, metadata):
        assert self.installPath
        assert self.packageName
        try:
            treestore.findPackage(self.packageName)
        except KeyError:
            raise KeyError, "package {} doesn't exist".format(self.packageName)

    def packageFiles(self, treestore, metadata):
        subpackage = treestore.findPackage(self.packageName)
        return packageFilesInSubdir(self.installPath, subpackage)

    def info(self):
        return '{} -> package {}'.format( self.installPath, self.packageName )

class LocalizedPackage(object):        
    """
    A sub package component of a MetaPackage, with localised installation.

    The localizedPackageName may use metadata keys , eg `local-{hostname}`. In
    which case the hostname metadata value will be used to generate the 
    actual package to be installed. If a package with this name is not available,
    then the sepewill be installed.

    """
    def __init__( self, installPath, localizedPackageName, defaultPackageName):
        self.installPath = installPath
        self.localizedPackageName = localizedPackageName
        self.defaultPackageName = defaultPackageName

    def verify(self, treestore, metadata):
        assert self.installPath
        assert self.localizedPackageName
        assert self.defaultPackageName
        metakeys = re.re.findall( '{([^}]*)}', self.localizedPackageName)
        for key in metakeys:
            if not metadata.has_key(key):
                raise RuntimeError, "Package references unknown metdata key '{}'".format(key)
            packageName = self.localizedPackageName.format(**metdata)
            try:
                treestore.findPackage(self.packageName)
            except KeyError:
                treestore.findPackage(defaultPackageName)

    def packageFiles(self, treestore, metadata):
        packageName = self.localizedPackageName.format(**metdata)
        try:
            subpackage = treestore.get(packageName)
        except KeyError:
            package = treestore.get(self.defaultPackageName)
        return packageFilesInSubdir(self.installPath, subpackage)

    def info(self):
        return '{} -> package {}'.format(self.installPath, self.localizedPackageName)

# In the json serialization code below we use OrderedDict
# to lock down the order of the fields in the generated json.
# We do this because we intend the resulting json to
# be human readable.

class MetaPackageJS(object):
    """A json de/serialiser for MetaPackage objects"""
    def __init__( self ):
        self.componentJS = ComponentJS()

    def fromJson( self, jv ):
        return MetaPackage(
            jv['name'],
            jv.get('description', ''),
            datetimeFromIso( jv['creationTime'] ),
            [self.componentJS.fromJson(jv1) for jv1 in jv['components']]
        )
                        
    def toJson( self, v ):
        return OrderedDict([
            ('name', v.name),
            ('description', v.description),
            ('creationTime', v.creationTime.isoformat()),
            ('components', [ self.componentJS.toJson(f) for f in v.components ]),
        ])

class ComponentJS(object):
    """A json de/serialiser for MetaPackage Component objects"""
    def __init__( self ):
        self.subPackageJS = SubPackageJS()
        self.localizedPackageJS = LocalizedPackageJS()

    def fromJson( self, jv ):
        if jv.has_key('subPackage'):
            return self.subPackageJS.fromJson( jv['subPackage'] )
        elif jv.has_key('localizedPackage'):
            return self.localizedPackageJS.fromJson( jv['localizedPackage'] )
        else:
            raise RuntimeError, "invalid metapackage component"
        
    def toJson( self, v ):
        if isinstance( v, SubPackage ):
            return { 'subPackage' : self.subPackageJS.toJson(v) }
        elif isinstance( v, LocalizedPackage ):
            return { 'localizedPackage' : self.localizedPackageJS.toJson(v) }
        else:
            raise RuntimeError, "invalid metapackage component"

class SubPackageJS(object):
    """A json de/serializer for metapackage SubPackage objects"""

    def fromJson( self, jv ):
        return SubPackage( jv['installPath'], jv['packageName'] )

    def toJson( self, v ):
        return OrderedDict([
            ('installPath', v.installPath),
            ('packageName', v.packageName),
        ])

class LocalizedPackageJS(object):
    """A json de/serializer for metapackage LocalizedPackage objects"""

    def fromJson( self, jv ):
        return LocalizedPackage( jv['installPath'], jv['localizedPackageName'], jv['defaultPackageName'] )

    def toJson( self, v ):
        return OrderedDict([
            ('installPath', v.installPath),
            ('localizedPackageName', v.localizedPackageName),
            ('defaultPackageName', v.defaultPackageName),
        ])
    
def packageFilesInSubdir(subdir, subpackage):
    files = []
    for file in subpackage.files:
        path = package.pathFromFileSystem( os.path.normpath( os.path.join(subdir, file.path) ) )
        files.append( package.PackageFile( file.sha1, path, file.chunks ) )
    return files
        
                
