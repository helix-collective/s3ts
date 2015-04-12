"""
contains classes for configuration and settings
"""

import os, json, datetime

from s3ts.utils import datetimeFromIso

class S3Access(object):
    """Provide connection parameters for an s3m store"""

    def __init__( self, awsKey, awsSecret, s3Bucket ):
        self.awsKey = awsKey
        self.awsSecret = awsSecret
        self.s3Bucket = s3Bucket

        
class S3AccessJS(object):
    """De/Serialise S3Access objects"""
    
    def fromJson( self, jv ):
        return S3Access(
            jv['awsKey'],
            jv['awsSecret'],
            jv['s3Bucket']
        )
    
class TreeStoreConfig(object):
    """Configuration data for an s3m store"""

    def __init__( self, chunkSize, useCompression ):
        self.chunkSize = chunkSize
        self.useCompression = useCompression

        
class TreeStoreConfigJS(object):
    """De/serialise Config objects"""

    def fromJson( self, jv ):
        return TreeStoreConfig(
            jv['chunkSize'],
            jv['useCompression']
            )

    def toJson( self, v ):
        return {
            'chunkSize' : v.chunkSize,
            'useCompression' : v.useCompression,
            }

class InstallProperties(object):
    """records the details of an installation"""

    def __init__( self, treeName, installTime ):
        self.treeName = treeName
        self.installTime = installTime

class InstallPropertiesJS(object):
    """De/serialise InstallProperties objects"""

    def fromJson( self, jv ):
        return InstallProperties(
            jv['treeName'],
            datetimeFromIso( jv['installTime'] )
            )

    def toJson( self, v ):
        return {
            'treeName' : v.treeName,
            'installTime' : v.installTime.isoformat(),
            }

S3TS_PROPERTIES = '.s3ts.properties'    


def writeInstallProperties( installDir, props ):
    with open( os.path.join( installDir, S3TS_PROPERTIES ), 'w' ) as f:
        f.write( json.dumps( InstallPropertiesJS().toJson( props ) ) )

def readInstallProperties( installDir ):
    with open( os.path.join( installDir, S3TS_PROPERTIES ), 'r' ) as f:
        return InstallPropertiesJS().fromJson( json.loads( f.read() ) )
    

        
