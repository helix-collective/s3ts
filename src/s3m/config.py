"""
contains classes for configuration and settings
"""

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

    
        
    
