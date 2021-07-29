#!/usr/bin/env python
import os, subprocess, zipfile

BASE_DIR = os.path.abspath( os.path.join( os.path.dirname( __file__ ), '..' ) )

def newZipFile( zipPath ):
    if not os.path.exists( os.path.dirname( zipPath ) ):
        os.makedirs( os.path.dirname( zipPath ) )
    return zipfile.ZipFile( zipPath, 'w' )

def addFilesToZip( zf, dir, matcher=None, arcname=None ):
    for root, dirnames, filenames in os.walk(dir):
        for f in filenames:
            path = os.path.join(root, f)

            if not matcher or matcher(path):
                if arcname:
                    arcname1 = os.path.join( arcname, os.path.relpath( path, dir) )
                else:
                    arcname1 = os.path.relpath( path, dir)
                zf.write( path, arcname=arcname1 )

                
# Build the s3ts python zip file
# (which is used for uploading packages to S3)
s3tsPythonZip = os.path.join( BASE_DIR, 'dist/s3ts.zip' )
zf = newZipFile( s3tsPythonZip )
addFilesToZip( zf, os.path.join( BASE_DIR, 'src' ), lambda f:f.endswith('.py') )
addFilesToZip( zf, os.path.join( BASE_DIR, 'python-venv/lib/python3.9/site-packages/boto' ), lambda f:f.endswith('.py'), 'boto' )
addFilesToZip( zf, os.path.join( BASE_DIR, 'python-venv/lib/python3.9/site-packages/requests' ), lambda f:f.endswith('.py'), 'requests' )
zf.close()
