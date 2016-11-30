import os, sys, tempfile, shutil

# You wouldn't think that writing a file would be so hard!

class InPlaceFileWriter(object):
    """
    Just write the file to where it needs to go.
    """
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.file = open(self.filename,'wb')
        return self.file        

    def __exit__(self, *args):
        self.file.close()

class PosixAtomicFileWriter(object):
    """
    Rely on posix semantics to update a file atomically,
    by writing to a temp file in the same directory
    and moving it into place
    """
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.tempfile = tempfile.NamedTemporaryFile(delete=False,dir=os.path.dirname(self.filename))
        return self.tempfile

    def __exit__(self, *args):
        self.tempfile.close()
        os.rename(self.tempfile.name, self.filename)

class ClunkySemiAtomicWindowsFileWriter(object):
    """
    Windows doesn't have atomic file updates, so
    we try and get close here
    """

    # TODO(timd): consider cutting over to using win32api.ReplaceFile
    # as described here:
    #    http://stupidpythonideas.blogspot.com.au/2014/07/getting-atomic-writes-right.html
    
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.tempfile = tempfile.NamedTemporaryFile(delete=False,dir=os.path.dirname(self.filename))
        return self.tempfile

    def __exit__(self, *args):
        # Need to flush both at libc and kernel layers here to
        # ensure that the os.rename() below works correctly under
        # windows
        self.tempfile.flush()
        os.fsync(self.tempfile.fileno())
    
        self.tempfile.close()
        if os.path.exists(self.filename):
           os.unlink(self.filename)
        os.rename(self.tempfile.name, self.filename)

def atomicFileWriter(filename):
    if sys.platform == 'win32':
        return ClunkySemiAtomicWindowsFileWriter(filename)
    else:
        return PosixAtomicFileWriter(filename)
