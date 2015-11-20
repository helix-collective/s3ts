# Overview

This is a python library and command line tool to efficiently manage
the storage of file trees within Amazon S3.

# Setup the environment

```
virtualenv python-venv
./python-venv/bin/pip install -r requirements.txt
```

# Running the tests

```
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export S3TS_BUCKET=... an empty s3 bucket ...
S3TS_LOCALCACHE=/tmp

PYTHONPATH=./src ./python-venv/bin/python test/test_treestore.py
```

# Build a standalone zip file

This builds a zip file than includes s3ts and it's dependencies.

```
./python-venv/bin/python tools/build-standalone-zip.py
```

It can be run directly from the command line:

```
python dist/s3ts.zip --help
```
