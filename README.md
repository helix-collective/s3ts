# Overview

This is a python library and command line tool to efficiently manage
the storage of file trees within Amazon S3.

# Setup the environment

```
virtualenv python-venv
./python-venv/bin/pip install -r requirements.txt
```

# Running the tests

The unit tests need access to S3, and an empty S3 bucket to use. These
are set through environment variables:

```
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export S3TS_BUCKET=... an empty s3 bucket ...

PYTHONPATH=./src ./python-venv/bin/python test/test_treestore.py
```

There is also a simple test script to exercise the commmand line
interface. It is a bit simplistic currently - it also requires an
empty S3 bucket, but leaves content in it on exit (which
you will need to clean out manually with the AWS web console).

```
sh -x test/cli-test.sh
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
