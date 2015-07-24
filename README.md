# An S3 TreeStore (S3TS)

This is a python library and command line tool to efficiently manage the storage of file trees within Amazon S3.

## Running the tests

```
virtualenv python-venv
./python-venv/bin/pip install -r requirements.txt

export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export S3TS_BUCKET=... an empty s3 bucket ...
S3TS_LOCALCACHE=/tmp

PYTHONPATH=./src ./python-venv/bin/python test/test_treestore.py
```
