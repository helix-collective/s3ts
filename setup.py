from distutils.core import setup

setup(
    name="s3ts",

    version="0.1.0",

    author="Tim Docker",
    author_email="timd@helixta.com.au",

    # Contents
    packages=["s3ts"],
    package_dir={'' : 'src'},

    include_package_data=False,

    # Details
    url="https://bitbucket.org/helix-collective/s3ts/wiki/Home",

    description="A library to manage versioned tree based data on S3",

    install_requires=[
        "boto",
        "requests"
    ],
)
