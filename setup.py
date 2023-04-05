import setuptools

from persisty.__version__ import __version__

with open("README.md", "r") as fh:
    long_description = fh.read()

extras_require = {
    "sql": ["SQLAlchemy~=1.4"],
    "server": ["servey[server]~=2.6"],
    "serverless": ["servey[serverless]~=2.6"]
}

setuptools.setup(
    name="persisty",
    version=__version__,
    author="Tim O'Farrell",
    author_email="tofarr@gmail.com",
    description="A better persistence layer for python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tofarr/lambsync",
    packages=setuptools.find_packages(exclude=("tests",)),
    install_requires=[
        "marshy~=3.0",
        "pyaes~=1.6",
        "schemey~=5.7",
        "servey~=2.6",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
