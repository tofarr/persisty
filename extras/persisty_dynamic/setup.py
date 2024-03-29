import setuptools

from persisty.__version__ import __version__

with open("README.md", "r") as fh:
    long_description = fh.read()

extras_require = {"sql": ["SQLAlchemy~=1.4"]}

setuptools.setup(
    name="persisty_dynamic",
    version=__version__,
    author="Tim O'Farrell",
    author_email="tofarr@gmail.com",
    description="A dynamic persistence layer build on top of persisty",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tofarr/lambsync",
    packages=setuptools.find_packages(exclude=("tests",)),
    install_requires=[
        "persisty",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
