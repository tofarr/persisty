import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

extras_require = {
    "dev": [
        "black~=23.3",
        "pytest~=7.2",
        "pytest-cov~=4.0",
        "pytest-xdist~=3.2",
        "pylint~=3.0",
        "boto3~=1.26",
        "moto~=3.1",
    ],
    "server": ["servey[server]~=3.0"],
    "serverless": ["servey[serverless]~=3.0", "opensearch-py~=2.2"],
    "sql": ["SQLAlchemy~=1.4"],
    "sqldev": ["alembic~=1.12"],
    "scheduler": ["servey[scheduler]~=3.0"],
}
extras_require["all"] = list(
    {
        dependency
        for dependencies in extras_require.values()
        for dependency in dependencies
    }
)

setuptools.setup(
    name="persisty",
    author="Tim O'Farrell",
    author_email="tofarr@gmail.com",
    description="A better persistence layer for python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tofarr/lambsync",
    packages=setuptools.find_packages(exclude=("tests*",)),
    install_requires=[
        "pyaes~=1.6",
        "servey~=3.0",
    ],
    extras_require=extras_require,
    setup_requires=["setuptools-git-versioning"],
    setuptools_git_versioning={"enabled": True, "dirty_template": "{tag}"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
