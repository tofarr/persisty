# Persisty Users

TODO: Here be Dragons! This is very much incomplete!

A users layer built on top of persisty providing support for OIDC, SAML and SCIM.

## Deploying new versions of this to Pypi

```
pip install setuptools wheel
python setup.py sdist bdist_wheel
pip install twine
python -m twine upload dist/*
```
