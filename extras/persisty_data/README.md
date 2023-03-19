# Persisty Data

Data plugin for persisty

* Strategy for bypassing server
* Using S3
* Read vs Write

## Deploying new versions of this Servey to Pypi

```
pip install setuptools wheel
python setup.py sdist bdist_wheel
pip install twine
python -m twine upload dist/*
```
