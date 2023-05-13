# Persisty Events

TODO: Here be Dragons! This is very much incomplete!

An Event Bridge built on top of Persisty...

Event types can be registered with the service. Admin roles are needed for this
Each event type has...
    a schema.
    trigger access control
    add listener access control
    remove listener access control
    list listeners access control

A listener can be...
    An action.
    A webhook.
    A web socket.

We can back all of this by AWS eventbridge in a lambda environment.

This is different from other projects as it should build a full serverless deploy

## Deploying new versions of this to Pypi

```
pip install setuptools wheel
python setup.py sdist bdist_wheel
pip install twine
python -m twine upload dist/*
```
