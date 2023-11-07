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

What would an API for this look like?

fire_event(channel: str, event: Dict, enqueue_at: Optional[datetime], authorization: Optional[Authorization])
subscribe(channel: str, web_hook_url: Optional[str], header_factory: Dict, authorization: Optional[Authorization]) -> str
unsubscribe(channel: str, subscription_id: str, authorization: Optional[Authorization]) -> subscriber_id
channel_search() -> ResultSet
channel_count() -> int
channel_create() -> Channel
channel_delete() -> str

We can back all of this by AWS eventbridge in a lambda environment.

This is different from other projects as it should build a full serverless deploy

## Deploying new versions of this to Pypi

```
pip install setuptools wheel
python setup.py sdist bdist_wheel
pip install twine
python -m twine upload dist/*
```
