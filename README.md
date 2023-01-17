
TODO:

Build Clientey
Build resource in servey
Migrations - need a way of building out serverless resources.
Build an AutoUI project in NPM

Extras
======
Move Triggers to extras.
Dynamic storage
File storage
Consider building a user app
Consider building a rate limiting app (Rate Limit access control)



Outside the scope of this project: Repo Synchronization

2 modes:

Pre existing storage defined programmatically
Storage derived from datastore state

caching right the way through to the client.


sqlalchemy integration
Revisit the filtered_storage class - make sure reads are not done unless required, and that they are definitely not repeated
change authorization permissions to scopes
make authorization jwt encodable / decodable


Demos...

Actions & Triggers. Not part of core! Recipes / Goodies
Also timestamping built using this

Triggers
action_name may correspond to lambda name in aws
trigger_type: before_create|after_create|...update|delete|web|timer

Action
name: str
param_schema
return_schema
access_control: is_executable(authorization)

Can also be periodic:


Add redis support

Add remote http support


Rate Limiting - Not Part of Core! But useful.


Demo a distributed lock.


Demo a custom index.
