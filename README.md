

Outside the scope of this project: Repo Synchronization

2 modes:

Pre existing storage defined programmatically
Storage derived from datastore state

I feel access control is wrong - it is being asked to filter generally and specifically - these may need to be 2 different operations
Maybe add additional fields? Or divide between item access control and storage access control? 
Different users have access to different things.


in update, should the key be separate? WHAT ABOUT CREATE?
item does not necessarily include key?


sqlalchemy integration
context setup for security / triggers

change authorization permissions to scopes
make authorization jwt encodable / decodable

Actions & Triggers. Not part of core!

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


Rate Limiting - Not Part of Core!


Demo a distributed lock.


Demo a custom index.
