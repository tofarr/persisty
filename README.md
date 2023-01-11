TODO:

Problems:

* Type hinting sucks because we are using generated types
* Storage use dict items rather than objects - wrappers may help
* Circular dependency hell will get old fast - move input and output objects to storage meta

@stored
class User
    Doc string 

    id: UUID
    username: str = Attr(schema=str_schema(max_length=255), is_indexed=True)
    full_name: Optional[str] = field(default=None, metadata=dict(schemey=str_schema(max_length=255)))
    email_address: Optional[str] = field(
        default=None,
        metadata=dict(schemey=str_schema(max_length=255, str_format=StringFormat.EMAIL))
    )
    password_digest: str
    admin: bool = False
    created_at: datetime
    updated_at: datetime
    authored_message_count: int = HasCount(linked_storage_name='message', key_field_name='author_id')
    authored_messages: int = HasMany(linked_storage_name='message', key_field_name='author_id')

stored processes everything.
Produces dataclass with format...

@dataclass
class FieldMeta:
  type=FieldType,
  creatable=True, 
  readable=True, 
  updatable=True, 
  searchable=True, 
  create_transform=Optional[EditTransformABC] = None,
  update_transform=Optional[EditTransformABC] = None,
  permitted_filter_ops: Tuple[FieldFilterOp, ...]


class Field:
  name: str
  meta: FieldMeta
  schema: Schema


class StorageMeta:
    key_config: KeyConfigABC = FIELD_KEY_CONFIG
    storage_access: StorageAccess = ALL_ACCESS
    cache_control: CacheControlABC = SecureHashCacheControl()
    batch_size: int = 100
    description: Optional[str] = None
    links: Tuple[LinkABC, ...] = tuple()
    indexes: ...


@dataclass
class User:
   __storage_meta__: StorageMeta
   id: UUID = field(default_value=UNDEFINED, metadata=dict(
      schemey=...
      persisty=FieldMeta
   )
   username: str = field(default_value=UNDEFINED, metadata=dict(
      schemey=str_schema(max_length=255), 
      persisty=FieldMeta
   ))
  ...

  @action
  def authored_message_count(self) -> int:
    ...

* We can produce the meta from the class
* We can produce the class from the meta - or variants for create, update and read
* we can produce an entity, which stores 2 versions.

storage loads these - not servey.
storage uses generics.





I dunno if I'm happy with the split into editable and non editable endpoints.
We may need to consolidate.
No - I dont like this. A lot of endpoints dont make sense.
"put" read only user. Seriously???

So the options are:

* Hidden logic. (This may be inevitable)
* Filter storage...
  * Storage prevent create unless old value is x
  * Prevent edit unless old value and new value are x
  * Prevent delete unless old value is x
    


Build post processing triggers
Migrations
Dynamic storage
Exports
Extras
======
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
