# Persisty - a better persistence Layer for Python

The objective of this project is to provide a resource oriented approach
accommodating both security and caching rich enough to handle most use
cases while still being independant of the underlying persistence 
mechanism. 

Using [Servey](https://github.org/tofarr/servey) to provide access via 
REST and GraphQL in either a hosted environment or AWS lambda 
environemnt, the project includes out of the box support for both 
SQL (via SQLAlchemy) and Dynamodb.

## Concepts

### Stored Items

Stored items are marked by the [stored](persisty/stored.py) decorator.
This decorator is similar to dataclass, and allows specifying attributes,
keys, schemas, and indexes.

### Stores

A [Store](persisty/store/StoreABC.py) provides a unified interface for
interacting with stored items. [StoreMeta](persisty/store_meta.py) contains
info on what exactly a store supports

Standard Actions are:

* **create** an item
* **read** and item given its key
* **update** an item
* **delete** and item given its key
* **search** for items given filter and sort criteria
* **count** items given filter criteria
* **read_batch** read a batch of items given a list of keys
* **edit_batch** execute a batch of edits (create, update, delete operations)

### Keys

Each item within a store has a string key, derived from the item. (Possibly based on 
one or more of it's attributes) A [KeyConfig](persisty/key_config/key_config_abc.py)
controls this process

### Attributes

Stored items have [Attributes](persisty/attr/attr.py) associated with them, similar
to columns in a relational database. Attributes can have generators associated with
them.

## SearchFilters

Search operations can have [SearchFilters](persisty/search_filter/search_filter_abc.py) 
These can be natively supported (Converted to SQL or dynamodb query / scan criteria)
or run locally in python.

## Extras

We have optional extra projects to accommodate certain common usage patterns

### Persisty Data

A binary data plugin for persisty, accommondating file uploads and downloads. (Delegating
to services like S3)

### Persisty Dynamic

A dynamic data plugin for persisty, accommodating the case where users can define
metadata on the fly, and upload data that conforms to it.

## Examples




## Future Ideas

* REDIS Store
* A user app with support for OIDC, SAML and SCIM
* A Rate Limiting app / Rate Limit Access Control
* An AutoUI project in NPM
* An Event Bridge Project (Backed by AWS Event bridge in AWS, or celery in other environments)
* A Remote HTTP Store
* A Distributed Lock
* An explicit custom index (Spatial RTree)


## Deploying new versions of Persisty to Pypi

```
pip install setuptools wheel
python setup.py sdist bdist_wheel
pip install twine
python -m twine upload dist/*
```
