# Persisty Example App : Messager : Part 3

In [the previous step](../messager_5), user SQL as our persistence mechanism.
In this example we will deploy to AWS: Lambdas are used for actions, SQS for 
subscriptions, KMS for Keys, and Dynamodb will be used for persistence.

Effectively this means the same application code can be used in test environments,
hosted environments, and AWS environments.

## Running the Code

* Clone the git repo `git clone https://github.com/tofarr/persisty.git`
* Go to the directory `cd persisty/examples/messager_5`
* Create a virtual environment. (I used [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/)
  for this)
  * `pip install virtualenvwrapper`
  * `mkvirtualenv messager_5`
  * `workon messager_5`
* Install requirements with `pip install -r requirments.txt`
* Generate serverless yml definitions with `dotenv run -- python -m servey --run=sls`. This creates serverless yml definitions.

## Deploying to AWS

### Install serverless / serverless plugins:

```
npm install serverless
npm install serverless-python-requirements
npm install serverless-prune-plugin
npm install serverless-appsync-plugin
```

### Deploy the serverless project:

`sls deploy`


This process typically takes a few minutes. Since this project does not define any Route53 or Cloudfront resources,
your API will only have the standard amazon URLs for access.

### Adding Seed Data

I used the dynamodb seed plugin to populate initial seed data

`npm install serverless-dynamodb-seed`
```
plugins:
...
- serverless-dynamodb-seed
custom:
  ...
  seed:
    user:
      table: user
      sources:
        - seeds/user.json
    message:
      table: message
      sources:
        - seeds/message.json
```

`sls dynamodb:seed`

## A Note on Appsync API Keys

Appsync requires that api keys be used to access your API, so we added an action to [auth.py](messager/actions/auth.py)
and had to add them to [the serverless yml](serverless_appsync_role_statements.yml)
to retrieve them, and a fetch to the index.html. The alternative to this is to use environment variables in your client
application to store the API key.

## Summary

We now have a version of the messager app deployed to AWS.
