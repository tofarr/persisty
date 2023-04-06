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
* Generate serverless yml definitions with `dotenv run -- python -m servey.servey_aws.serverless`. This creates serverless yml definitions.

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

## Summary

We now have an API with a UI which may be viewed in browsers. It still doesn't actually
persist data, nor is it deployed anywhere outside of a local enviroment - in the 
[next step we will cover using SQLAlchemy and Alembic to connect to a SQL database](../messager_4),
suitable for hosted enviroments. Alternativey, the [step after that does not user SQL
but instead uses DynamoDB and lambda to deploy to AWS Cloud using Serverless](../messager_5).