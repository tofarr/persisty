# Persisty Example App : messenger : Part 3

In [the previous step](../messenger_2), we secured our application and added an event when messages are
created. We still don't have a real UI for it though. Even though UI is outside the scope of this project,
it would be negligent to ignore this completely, so this step adds a very basic UI for the API created
in the previous step.

## Running the Code

* Clone the git repo `git clone https://github.com/tofarr/persisty.git`
* Go to the directory `cd persisty/examples/messenger_3`
* Create a virtual environment. (I used [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/)
  for this)
  * `pip install virtualenvwrapper`
  * `mkvirtualenv messenger_3`
  * `workon messenger_3`
* Install requirements with `pip install -r requirments.txt`
* Run the project `python -m main`

## What is Going On Here...

We added a `static_site` directory to serve html / css / js / images on 
[https://localhost:8000](https://localhost:8000). This doesn't use any framework, but the
accessibility of the UI (GraphQL or REST with OpenAPI) should make it compatible with
many client side frameworks.

* Login with `admin` / `Password123!` ![Login](readme/login.png)
* The Main UI should allow viewing and posting messages ![Main UI](readme/main_ui.png)

## Alternatives

The [`Ewey`](https://github.com/tofarr/ewey) project provides a way of auto generating a UI
based on an OpenApi Schema

## Summary

We now have an API with a UI which may be viewed in browsers. It still doesn't actually
persist data, nor is it deployed anywhere outside of a local enviroment - in the 
[next step we will cover using SQLAlchemy and Alembic to connect to a SQL database](../messenger_4),
suitable for hosted enviroments. Alternativey, the [step after that does not user SQL
but instead uses DynamoDB and lambda to deploy to AWS Cloud using Serverless](../messenger_5).