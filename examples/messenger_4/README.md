# Persisty Example App : messenger : Part 4

In [the previous step](../messenger_3), we added a basic UI for our application. In this step we will
make it actually persistant using SQL. Since we use, SQLAlchemy, we have compatibility with a number 
database implementations out of the box. For the moment, we will simply use SQLite. (Because a
postgresql / mysql installation is outside the scope of this document!)

## Running the Code

There are some more steps this time due to the addition of alembic:

* Clone the git repo `git clone https://github.com/tofarr/persisty.git`
* Go to the directory `cd persisty/examples/messenger_4`
* Create a virtual environment. (I used [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/)
  for this)
  * `pip install virtualenvwrapper`
  * `mkvirtualenv messenger_4`
  * `workon messenger_4`
* Install requirements with `pip install -r requirments-dev.txt`
* Run the alembic migrations using `alembic upgrade head`. Because we are using sqlite, this will create a
  `messages.db` file in the project root directory
* Run the project `python -m main`

## What is Going On Here...

* We now have a requirements-dev.txt for alembic

* We defined a database connection string in .env `PERSISTY_SQL_URN="sqlite:///messenger.db"`

* We generated an alembic environment using `alembic init alembic`

* We defined the database connection in the generated alembic.ini `sqlalchemy.url = sqlite:///messenger.db`

* We updated our alembic [env.py](alembic/env.py) with the following:
  ```
  from dotenv import load_dotenv
  ...
  from persisty.migration.alembic import get_target_metadata
  ...
  load_dotenv()
  ...
  target_metadata = get_target_metadata()
  ...
  ```

* We generated a migration with `alembic revision --autogenerate -m "Initial Configuration"`

* We added the seed data into the [migration](alembic/versions/d7c34c35b662_initial_configuration.py):
  ```
    ...
    def upgrade() -> None:
    ...
    from persisty.migration.alembic import add_seed_data
    add_seed_data(op)
  ```

* We run the resultant migration with `alembic upgrade head`

## Summary

We now have a version of the application backed by a SQL server,
so data is persisted between server restarts. This is everything
required to run the application in a hosted environment (Aside
from SSL and DNS).

In the next step, we will go back and [deploy to AWS Cloud rather 
than a hosted environment](../messenger_5).
