# Persisty Example App : Messager : Part 4

In [the previous step](../messager_3), we added a basic UI for our application. In this step we will
make it actually persistant using SQL. Since we use, SQLAlchemy, we have compatibility with a number 
database implementations out of the box. For the moment, we will simply use SQLite. (Because a
postgresql / mysql installation is outside the scope of this document!)

## Running the Code

There are some more steps this time due to the addition of alembic:

* Clone the git repo `git clone https://github.com/tofarr/persisty.git`
* Go to the directory `cd persisty/examples/messager_4`
* Create a virtual environment. (I used [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/)
  for this)
  * `pip install virtualenvwrapper`
  * `mkvirtualenv messager_4`
  * `workon messager_4`
* Install requirements with `pip install -r requirments-dev.txt`
* Run the alembic migrations using `alembic upgrade head`
* Run the project `python -m servey`

## What is Going On Here...

We now have a requirements-dev.txt for alembic

We defined a database connection string in .env `PERSISTY_SQL_URN="sqlite:///messager.db"`

We generated an alembic environment using `alembic init alembic`

We defined the database connection in the generated alembic.ini `sqlalchemy.url = sqlite:///messager.db`

We updated our alembic [env.py](alembic/env.py) with the following:
```
from dotenv import load_dotenv
...
from persisty.migration.alembic import get_target_metadata
...
# Load environment
load_dotenv()
...
target_metadata = get_target_metadata()
...
```

We generated a migration with `alembic revision --autogenerate -m "Initial Configuration"`

We added the seed data into the [migration](alembic/versions/d7c34c35b662_initial_configuration.py):
```
    ...
    def upgrade() -> None:
    ...
    from persisty.migration.alembic import add_seed_data
    add_seed_data(op)
```

We run the resultant migration with `alembic upgrade head`