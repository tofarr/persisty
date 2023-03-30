# Persisty Example App : Messager : Part 4

In [the previous step](../messager_3), we added a basic UI for our application. In this step we will
make it actually persistant using SQL. Since we use, SQLAlchemy, we have compatibility with a number 
database implementations out of the box. For the moment, we will simply use SQLite. (Because a
postgresql / mysql installation is outside the scope of this document!)

## Running the Code

* Clone the git repo `git clone https://github.com/tofarr/persisty.git`
* Go to the directory `cd persisty/examples/messager_4`
* Create a virtual environment. (I used [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/)
  for this)
  * `pip install virtualenvwrapper`
  * `mkvirtualenv messager_4`
  * `workon messager_4`
* Install requirements with `pip install -r requirments-dev.txt`
* Run the project `python -m servey`

## What is Going On Here...

We now have a requirements-dev.txt for alembic

We defined a database connection string in .env

We generated parameters for alembic