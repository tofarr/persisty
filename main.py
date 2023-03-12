import os
os.environ['JWT_SECRET_KEY'] = 'FOOBAR'
# persisty_data_directory = os.environ['PERSISTY_DATA_DIRECTORY'] = 'persisty_data_dir'
from servey.__main__ import main

if __name__ == "__main__":
    main()
