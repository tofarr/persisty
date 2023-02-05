import os
os.environ['JWT_SECRET_KEY'] = 'FOOBAR'
from servey.__main__ import main

if __name__ == "__main__":
    main()
