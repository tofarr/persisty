if __name__ == "__main__":
    import sys

    sys.argv = ["alembic", "upgrade", "head"]
    from alembic.config import main

    main()
