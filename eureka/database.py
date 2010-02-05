import os

def connect(database):
    ''' connects to an sqlite database with given file name '''

    try:
        import sqlite3 as sqlite
    except ImportError:
        try:
            import sqlite
        except ImportError:
            return None

    if database != ':memory:':
        dir = os.path.dirname(__file__)
        eureka_base, _ = os.path.split(dir)
        database = os.path.join(eureka_base, database)

    connection = sqlite.connect(database)
    connection.text_factory = str

    if database != ':memory:':
        umask = os.umask(0) # unfortunately, os.umask will also set to umask
        os.umask(umask)     # so we need to reset it

        os.chmod(database, 0o666 & ~umask)

    return connection
