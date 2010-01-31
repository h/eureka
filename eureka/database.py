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
        from os import path
        dir = path.dirname(__file__)
        eureka_base, _ = path.split(dir)
        database = path.join(eureka_base, database)

    connection = sqlite.connect(database)
    connection.text_factory = str

    return connection
