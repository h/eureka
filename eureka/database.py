def connect(database):
    ''' connects to an sqlite database with given file name '''

    try:
        import sqlite3 as sqlite
    except ImportError:
        try:
            import sqlite
        except ImportError:
            return None

    connection = sqlite.connect(database)
    connection.text_factory = str

    return connection
