
# Authors:  Ayush Jiwarajka <ayush9990@gmail.com>
#           Jay Searson <jay@schedulizer.com>
#           Samuel Hoffstaetter <sam@schedulizer.com>


import os, sqlite3

def connect(database, return_false_if_not_found=False):
    ''' connects to an sqlite database with given file name '''

    is_new_file = False

    if database != ':memory:':
        dir = os.path.dirname(__file__)
        eureka_base, _ = os.path.split(dir)
        database = os.path.join(eureka_base, database)
        is_new_file = not os.path.isfile(database)

    connection = sqlite3.connect(database)
    connection.text_factory = str

    if is_new_file:
        if return_false_if_not_found:
            return False

        umask = os.umask(0) # unfortunately, os.umask will also set to umask
        os.umask(umask)     # so we need to reset it

        os.chmod(database, 0o666 & ~umask)

    return connection


