"""A minimal version of bsddb3.DB."""
import bsddb3.db

# This might be somewhere in the bsddb3 module, but I wanted to compare the
# performance of bsddb3 with semidbm and I could not have a bare bones dict
# interface exposed with a shelve like interface.

def open(filename, mode):
    db = bsddb3.db.DB(None)
    if mode == 'r':
        flags = bsddb3.db.DB_RDONLY
    elif mode == 'rw':
        flags = 0
    elif mode == 'w':
        flags =  bsddb3.db.DB_CREATE
    elif mode == 'c':
        flags =  bsddb3.db.DB_CREATE
    elif mode == 'n':
        flags = bsddb3.db.DB_TRUNCATE | bsddb3.db.DB_CREATE
    else:
        raise bsddb3.db.DBError(
            "flags should be one of 'r', 'w', 'c' or 'n' or use the "
            "bsddb.db.DB_* flags")
    db.open(filename, None, bsddb3.db.DB_HASH, flags)
    return db
