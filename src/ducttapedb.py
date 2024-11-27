import sqlite3
import json
from threading import local

class DuctTapeDB:
    _state = local()

    def __init__(self):
        pass