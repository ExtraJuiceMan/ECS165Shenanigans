from lstore.db import Database
from lstore.query import Query
from time import perf_counter
from random import choice, randrange, seed, randint

from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open("./test")

grades_table = db.create_table('Grades', 5, 0)

db.close()
