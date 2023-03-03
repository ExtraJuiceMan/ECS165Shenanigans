from lstore.db import Database
from lstore.query import Query
from time import sleep
from random import choice, randint, sample, seed

db = Database()
db.open('./meme')

grades_table = db.create_table('Grades', 5, 0)

query = Query(grades_table)

records = {}

seed(3562901)

key_original = 92106429
key = key_original
records[key] = [key, 0, 0, 0, 0]
query.insert(*records[key])
key += 1
records[key] = [key, 1, 2, 3, 4]
query.insert(*records[key])
key += 1
records[key] = [key, 1, 2, 3, 4]
query.insert(*records[key])

keys = sorted(list(records.keys()))
print("Insert finished")

# test 1: select on non-primary columns with index




# test 2: select on non-primary columns without index

# test 3: select that returns multiple records
for key in records:
    record = query.select(key, 0, [1, 1, 1, 1])
    print('select on', key, ':', record)
print('test 3 finished')

# test 4: select that returns no records
for key in records:
    record = query.select(key, 5, [1, 1, 1, 1])
    print('select on', key, ': ', record)
print('test 4 finished')

# test 5: update on no record (primary key does not exist in the table)
print('successfully updated : ', query.update(90, *[1, 1, 1, 1]))
print('test 5 finished')
db.close()
