from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed
import shutil

try:
    shutil.rmtree('./lulzz')
except:
    pass

db = Database()
db.open('./lulzz')

grades_table = db.create_table('lolzz',4,0)
query = Query(grades_table)
records = {}

for i in range(0, 600):
    key = i
    records[key] = [key, randint(0, 10), randint(0, 10), randint(0, 10)]
    query.insert(*records[key])

keys = sorted(list(records.keys()))
print("Insert finished")

for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)
print("Select finished")

for key in keys:
    updated_columns = [None, None, None, None]
    for i in range(2, grades_table.num_columns):
        # updated value
        value = randint(0, 10)
        updated_columns[i] = value
        # copy record to check
        original = records[key].copy()
        # update our test directory
        records[key][i] = value
        query.update(key, *updated_columns)
        record = query.select(key, 0, [1, 1, 1, 1])[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
        else:
            pass
            # print('update on', original, 'and', updated_columns, ':', record)
        updated_columns[i] = None
print("Update finished")

for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)
print("Select finished")
    
