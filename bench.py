from lstore.db import Database
from lstore.query import Query
from time import perf_counter
from random import choice, randrange
from time import perf_counter
from statistics import mean

def bench():
    # Student Id and 4 grades
    db = Database()
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)
    keys = []

    insert_time_0 = perf_counter()
    for i in range(0, 10000):
        query.insert(906659671 + i, 93, 0, 0, 0)
        keys.append(906659671 + i)
    insert_time_1 = perf_counter()

    insert_time = insert_time_1 - insert_time_0

    # Measuring update Performance
    update_cols = [
        [None, None, None, None, None],
        [None, randrange(0, 100), None, None, None],
        [None, None, randrange(0, 100), None, None],
        [None, None, None, randrange(0, 100), None],
        [None, None, None, None, randrange(0, 100)],
    ]


    update_time_0 = perf_counter()
    for i in range(0, 10000):
        query.update(choice(keys), *(choice(update_cols)))
    update_time_1 = perf_counter()

    update_time = update_time_1 - update_time_0

    # Measuring Select Performance
    select_time_0 = perf_counter()
    for i in range(0, 10000):
        query.select(choice(keys),0 , [1, 1, 1, 1, 1])
    select_time_1 = perf_counter()

    select_time = select_time_1 - select_time_0

    # Measuring Aggregate Performance
    agg_time_0 = perf_counter()
    for i in range(0, 10000, 100):
        start_value = 906659671 + i
        end_value = start_value + 100
        result = query.sum(start_value, end_value - 1, randrange(0, 5))
    agg_time_1 = perf_counter()

    agg_time = agg_time_1 - agg_time_0

    # Measuring Delete Performance
    delete_time_0 = perf_counter()
    for i in range(0, 10000):
        query.delete(906659671 + i)
    delete_time_1 = perf_counter()

    delete_time = delete_time_1 - delete_time_0

    return (insert_time, update_time, select_time, agg_time, delete_time)

insert = []
update = []
select = []
agg = []
delete = []

for i in range(0, 10):
    (i, u, s, a, d) = bench()
    insert.append(i)
    update.append(u)
    select.append(s)
    agg.append(a)
    delete.append(d)

print(f"Mean insert time for 10k records over 10 runs: {mean(insert)}")
print(f"Mean update time for 10k records over 10 runs: {mean(update)}")
print(f"Mean select time for 10k records over 10 runs: {mean(select)}")
print(f"Mean agg time for 10k records over 10 runs: {mean(agg)}")
print(f"Mean delete time for 10k records over 10 runs: {mean(delete)}")