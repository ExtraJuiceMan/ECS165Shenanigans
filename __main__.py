from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange, seed, randint

def validate():
	db = Database()
	# Create a table  with 5 columns
	#   Student Id and 4 grades
	#   The first argument is name of the table
	#   The second argument is the number of columns
	#   The third argument is determining the which columns will be primay key
	#       Here the first column would be student id and primary key
	grades_table = db.create_table('Grades', 5, 0)

	# create a query class for the grades table
	query = Query(grades_table)

	# dictionary for records to test the database: test directory
	records = {}

	number_of_records = 1000
	number_of_aggregates = 100
	seed(3562901)

	for i in range(0, number_of_records):
		key = 92106429 + randint(0, number_of_records)

		#skip duplicate keys
		while key in records:
			key = 92106429 + randint(0, number_of_records)

		records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
		query.insert(*records[key])
		# print('inserted', records[key])
	print("Insert finished")

	# Check inserted records using select query
	for key in records:
		# select function will return array of records 
		# here we are sure that there is only one record in t hat array
		record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
		error = False
		for i, column in enumerate(record.columns):
			if column != records[key][i]:
				error = True
		if error:
			print('select error on', key, ':', record, ', correct:', records[key])
		else:
			pass# print('select on', key, ':', record)
	print("Select finished")
			
def benchmark():
	db = Database()
	grades_table = db.create_table('Grades', 5, 0)
	query = Query(grades_table)
	keys = []

	insert_time_0 = process_time()
	for i in range(0, 10000):
		query.insert(906659671 + i, 93, 0, 0, 0)
		keys.append(906659671 + i)
	insert_time_1 = process_time()

	print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

	# Measuring update Performance
	update_cols = [
		[None, None, None, None, None],
		[None, randrange(0, 100), None, None, None],
		[None, None, randrange(0, 100), None, None],
		[None, None, None, randrange(0, 100), None],
		[None, None, None, None, randrange(0, 100)],
	]

	# Measuring Select Performance
	select_time_0 = process_time()
	for i in range(0, 10000):
		query.select(choice(keys), 0, [1, 1, 1, 1, 1])
	select_time_1 = process_time()
	print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)
	
print("Validate: ")
validate()
print("Benchmark: ")
benchmark()