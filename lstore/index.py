"""
A wrapper for the real indexing mechanism. We don't do it this way!
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        pass

    def create_index(self, column_number):
        self.table.build_index(column_number)

    def drop_index(self, column_number):
        self.table.drop_index(column_number)