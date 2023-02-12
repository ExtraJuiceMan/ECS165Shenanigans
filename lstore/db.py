from store import CrabStore

class Database:
    def __init__(self):
        self.crab_store = CrabStore()
        
    def __getattr__(self, name):
        return getattr(self.crab_store, name)
