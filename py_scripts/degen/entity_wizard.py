class EntityWizard:
    def __init__(self, bulk):
        self.bulk = bulk
        self.keys = self.get_keys()
        self.keys.sort()

    def get_keys(self):
        keys = list(self.bulk[0].keys())
        keys.sort()
        if 'remoteId' in keys:
            keys.remove('remoteId')
        result = list({'remoteId'})
        result.extend(keys)
        return result
