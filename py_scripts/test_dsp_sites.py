class TestSites:
    def __init__(self, data, connect):
        self.data = data
        self.connect = connect

    def check_types(self):
        pass

    def get_site_remoteids(self):
        remoteids = list()
        for item in self.data:
            remoteids.append(item.get('criteria').get('remoteId'))
        return remoteids

    def get_forbidden_sites(self):
        pass