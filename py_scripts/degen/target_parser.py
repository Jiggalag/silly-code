import json


class TargetParser:
    def __init__(self, connection, db, logger):
        self.logger = logger
        connection.select_db(db)
        self.connection = connection

    def get_targeting(self):
        query = "SELECT * FROM ricktarget;"
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
        targeting = dict()
        for item in result:
            targeting.update({item.get('rickCampaign_id'): json.loads(item.get('json'))})
        return targeting
