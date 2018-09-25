import pandas as pd
import numpy as np
import datetime

import pymysql


class Delta:
    def __init__(self, tables, connection, source_db, target_db, logger):
        self.tables = tables
        self.connection = connection
        self.source_db = source_db
        self.target_db = target_db
        self.logger = logger
        self.today = str(datetime.datetime.date(datetime.datetime.today()))

    def calculate_deltas(self):
        # TODO: should compare two tables for different dates, find differences and write differences in special tables
        for table in self.tables:
            source_query = "SELECT * FROM {}.{} ORDER BY remoteid;".format(self.source_db, table)
            # TODO: change to pandas.read_sql
            # source_query = "SELECT * FROM rick_quality.rickcampaign2018_06_19 ORDER BY remoteid;"
            target_query = "SELECT * FROM {}.{}{} ORDER BY remoteid;".format(self.target_db, table,
                                                                             self.today.replace('-', '_'))
            # TODO: change to pandas.read_sql
            # target_query = "SELECT * FROM rick_quality.rickcampaign2018_06_13 ORDER BY remoteid;"
            with self.connection.cursor() as cursor:
                cursor.execute(source_query)
                source_result = cursor.fetchall()
                source_df = pd.DataFrame(source_result).fillna(0)
                source_df.sort_index(inplace=True)
                cursor.execute(target_query)
                target_result = cursor.fetchall()
                target_df = pd.DataFrame(target_result).fillna(0)
                target_df.sort_index(inplace=True)
                return pd.merge(target_df, source_df, how='inner', sort='remoteId')
                # df_all = pd.concat([source_df, target_df], axis='columns', join = 'inner', keys=['First', 'Second']).drop_duplicates().reset_index(drop=True)
                # df_merge = source_df.merge(target_df, source_df.columns, on='remoteId', how='outer')
                # df_final = df_all.swaplevel(axis='columns')[source_df.columns[1:]]
                # df_final.style.apply(self.highlight_diff, axis=None)

    def compare_two_dfs(self, input_df_1, input_df_2):
        df_1, df_2 = input_df_1.copy(), input_df_2.copy()
        df_1.sort_index(inplace=True)
        df_2.sort_index(inplace=True)
        ne_stacked = (df_1 != df_2).stack()
        changed = ne_stacked[ne_stacked]
        changed.index.names = ['id', 'col']
        difference_locations = np.where(df_1 != df_2)
        changed_from = df_1.values[difference_locations]
        changed_to = df_2.values[difference_locations]
        df = pd.DataFrame({'from': changed_from, 'to': changed_to}, index=changed.index)
        return df

    def write_deltas(self, sqlalchemy_engine, data):
        for table in self.tables:
            create_query = 'CREATE TABLE {0}.{1}_delta LIKE {2}.{1};'.format(self.target_db, table, self.source_db)
            with self.connection.cursor() as cursor:
                try:
                    cursor.execute(create_query)
                except pymysql.err.InternalError as e:
                    self.logger.warn(e.args[1])
            data.to_sql(table + '_delta', con=sqlalchemy_engine)
            sqlalchemy_engine.execute('SELECT * FROM rick_quality.rickcampaign_delta;')

    # TODO: not forget this helpful function
    def highlight_diff(self, data, color='yellow'):
        attr = 'background-color: {}'.format(color)
        other = data.xs('First', axis='columns', level=-1)
        return pd.DataFrame(np.where(data.ne(other, level=0), attr, ''),
                            index=data.index, columns=data.columns)

    # TODO: is it helpful? Check usage
    def get_remoteids_set(self, source, target):
        source_remoteids = set()
        target_remoteids = set()
        for item in source:
            source_remoteids.update(item.get('remoteId'))
        for item in target:
            target_remoteids.update(item.get('remoteId'))
        return source_remoteids, target_remoteids
