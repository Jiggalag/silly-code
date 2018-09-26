import datetime

import numpy as np
import pandas as pd


class Delta:
    def __init__(self, engine, tables, source_db, target_db, logger):
        self.engine = engine
        self.tables = tables
        self.source_db = source_db
        self.target_db = target_db
        self.logger = logger
        self.today = str(datetime.datetime.date(datetime.datetime.today()))

    def calculate_deltas(self):
        # TODO: should compare two tables for different dates, find duplicates and write duplicates in special tables
        for table in self.tables:
            source_query = "SELECT * FROM {}.{} ORDER BY remoteid;".format(self.source_db, table)
            target_query = "SELECT * FROM {}.{}{} ORDER BY remoteid;".format(self.target_db, table,
                                                                             self.today.replace('-', '_'))
            source_df = pd.read_sql_query(source_query, self.engine, index_col='remoteId')
            target_df = pd.read_sql_query(target_query, self.engine, index_col='remoteId')
            return pd.merge(target_df, source_df, how='inner', sort='remoteId')

    def write_deltas(self, data):
        for table in self.tables:
            self.logger.info('Now we process table {}...'.format(table))
            if data.empty:
                self.logger.warn('Dataframe is empty for table {}!'.format(table + '_delta'))
            else:
                data.to_sql(table + '_delta', con=self.engine, schema=self.target_db, if_exists='replace',
                            index=False)
                self.logger.info('Deltas successfully writed to table {}'.format(table + '_delta'))
        self.logger.info('All tables successfully processed!')

    # TODO: not forget this helpful function
    @staticmethod
    def highlight_diff(data, color='yellow'):
        attr = 'background-color: {}'.format(color)
        other = data.xs('First', axis='columns', level=-1)
        return pd.DataFrame(np.where(data.ne(other, level=0), attr, ''),
                            index=data.index, columns=data.columns)
