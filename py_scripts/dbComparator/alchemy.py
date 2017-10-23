import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData


class SQLConnection:
    def __init__(self, user, password, host, db):
        connect_string = 'mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4;'.format(user,
                                                                               password,
                                                                               host,
                                                                               db)

        engine = sqlalchemy.create_engine(connect_string)

        metadata = MetaData()
        metadata.reflect(engine)
        Base = automap_base(metadata=metadata)
        Base.prepare(engine, reflect=True)
        base_tables = Base.classes.keys()
        tables = list(metadata.tables.keys())
        tables.sort()
        self.tables = tables
        not_mapped = list(set(tables) - set(base_tables))
        not_mapped.sort()
        for table in not_mapped:
            print(table)



#
# Session = sessionmaker(bind=engine)
# session = Session()

SQLConnection('sqlclient', 'sqlclient', 'samaradb03.maxifier.com', 'rick')