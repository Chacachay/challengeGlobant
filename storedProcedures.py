"""Script for creating the stored procedures"""
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from decouple import config

postgres_url = config('POSTGRES_URL')
engine = create_engine(postgres_url)
tableList = config('tableList')

fd = open('sql/backupTableCreate.sql', 'r')
sqlFile = fd.read()
fd.close()
sqlCommands = sqlFile.split(';')
for command in sqlCommands:
    db = scoped_session(sessionmaker(bind=engine))
    db.execute(command)
    db.commit()
    db.close()

for i in tableList:
    db = scoped_session(sessionmaker(bind=engine))
    db.execute(
        f"""
        CREATE OR REPLACE PROCEDURE backupTable_{i}()
        LANGUAGE SQL
        AS $$
        DROP TABLE {i}_backup;
        SELECT * INTO {i}_backup FROM {i};
        $$;
        """
    )
    db.commit()
    db.close()
