"""Globant coding challenge"""
import sqlalchemy.types
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from decouple import config
import pandas as pd
import logging

# Logger configuration...
logging.basicConfig(
    level=logging.INFO,
    filename='debug.log',
    filemode='w',
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Here we create the Postgres connection
postgres_url = config('POSTGRES_URL')
engine = create_engine(postgres_url)
logging.info("Connection created...")

# Here we create the tables in Postgres
fd = open('tableCreate.sql', 'r')
sqlFile = fd.read()
fd.close()
sqlCommands = sqlFile.split(';')
for command in sqlCommands:
    db = scoped_session(sessionmaker(bind=engine))
    db.execute(command)
    db.commit()
    db.close()

logging.info("Created the tables...")

# Here we populate the Postgres tables
hired_employees = 'csv/hired_employees.csv'
departments = 'csv/departments.csv'
jobs = 'csv/jobs.csv'

files = [hired_employees, departments, jobs]

for file in files:
    with open(file, 'rb') as f:
        df = pd.read_csv(f, header=None)
    if file.split('/')[-1] == 'hired_employees.csv':
        df.columns = ['id', 'name', 'datetime', 'department_id', 'job_id']
        nullValues = df[df.isna().any(axis=1)]
        logging.info(f'Entries not according to Data Rules for file {file.split("/")[-1]}: {nullValues}')
        df.dropna(inplace=True)
        df.to_sql('hired_employees', con=engine, index=False, if_exists='replace', dtype={
            'id': sqlalchemy.types.INTEGER(),
            'name': sqlalchemy.types.TEXT(),
            'datetime': sqlalchemy.types.TEXT(),
            'department_id': sqlalchemy.types.INTEGER(),
            'job_id': sqlalchemy.types.INTEGER(),
        })
    elif file.split('/')[-1] == 'departments.csv':
        df.columns = ['id', 'department']
        nullValues = df[df.isna().any(axis=1)]
        logging.info(f'Entries not according to Data Rules for file {file.split("/")[-1]}: {nullValues}')
        df.dropna(inplace=True)
        df.to_sql('departments', con=engine, index=False, if_exists='replace', dtype={
            'id': sqlalchemy.types.INTEGER(),
            'department': sqlalchemy.types.TEXT()
        })
    elif file.split('/')[-1] == 'jobs.csv':
        df.columns = ['id', 'job']
        nullValues = df[df.isna().any(axis=1)]
        logging.info(f'Entries not according to Data Rules for file {file.split("/")[-1]}: {nullValues}')
        df.dropna(inplace=True)
        df.to_sql('jobs', con=engine, index=False, if_exists='replace', dtype={
            'id': sqlalchemy.types.INTEGER(),
            'job': sqlalchemy.types.TEXT()
        })

logging.info("Finish populating tables...")
