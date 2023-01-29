"""DAG created for scheduling table backups"""
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from decouple import config

import logging

# Logger configuration...
logging.basicConfig(
    level=logging.INFO,
    filename='debugDag.log',
    filemode='w',
    format="%(asctime)s [%(levelname)s] %(message)s",
)


tableList = config('tableList')

def callStoredProcedure(tableList):
    postgres_url = config('POSTGRES_URL')
    engine = create_engine(postgres_url)
    for i in tableList:
        query = f'CALL backupTable_{i}();'
        db = scoped_session(sessionmaker(bind=engine))
        db.execute(query)
        db.commit()
        db.close()


with DAG(
        "backupTables",
        description="Creates a table to table backup",
        schedule_interval="0 1 * * *",
        start_date=datetime(2023, 1, 30),
        catchup=False,
) as dag:
    backupOperator = PythonOperator(
        task_id="virtualenv_classic",
        requirements="colorama==0.4.0",
        python_callable=callStoredProcedure(),
    )

    backupOperator
