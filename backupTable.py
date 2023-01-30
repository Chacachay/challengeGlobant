"""DAG created for scheduling table backups"""
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from decouple import config
import storedProcedures as sT


tableList = config('tableList')


def callStoredProcedure(tableList):
    """This function calls the stored procedures for
    creating table backups"""
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
    storedprocedures = PythonOperator(
        task_id="stored_procedures",
        python_callable=sT.storedProcedures,
    )

    backupOperator = PythonOperator(
        task_id="backup_tables",
        requirements="SQLAlchemy==1.4.37",
        python_callable=callStoredProcedure(),
    )

    storedprocedures >> backupOperator
