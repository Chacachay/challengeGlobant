# challengeGlobant
Coding challenge for Globant

_A series of python scripts for migrating CSVs to a PostgreSQL database.

### Requirements
_For this project you will need 'Python>10' and the following libraries_
```
pip install pandas
pip install SQLAlchemy
pip install python-decouple
pip install requests
pip install psycopg
pip install logging
pip install apache-airflow
pip install DateTime
```
_You need a Postgres database with the configuration settings for the conection in the 'settings.ini'_

### Postgres settings
_Modify the 'settings.ini' file with your Postgres url_
```
POSTGRES_URL=postgresql://[user[:password]@][netloc][:port][/dbname]
```

## Build with

* [Pycharm](https://www.jetbrains.com/pycharm/)
* [Postgres](https://www.postgresql.org/)
* [Airflow](https://airflow.apache.org/)
