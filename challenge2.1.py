"""Script for running queries from challenge 2"""
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from decouple import config
import pandas as pd


postgres_url = config('POSTGRES_URL')
engine = create_engine(postgres_url)

query_1="""
select
       dep.department as Department,
       jo.job as Job,
       count(*) FILTER (WHERE EXTRACT(MONTH FROM (CAST(datetime AS DATE))) in ('1','2','3')) AS Q1,
       count(*) FILTER (WHERE EXTRACT(MONTH FROM (CAST(datetime AS DATE))) in ('4','5','6')) AS Q2,
       count(*) FILTER (WHERE EXTRACT(MONTH FROM (CAST(datetime AS DATE))) in ('7','8','9')) AS Q3,
       count(*) FILTER (WHERE EXTRACT(MONTH FROM (CAST(datetime AS DATE))) in ('10','11','12')) AS Q4
from hired_employees he
join jobs jo
on jo.id = he.job_id
join departments dep
on dep.id = he.department_id
WHERE EXTRACT(YEAR FROM (CAST(datetime AS DATE))) = '2021'
group by dep.department, jo.job
order by dep.department, jo.job asc;
"""

db = scoped_session(sessionmaker(bind=engine))
result_1 = db.execute(query_1).fetchall()
db.commit()
db.close()

df_1 = pd.DataFrame(result_1)
df_1.columns = ['department', 'job', 'Q1', 'Q2', 'Q3', 'Q4']

print(df_1.to_markdown(index=False))

query_2="""
with counts as(
select
       department_id as dep,
       count(*) as test
from hired_employees
where EXTRACT(YEAR FROM (CAST(datetime AS DATE))) = '2021'
group by department_id
    )
select
       he.department_id as ID,
       dep.department as DEPARTMENT,
       count(*) as HIRED
from hired_employees he
join departments dep
on dep.id = he.department_id
where EXTRACT(YEAR FROM (CAST(he.datetime AS DATE))) = '2021'
group by dep.department, he.department_id
having count(*) > (
    select avg(counts.test) from counts
    )
order by HIRED desc ;
"""

db = scoped_session(sessionmaker(bind=engine))
result_2 = db.execute(query_2).fetchall()
db.commit()
db.close()

df_2 = pd.DataFrame(result_2)
df_2.columns = ['ID', 'DEPARTMENT', 'HIRED']

print(df_2.to_markdown(index=False))
