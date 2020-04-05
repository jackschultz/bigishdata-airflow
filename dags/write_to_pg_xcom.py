import datetime as dt
import time

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

pg_hook = PostgresHook(postgres_conn_id='postgres_bigishdata')
engine = pg_hook.get_sqlalchemy_engine()
Session = sessionmaker(bind=engine)
session = Session() #session for querying


class Dts(Base):
    __tablename__ = 'dts'

    id = Column(Integer, primary_key=True)
    run_time = Column(DateTime)
    execution_time = Column(DateTime)
    formatted_run_time = Column(String)

    def __repr__(self):
        return f"<Dts(id={self.id}, run_time={self.run_time})>"


def write_to_pg_xcom(**kwargs):
    run_time = dt.datetime.utcnow()
    execution_time = kwargs['ts']
    print('Writing to pg', run_time, execution_time)
    new_dt = Dts(run_time=run_time, execution_time=execution_time)
    session.add(new_dt)
    session.commit()  # Flushing means commit and refresh, so new_dt has the assigned id
    print(new_dt)
    kwargs['ti'].xcom_push(key='inserted_id', value=new_dt.id)


def format_run_time_pg_xcom(**kwargs):
    time.sleep(5)
    inserted_id = kwargs['ti'].xcom_pull(task_ids='write_to_pg_xcom', key='inserted_id')
    print(inserted_id)
    recent_dt = session.query(Dts).get(inserted_id)
    print(recent_dt)  # put in the logs
    formatted_run_time = recent_dt.run_time.strftime('%m/%d/%Y %H:%M:%S')
    recent_dt.formatted_run_time = formatted_run_time
    session.commit()


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime.now(),
    'retries': 0
}
 
with DAG('writing_to_pg_xcom',
          default_args=default_args,
          start_date=dt.datetime.now(),
          schedule_interval=dt.timedelta(seconds=10)
          ) as dag:

    write_to_pg_xcom_operator = PythonOperator(task_id='write_to_pg_xcom',
                                               python_callable=write_to_pg_xcom,
                                               provide_context=True,
                                               dag=dag)
    format_run_time_pg_xcom_operator = PythonOperator(task_id='format_run_time_pg_xcom',
                                                      python_callable=format_run_time_pg_xcom,
                                                      provide_context=True,
                                                      dag=dag)

    write_to_pg_xcom_operator >> format_run_time_pg_xcom_operator

