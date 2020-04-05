from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime as dt


def write_to_file(**kwargs):
    kwarg_filename = kwargs['filename']
    execution_time = kwargs['ts']
    dtn = dt.datetime.now()
    with open(kwarg_filename, 'a') as f:  # 'a' means to append
        print('Times to be written to file:', dtn, execution_time)
        f.write(f'{dtn}, {execution_time}\n')


default_args = {
    'owner': 'airflow',
    'retries': 0
}
 
dag = DAG('writing_to_file',
          default_args=default_args,
          start_date=dt.datetime.now(),
          schedule_interval=dt.timedelta(seconds=10)
          )

filename = 'self_logs/dt.txt'
write_to_file_operator = PythonOperator(task_id='write_to_file',
                                        python_callable=write_to_file,
                                        provide_context=True,
                                        op_kwargs={'filename': filename}, dag=dag)
 
write_to_file_operator
