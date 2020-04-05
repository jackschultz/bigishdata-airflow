import datetime as dt
import tempfile

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

pg_hook = PostgresHook(postgres_conn_id='postgres_bigishdata')
engine = pg_hook.get_sqlalchemy_engine()
Session = sessionmaker(bind=engine)
session = Session()  # session for querying


class Post(Base):
    __tablename__ = 'posts'
    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    body = Column(String)
    posted_at = Column(DateTime)

    def __repr__(self):
        return f"<Post id={self.id}, title={self.title}>"


http_local_posts_conn_id = 'http_local_posts'
user_agent = 'Bigish Data Airflow Testing'
headers = {'User-Agent': user_agent}
post_index_endpoint = '/posts'

posts_bucket_name = 'bigishdata-airflow-posts'


def gather_posts_html(**kwargs):
    print('About to gather post index html')
    http_hook = HttpHook(method='GET', http_conn_id=http_local_posts_conn_id)
    res = http_hook.run(post_index_endpoint, headers=headers)

    print('Finished gathering post index html')

    # with the response, now we insert into the bucket
    execution_time = dt.datetime.fromisoformat(kwargs['ts'])
    print(type(execution_time))
    print(execution_time)
    formatted_execution_time = execution_time.strftime('%Y%m%d-%H%M%S')
    key = f"indexes/{formatted_execution_time}-posts.html"

    with tempfile.NamedTemporaryFile() as temp:
        print(temp)
        temp.write(res.content)
        print(f"Writing {temp.name} to html to s3 with key {key}")
        temp.seek(0)
        print(res.content)
        s3_hook = S3Hook(aws_conn_id='s3_posts_html')
        s3_hook.load_file(temp.name, key, bucket_name=posts_bucket_name)
        print('Finished writing html to s3')


def scrape_posts_html(**kwargs):
    print('About to gather post index html')


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime.now(),
    'retries': 0
}

with DAG('scrape_posts',
         default_args=default_args,
         start_date=dt.datetime.now(),
         schedule_interval=dt.timedelta(seconds=10)
         ) as dag:

    gather_posts_html_operator = PythonOperator(task_id='gather_posts_html',
                                                python_callable=gather_posts_html,
                                                provide_context=True, dag=dag)

gather_posts_html_operator
