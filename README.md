# bigishdata-airflow

Code for a series of posts on bigishdata.com that go through the process of getting airflow set up running locally, to creating DAGs that get data from different services, transform the data, and load it into database.

### Blog Posts

Part 1 [Apache Airflow Part 1 — Introduction, setup, and writing data to files](https://bigishdata.com/2020/04/05/apache-airflow-part-1-introduction-setup-and-writing-data-to-files/)

Part 2 -- [Apache Airflow Part 2 — Connections, Hooks, reading and writing to Postgres, and XComs](https://bigishdata.com/2020/04/20/apache-airflow-part-2-connections-hooks-reading-and-writing-to-postgres-and-xcoms/)

Part 3 -- To come...

### Cloning and Activiting

When after cloning locally, `pwd` and add the environment variable to your `~/.bash_profile` or whatever terminal you use.

```
...
# Bigish Data Airflow env vars
export AIRFLOW_HOME=bigishdata/bigishdata-airflow # for example
...
```

Then create a virtual env somewhere and install `requirements.txt`.

```
jds:bigishdata-airflow jackschultz$ mkdir ../venvs && python3 -m venv ../venvs/bidaf
jds:bigishdata-airflow jackschultz$ source ../venvs/bidaf/bin/activate
(bidaf) jds:bigishdata-airflow jackschultz$ pip install requirements.txt
```

Make sure postgres is running, and init the db that Airflow needs

```
(bidaf) jds:bigishdata-airflow jackschultz$ airflow initdb
```

Run 
```
(bidaf) jds:bigishdata-airflow jackschultz$ airflow webserver
```
go to `http://localhost:8080/admin/` and you should see the DAGs included in this repo.

### Airflow Scheduler

In another tab, activate the same virtualenv and run

```
(bidaf) jds:bigishdata-airflow jackschultz$ airflow scheduler
```

### Running DAGs

From here, go and view the blog posts listed below about how to get the DAG examples up and running. Those posts are writting as if created the code here yourself, but if you have this repo cloned, you should be able to read though and find the other step needed before running, like for example, creating the databases and tables that are needed for the reads and writes in the simple examples.
