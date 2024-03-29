### Introduction

This repo contains an Airflow DAG that outputs a [New Line Delimited Json file](http://ndjson.org/).
An output file example is located in the output_files directory.

In this example, we use docker-compose to set up airflow. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/).

Airflow CeleryExecutor is used for running tasks.

### Setup

- #### How to install Python

This setup works with Python 3.7

check your python3 version
```bash 
$ which python3
/usr/local/bin/python3
```
If the version doesn't match the version installed, set the path to python in your bash_profile file or zshrc file
or add the following line at the end of the file
```
alias python='/usr/local/bin/python3'
```

- #### How to Setup the env 
In order to setup your dev environment, launch the following commands in the servier_use_case directory:

```bash
python -m venv venv  # only the first time
pip install pip-tools   # only the first time, in order to get pip-compile
source venv/bin/activate  # every time you start working on the project
pip install --upgrade pip # the first time and every time a dependency changes
pip install -r requirements-tests.txt  # the first time and every time a dependency changes
pip install -e .  # only the first time
```
- #### How to Launch the DAG

Once Docker Desktop is installed, run `docker-compose build` then `docker-compose up -d` in servier_use_case directory
Then go to `http://localhost:8080`

On the Airflow Web UI, the username is `airflow` and the password is `airflow`

Trigger the dag by pressing the play button.

- #### Run Tests

Run `pytest` to run tests

### Thoughts about the test

- The simplest and most efficient way to deploy this dag is on Google Cloud Composer (no need for the docker-compose)

- Reading large csv files should not be done outside the Airflow execution context. Instead, these files should be placed in a file storage service like Google Cloud Storage and read using [Google Cloud Storage Operator](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/gcs/index.html)

- In a production version, the python code should be executed on a serverless GCP service like Google Cloud Run or Cloud Functions.

- Another alternative for Airflow in this case would be Google Dataflow (Apache Beam)

### SQL

I used bigquery standard sql

Première partie:

````
SELECT
  date,
  SUM(prod_price*prod_qty) AS ventes
FROM
  `project.dataset.TRANSACTION`
where date between date(2019,1,1) and date(2019,1,31)
GROUP BY
  1
ORDER BY
  2
````

Deuxième partie:

````
WITH
  client_product_ventes AS (
  SELECT
    client_id,
    prop_id,
    SUM(prod_price*prod_qty) AS ventes
  FROM
    `project.dataset.TRANSACTION`
  where date between date(2019,1,1) and date(2019,1,31)
  GROUP BY
    1,
    2),
  ventes_client_product_type AS (
  SELECT
    client_id,
    product_type,
    ventes
  FROM
    client_product_ventes A
  LEFT JOIN
    `project.dataset.PRODUCT_NOMENCLATURE` B
  ON
    A.prop_id = B.product_id),
  ventes_meuble AS (
  SELECT
    client_id,
    SUM(ventes) AS ventes_meuble
  FROM
    ventes_client_product_type
  WHERE
    product_type='MEUBLE'
  GROUP BY
    1),
  ventes_deco AS (
  SELECT
    client_id,
    SUM(ventes) AS ventes_deco
  FROM
    ventes_client_product_type
  WHERE
    product_type='DECO'
  GROUP BY
    1)
SELECT
  C.client_id,
  ventes_meuble,
  ventes_deco
FROM
  ventes_meuble C
JOIN
  ventes_deco D
ON
  C.client_id = D.client_id
```
