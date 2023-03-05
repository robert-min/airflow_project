from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

default_args = {
    'start_date': datetime(2021, 1, 1)
}

dags_path = os.path.abspath(os.path.join(__file__, os.path.pardir))
taxi_price_path = os.path.abspath(os.path.join(dags_path, os.path.pardir))
src_path = os.path.abspath(os.path.join(taxi_price_path, "src"))

with DAG(dag_id="taxi-price-pipeline",
         schedule_interval="@daily",
         default_args=default_args,
         tags=['spark'],
         catchup=False) as dag:

    # preprocessing
    preprocess = SparkSubmitOperator(
        application=src_path + "/preprocessing.py",
        task_id="preprocess",
        conn_id="spark_local"
    )

    # tune hyperparameter
    hyperparameter = SparkSubmitOperator(
        application=src_path + "/hyperparameter.py",
        task_id="hyperparameter",
        conn_id="spark_local"
    )

    # train model
    train = SparkSubmitOperator(
        application=src_path + "/train_model.py",
        task_id="train",
        conn_id="spark_local"
    )

    preprocess >> hyperparameter >> train