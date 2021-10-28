from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from csv_process import sqlLoad

default_args = {
    "owner": "airflow",
    "start_date": datetime.today() - timedelta(days=1)
              }

param_dic = {
    "host"      : Variable.get("host"),
    "database"  : Variable.get("database"),
    "user"      : Variable.get("user"),
    "password"  : Variable.get("password"),
}
with DAG(
    "Migrate user_purchase",
    default_args=default_args,
    schedule_interval = "0 1 * * *",
    ) as dag:
    sqlLoad = PythonOperator(
        task_id="sql_load",
        python_callable=sqlLoad,
        op_kwargs={'param_dic': param_dic},
    )

sqlLoad