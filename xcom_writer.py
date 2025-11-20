from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def push_to_xcom(ti, **_):
    msg = "Привет из первого дага! 👋"
    ti.xcom_push(key="message", value=msg)
    print(f"[writer] Pushed to XCom: {msg}")

with DAG(
    dag_id="xcom_writer",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # запускаем вручную
    catchup=False,
    tags=["example", "xcom"],
) as dag:
    push_message = PythonOperator(
        task_id="push_message",
        python_callable=push_to_xcom,
    )
