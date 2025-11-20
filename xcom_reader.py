from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.xcom import XCom
from airflow.utils.session import provide_session

@provide_session
def read_from_xcom(session=None, **_):
    # Берём последнее значение XCom по dag_id/task_id/key из первого дага
    value = XCom.get_one(
        dag_id="xcom_writer",
        task_id="push_message",
        key="message",
        include_prior_dates=True,
        session=session,
    )
    if value is None:
        print("[reader] XCom не найден. Убедись, что xcom_writer уже отработал.")
        return None

    print(f"[reader] Прочитано из XCom: {value}")
    return value  # вернётся как XCom текущего таска с ключом 'return_value'

with DAG(
    dag_id="xcom_reader",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # запускаем вручную после writer
    catchup=False,
    tags=["example", "xcom"],
) as dag:
    read_message = PythonOperator(
        task_id="read_message",
        python_callable=read_from_xcom,
    )
