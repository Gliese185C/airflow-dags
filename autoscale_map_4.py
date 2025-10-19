from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from datetime import datetime
import time
import random

DEFAULT_N = random.randint(2,8)  
DEFAULT_SECONDS = random.randint(3,15)    

with DAG(
    dag_id="autoscale_map_4",
    schedule_interval="*/1 * * * *"
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=1024,   
    tags=["autoscale", "mapping"],
) as dag:

    @task
    def gen_durations(default_n=DEFAULT_N, default_seconds=DEFAULT_SECONDS):
        """Читает n/sleep из dag_run.conf (если передали при триггере),
        иначе берёт дефолты. Возвращает список длительностей для .expand()."""
        ctx = get_current_context()
        conf = (getattr(ctx.get("dag_run"), "conf", None) or {})
        n = int(conf.get("n", default_n))
        seconds = int(conf.get("sleep", default_seconds))
        return [seconds] * n

    @task
    def sleeper(seconds: int):
        time.sleep(seconds)
        return seconds

    durations = gen_durations()
    sleeper.expand(seconds=durations)


