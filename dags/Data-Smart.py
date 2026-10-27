"""DAG diario: CSV -> Bronze -> Silver -> Gold."""

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from Etl.Bronce import extraer_csv_a_bronce
from Etl.Gold import cargar_ventas_diarias
from Etl.Silver import transformar_online_retail

CONN_ID = "warehouse_db"
CHUNK_SIZE = 50_000


@dag(
    dag_id="datamart_etl",
    description="Carga diaria de ventas e historial en PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["datamart", "etl"],
)
def datamart_etl():
    def extraer(archivo, fuente):
        ruta = Variable.get(archivo)
        filas = extraer_csv_a_bronce(
            PostgresHook(postgres_conn_id=CONN_ID),
            ruta,
            int(Variable.get("etl_chunk_size", default_var=str(CHUNK_SIZE))),
            fuente,
        )
        logging.info("%s: se extrajeron %s filas", fuente, filas)
        return filas

    @task(task_id="extraer_ventas_diarias_a_bronce")
    def extraer_ventas():
        return extraer("online_retail_csv_path", "online_retail")

    @task(task_id="extraer_historial_a_bronce")
    def extraer_historial():
        return extraer("online_retail_ii_csv_path", "online_retail_ii")

    @task(task_id="transformar_a_silver")
    def transformar():
        return transformar_online_retail(
            PostgresHook(postgres_conn_id=CONN_ID),
            int(Variable.get("etl_chunk_size", default_var=str(CHUNK_SIZE))),
        )

    @task(task_id="validar_calidad_de_datos")
    def validar_calidad(resultado):
        total = resultado["loaded"] + resultado["rejected"]
        tasa_rechazo = resultado["rejected"] / total if total else 0
        maximo = float(Variable.get("max_rejection_rate", default_var="0.05"))

        if not 0 <= maximo <= 1:
            raise ValueError("max_rejection_rate debe estar entre 0 y 1")
        if tasa_rechazo > maximo:
            raise ValueError(
                f"Calidad rechazada: {tasa_rechazo:.2%} de filas inválidas "
                f"(máximo permitido: {maximo:.2%})"
            )

        logging.info(
            "Calidad validada: %.2f%% de filas rechazadas",
            tasa_rechazo * 100,
        )
        return resultado["loaded"]

    @task(task_id="cargar_resultados_a_gold")
    def cargar_gold(filas_silver):
        filas_gold = cargar_ventas_diarias(
            PostgresHook(postgres_conn_id=CONN_ID)
        )
        logging.info(
            "Gold: %s agregados publicados a partir de %s filas",
            filas_gold,
            filas_silver,
        )

    ventas = extraer_ventas()
    historial = extraer_historial()
    silver = transformar()
    calidad = validar_calidad(silver)
    [ventas, historial] >> silver >> calidad >> cargar_gold(calidad)


dag = datamart_etl()
