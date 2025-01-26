from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv
import os

# Uso de variáveis de ambiente seguindo boas práticas
load_dotenv()

# Diretórios 
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 3,
}

with DAG(
    'dag_dbt_run_and_test',
    default_args=default_args,
    description='Dag que roda os modelos e os testes.',
    schedule_interval='@daily',
) as dag:

    # Rodar modelos
    dbt_run_models = BashOperator(
        task_id='dbt_run_models',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} && '
            f'dbt run --profiles-dir {DBT_PROFILES_DIR}'
        ),
    )

    # Rodar testes
    dbt_run_tests = BashOperator(
        task_id='dbt_run_tests',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} && '
            f'dbt test --profiles-dir {DBT_PROFILES_DIR}'
        ),
    )

    dbt_run_models >> dbt_run_tests
