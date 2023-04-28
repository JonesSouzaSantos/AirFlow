from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import superdigital_mdc.dw.monitor.carga_diaria as cd
from superdigital_mdc.dw.database import Constantes


def get_main_dag():

    _id = 'carga_diaria_metadados'
    MAIN_DAG_NAME = 'z_eng__%s' % _id

    op_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__CONNECTION_STRING, }

    default_args = {
        'owner': 'Engenharia',
        'depends_on_past': False,
        'start_date': datetime.today(),
        'email_on_failure': False,
        'email_on_retry': False,
        'catchup': False,
    }

    main_dag = DAG(MAIN_DAG_NAME,
                   description='DAG de Consulta de Metadados (Status e Logs) da Carga Diaria do DW.',
                   default_args=default_args,
                   schedule_interval='*/5 * * * *',
                   dagrun_timeout=timedelta(minutes=5),
                   max_active_runs=1,
                   catchup=False,
                   )

    task_atual = PythonOperator(task_id='carga_diaria_metadados__consulta_status_logs',
                                provide_context=True,
                                op_kwargs=op_kwargs,
                                python_callable=cd.atualizar_logs_status_carga_diaria,
                                dag=main_dag)

    main_dag >> task_atual

    return main_dag

main_dag = get_main_dag()