import logging
from datetime import datetime

import superdigital_mdc.di.allin as facade_allin
import superdigital_mdc.dw.monitor.jobs_db as jobs
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from superdigital_mdc.dw.utils import Constantes

_desc_dag = 'DAG de controle de Geracao da Integracao de Dashboard de Vendas com a Holos.'
logging.info(_desc_dag)

default_args = {
    'owner': 'SuperDW',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    # 'email': ['airflow@example.com'],
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

op_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__HML_CONNECTION_STRING}

_id = 'holos_dashboard_vendas'
dag = DAG('dw__%s' % _id,
          description=_desc_dag,
          default_args=default_args,
          schedule_interval='0 2 * * *',
          max_active_runs=1,
          catchup=False,
          )

with dag:
    op_0 = PythonOperator(task_id='%s__expurge' % _id,
                          provide_context=True,
                          op_kwargs=op_kwargs,
                          python_callable=jobs.expurge__holos_dashboarddevendas,
                          dag=dag)

    op_1 = PythonOperator(task_id='%s__runjob' % _id,
                          provide_context=True,
                          op_kwargs=op_kwargs,
                          python_callable=jobs.call__holos_dashboarddevendas,
                          dag=dag)

    op_2 = PythonOperator(task_id='%s__assert' % _id,
                          provide_context=True,
                          op_kwargs=op_kwargs,
                          python_callable=jobs.assert__holos_dashboarddevendas,
                          dag=dag)

    op_3 = PythonOperator(task_id='%s__ftp' % _id,
                         provide_context=True,
                         op_kwargs=op_kwargs,
                         python_callable=jobs.ftp__holos_dashboarddevendas,
                         dag=dag)

    op_4 = PythonOperator(task_id='%s__token' % _id,
                         provide_context=True,
                         op_kwargs=op_kwargs,
                         python_callable=facade_allin.busca_allin_token_v2,
                         dag=dag)

    op_5 = PythonOperator(task_id='%s__notificar_interessados' % _id,
                         provide_context=True,
                         op_kwargs=op_kwargs,
                         python_callable=jobs.notificar_interessados__holos_dashboarddevendas,
                         dag=dag)

    dag >> op_0
    op_0 >> op_1
    op_1 >> op_2
    op_2 >> op_3
    op_3 >> op_4
    op_4 >> op_5
