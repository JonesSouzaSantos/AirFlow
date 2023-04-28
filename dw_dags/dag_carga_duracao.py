import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import superdigital_mdc.dw.monitor.metrics as metricas
from superdigital_mdc.dw.utils import Constantes

logging.info('DAG de construcao de Estatisticas de Duracao de Carga.')

default_args = {
    'owner': 'Engenharia',
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

op_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__CONNECTION_STRING}

_id = 'carga_duracao'
dag = DAG('z_eng__%s' % _id,
          description='DAG de construcao de Estatisticas de Duracao de Cargas.',
          default_args=default_args,
          schedule_interval='0 4 * * *',
          max_active_runs=1,
          catchup=False,
          )

with dag:
    op_prepara = PythonOperator(task_id='%s__prepara' % _id,
                                provide_context=True,
                                op_kwargs=op_kwargs,
                                python_callable=metricas.prepara_repositorio_carga_duracao,
                                dag=dag)

    op_d1 = PythonOperator(task_id='%s__d1' % _id,
                           provide_context=True,
                           op_kwargs=op_kwargs,
                           python_callable=metricas.calcula_indicadores_D1,
                           dag=dag)

    # Por ser diario e expurgo de D90, datas anteriores nao precisam ser recalculadas
    # op_d7 = PythonOperator(task_id='%s__d7' % _id,
    #                        provide_context=True,
    #                        op_kwargs=op_kwargs,
    #                        python_callable=metricas.calcula_indicadores_D7,
    #                        dag=dag)
    #
    # op_d30 = PythonOperator(task_id='%s__d30' % _id,
    #                         provide_context=True,
    #                         op_kwargs=op_kwargs,
    #                         python_callable=metricas.calcula_indicadores_D30,
    #                         dag=dag)

    dag >> op_prepara
    op_prepara >> op_d1
    # op_d1 >> op_d7
    # op_d7 >> op_d30


