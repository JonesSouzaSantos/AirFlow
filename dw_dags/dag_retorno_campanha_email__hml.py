import logging
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import supercm_de.di.retorno_campanha_email as facade
import superdigital_mdc.di.allin as facade_allin

from superdigital_mdc.dw.utils import Constantes

default_args = {
    'owner': 'Campanhas',
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

_dias_a_considerar = facade.get_dias_a_considerar(**op_kwargs)
dt_ref = date.today() - timedelta(days=int(_dias_a_considerar))
dt_fim = date.today()

# PARA TESTES
# dt_ref = date.today() - timedelta(days=1)
# dt_fim = date(2020, 2, 1)

logging.info('Definindo DAG a partir de [%s] por mais [%s] dias' % (dt_ref, _dias_a_considerar))

dag = DAG('_hml_alpha0__cm_n5__retorno_campanha_email',
          description='DAG responsavel pela analise do Retorno de Campanha de Emails do Campaign Manager N5 - 85.',
          default_args=default_args,
          schedule_interval='00 01 * * *',
          max_active_runs=1,
          catchup=False,
          )

_op_prefix = 'retorno_campanha_email__'

with dag:
    op_token = PythonOperator(task_id='%s%s' % (_op_prefix, Constantes.KEY__ALLIN_API_TOKEN),
                              provide_context=True,
                              op_kwargs=op_kwargs,
                              python_callable=facade_allin.busca_allin_token,
                              dag=dag)

    dag >> op_token

    _op_bef = op_token

    op_names = []
    while dt_ref < dt_fim:
        _op_name = '%s%s-%s-%s' % (_op_prefix, dt_ref.year, dt_ref.month, dt_ref.day)
        op_names.append(_op_name)
        op = PythonOperator(task_id=_op_name,
                            provide_context=True,
                            op_args=[dt_ref],
                            op_kwargs=op_kwargs,
                            python_callable=facade.analisa_retorno_campanhas,
                            dag=dag)

        _op_bef >> op
        _op_bef = op

        dt_ref += timedelta(days=1)

    op_resumo = PythonOperator(task_id='%sresumo' % _op_prefix,
                               provide_context=True,
                               op_args=[op_names],
                               op_kwargs=op_kwargs,
                               python_callable=facade.resumo_retorno_campanhas,
                               dag=dag)
    _op_bef >> op_resumo
