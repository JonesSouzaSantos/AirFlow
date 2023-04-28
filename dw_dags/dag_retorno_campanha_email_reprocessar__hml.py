import logging
from datetime import date, datetime

import supercm_de.di.retorno_campanha_email as facade
import superdigital_mdc.di.allin as facade_allin

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

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

list_dt_reprocessar = facade.get_dias_a_reprocessar(**op_kwargs)
dt = date.today()

dag_desc = 'DAG de Reprocessamento de Retorno de Email [Server 85] datas[%s]' % list_dt_reprocessar
logging.info(dag_desc)

dag = DAG('_hml_alpha0__cm_n5__retorno_campanha_email_reprocessar',
          description=dag_desc,
          default_args=default_args,
          schedule_interval=None,
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
    for dt_ref in list_dt_reprocessar:
        dt_ref = date(*(int(_)for _ in dt_ref.split('-')))

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

    op_resumo = PythonOperator(task_id='%sresumo' % _op_prefix,
                               provide_context=True,
                               op_args=[op_names],
                               op_kwargs=op_kwargs,
                               python_callable=facade.resumo_retorno_campanhas,
                               dag=dag)
    _op_bef >> op_resumo
