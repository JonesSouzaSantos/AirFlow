from datetime import datetime

import superdigital_de.di.transmite_push as facade
import superdigital_mdc.di.allin as facade_allin
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from superdigital_de.di.repositorios import ExecutionLog
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

op_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__CONNECTION_STRING, }

dag = DAG('cm_n5__transmitir_push',
          description='DAG responsavel por realizar transmissao de PUSH do Campaign Manager N5.',
          default_args=default_args,
          schedule_interval='30 14 * * *',
          max_active_runs=1,
          catchup=False,
          )

_op_prefix = 'transmite_push__'

op_token = PythonOperator(task_id='%s%s' % (_op_prefix, Constantes.KEY__ALLIN_API_TOKEN),
                          provide_context=True,
                          op_kwargs=op_kwargs,
                          python_callable=facade_allin.busca_allin_token_v2,
                          dag=dag)

dag >> op_token

_op_bef = op_token

op_names = []

for executionLog in facade.obter_execucoes_a_transmitir(**op_kwargs):
    id_definition = executionLog[ExecutionLog.ID_DEFINITION]
    id_execution = executionLog[ExecutionLog.ID_EXECUTION]

    _op_name = '%s%s' % (_op_prefix, id_definition)
    if op_names.count(_op_name) > 0:
        _op_name = '%s_%s' % (_op_name, id_execution)
    op_names.append(_op_name)

    op = PythonOperator(task_id=_op_name,
                        provide_context=True,
                        op_args=[id_definition, id_execution],
                        op_kwargs=op_kwargs,
                        python_callable=facade.transmitir_push_by_idDefinitionIdExecution,
                        dag=dag)
    _op_bef >> op
    _op_bef = op

op_resumo = PythonOperator(task_id='%sresumo' % _op_prefix,
                           provide_context=True,
                           op_args=[op_names],
                           op_kwargs=op_kwargs,
                           python_callable=facade.resumo_transmitir_push,
                           dag=dag)
_op_bef >> op_resumo
