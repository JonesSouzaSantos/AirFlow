from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import superdigital_mdc.di.allin as di
import superdigital_mdc.di.atualiza_lista as bo
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

dag = DAG('cm_yc__atualiza_lista',
          description='DAG da integração com ALLIN - Atualização das listas de envio de email.',
          default_args=default_args,
          schedule_interval='30 19 * * *',
          max_active_runs=1,
          catchup=False,
          )

_op_prefix = 'atualiza_lista__'

op_token = PythonOperator(task_id='%s%s' % (_op_prefix, Constantes.KEY__ALLIN_API_TOKEN),
                          provide_context=True,
                          op_kwargs=op_kwargs,
                          python_callable=di.busca_allin_token,
                          dag=dag)

dag >> op_token

_op_bef = op_token

op_names = []
for id_definition in bo.obter_id_definition_4_envio_lista(**op_kwargs):
    # for id_definition in (_ for _ in bo.obter_id_definition_4_envio_lista(**op_kwargs) if _ not in [154]): # em caso de garantir a nao atualizacao

    _op_name = '%s%s' % (_op_prefix, id_definition)
    op_names.append(_op_name)

    op = PythonOperator(task_id=_op_name,
                        provide_context=True,
                        op_args=[id_definition],
                        op_kwargs=op_kwargs,
                        python_callable=di.envia_lista_by_id_definition,
                        dag=dag)
    _op_bef >> op
    _op_bef = op

op_resumo = PythonOperator(task_id='%sresumo' % _op_prefix,
                           provide_context=True,
                           op_args=[op_names],
                           op_kwargs=op_kwargs,
                           python_callable=di.resumo_atualiza_lista,
                           dag=dag)
_op_bef >> op_resumo
