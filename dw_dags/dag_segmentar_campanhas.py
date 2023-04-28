from datetime import datetime

import superdigital_de.di.segmenta_campanha as facade
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from superdigital_de.di.repositorios import Campanha
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

dag = DAG('cm_n5__segmentar_campanhas',
          description='DAG responsavel por inicializar a Segmentacao de Campanhas do Campaign Manager N5.',
          default_args=default_args,
          schedule_interval='00 17 * * *',
          max_active_runs=1,
          catchup=False,
          )

_op_prefix = 'segmentar_campanhas__'

_op_bef = dag

op_names = []

for campanha in facade.obter_campanhas_a_segmentar(**op_kwargs):
    id_definition = campanha[Campanha.ID_DEFINITION]

    _op_name = '%s_%s' % (_op_prefix, id_definition)
    op_names.append(_op_name)

    op = PythonOperator(task_id=_op_name,
                        provide_context=True,
                        op_args=[campanha],
                        op_kwargs=op_kwargs,
                        python_callable=facade.segmentar_campanha,
                        dag=dag)
    _op_bef >> op
    _op_bef = op

op_resumo = PythonOperator(task_id='%sresumo' % _op_prefix,
                           provide_context=True,
                           op_args=[op_names],
                           op_kwargs=op_kwargs,
                           python_callable=facade.resumo_segmentar_campanha,
                           dag=dag)
_op_bef >> op_resumo
