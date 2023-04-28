import logging
from datetime import datetime

import superdigital_mdc.dw.etl.ingestao as facade
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from superdigital_de.di.repositorios import Extracao, FonteDados
from superdigital_mdc.dw.utils import Constantes

logging.info('DAG de controle de Extracao de Fonte de Dados.')

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
hml_op_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__HML_CONNECTION_STRING}


def create_dag(env, op_conf):
    _id = 'extracao_diaria'
    dag = DAG('%sz_eng__%s' % (env, _id),
              description='DAG de controle de Extracao de Fonte de Dados.%s' % env,
              default_args=default_args,
              schedule_interval=None,  # '0 4 * * *',
              max_active_runs=1,
              catchup=False,
              )

    with dag:
        op_bef = dag
        op_names = []
        for extracao, origem, destino in facade.obter_extracao_diaria_ativa(**op_conf):
            _op_name = '%s__E_%s_O_%s_D_%s' % (
                _id, extracao[Extracao.ID_EXTRACAO], origem[FonteDados.NOME].split('.')[-1],
                destino[FonteDados.NOME].split('.')[-1])
            op = PythonOperator(task_id=_op_name,
                                provide_context=True,
                                op_kwargs=op_conf,
                                op_args=[extracao, origem, destino],
                                python_callable=facade.iniciar_ingestao,
                                dag=dag)

            op_names.append(_op_name)

            op_bef >> op
            op_bef = op

        op_resumo = PythonOperator(task_id='%s__resumo' % _id,
                                   provide_context=True,
                                   op_args=[op_names],
                                   op_kwargs=op_conf,
                                   python_callable=facade.resumo_extracao,
                                   dag=dag)
        op_bef >> op_resumo

        return dag


# dag = create_dag('', op_kwargs)
# dag_hml = create_dag('_hml_', hml_op_kwargs) if True else None
