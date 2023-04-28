import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

import superdigital_mdc.dw.monitor.carga_diaria as cd
from superdigital_mdc.dw.database import Constantes

from superdigital_mdc.dw.monitor.carga_diaria import ORIGENS_SUPER, PARAMS_CLEANSING
from superdigital_mdc.dw.monitor.carga_diaria import PIPES_PROCESSAMENTO, WATCHERS_PROCESSAMENTO

logging.info('DAG de Carga Diaria do DW da SuperDigital - Ingestao e Processamento.')


def get_sub_dag_especial(origem_row,
                         origens_sd,
                         parent_dag_name,
                         task_dag_name,
                         default_args,
                         schedule_interval,
                         op_kwargs):

    sdag_task = DAG('%s.%s' % (parent_dag_name, task_dag_name),
                    default_args=default_args,
                    schedule_interval=schedule_interval,
                    max_active_runs=1,
                    catchup=False,
                    )

    task_anterior = sdag_task

    for origem_super_digital in origens_sd:

        id_origem_sd = origem_super_digital['id_origem_sd']
        id_watcher_init = origem_super_digital['id_watcher']

        for id_watcher in range(200, 205):
            if id_watcher >= int(id_watcher_init):

                if id_watcher < 204:
                    task_atual = PythonOperator(task_id='%s.%s.%s.%s' % (task_dag_name,
                                                                         id_origem_sd,
                                                                         ORIGENS_SUPER[id_origem_sd],
                                                                         id_watcher),
                                                provide_context=True,
                                                op_kwargs=op_kwargs,
                                                python_callable=cd.stub,
                                                dag=sdag_task)

                    task_anterior >> task_atual
                    task_anterior = task_atual

                elif id_watcher == 204:
                    for id_param_cleansing in range(1, 5):
                        task_atual = PythonOperator(task_id='%s.%s.%s.%s.%s.%s' % (task_dag_name,
                                                                                   id_origem_sd,
                                                                                   ORIGENS_SUPER[id_origem_sd],
                                                                                   id_watcher,
                                                                                   id_param_cleansing,
                                                                                   PARAMS_CLEANSING[id_param_cleansing]
                                                                                   ),
                                                    provide_context=True,
                                                    op_kwargs=op_kwargs,
                                                    python_callable=cd.stub,
                                                    dag=sdag_task)
                        task_anterior >> task_atual
                        task_anterior = task_atual

    return sdag_task


def get_task_subdag_pipe_especial(origem_row,
                                  parent_dag_name,
                                  task_dag_name,
                                  default_args,
                                  schedule_interval,
                                  op_kwargs):

    if origem_row.id_origem == 18:
        return get_sub_dag_especial(origem_row,
                                    [{'id_origem_sd': '1001', 'id_watcher': '200'}],
                                    parent_dag_name,
                                    task_dag_name,
                                    default_args,
                                    schedule_interval,
                                    op_kwargs)
    elif origem_row.id_origem == 22:
        return get_sub_dag_especial(origem_row,
                                    [{'id_origem_sd': '1002', 'id_watcher': '200'}],
                                    parent_dag_name,
                                    task_dag_name,
                                    default_args,
                                    schedule_interval,
                                    op_kwargs)
    elif origem_row.id_origem == 35:
        return get_sub_dag_especial(origem_row,
                                    [{'id_origem_sd': '1005', 'id_watcher': '203'},
                                     {'id_origem_sd': '1006', 'id_watcher': '203'},
                                     {'id_origem_sd': '1007', 'id_watcher': '201'}],
                                    parent_dag_name,
                                    task_dag_name,
                                    default_args,
                                    schedule_interval,
                                    op_kwargs)
    elif origem_row.id_origem == 88:
        return get_sub_dag_especial(origem_row,
                                    [{'id_origem_sd': '1003', 'id_watcher': '203'},
                                     {'id_origem_sd': '1004', 'id_watcher': '203'}],
                                    parent_dag_name,
                                    task_dag_name,
                                    default_args,
                                    schedule_interval,
                                    op_kwargs)

    return None


def get_sub_dag_cleansing(parent_dag_name,
                          sub_dag_name,
                          default_args,
                          schedule_interval,
                          op_kwargs):

    sub_dag_full_name = '%s.%s' % (parent_dag_name, sub_dag_name)
    sdag_cleansing = DAG(sub_dag_full_name,
                         default_args=default_args,
                         schedule_interval=schedule_interval,
                         max_active_runs=1,
                         catchup=False,
                         )

    task_anterior = sdag_cleansing

    for index, origem in cd.obter_pipes_ingestao(**op_kwargs).iterrows():

        if origem.id_origem in [18, 22, 35, 88]:
            task_dag_name = 'cleansing.subdag.%s.%s' % (origem.id_origem, origem.objeto)

            task_atual = SubDagOperator(subdag=get_task_subdag_pipe_especial(origem,
                                                                             sub_dag_full_name,
                                                                             task_dag_name,
                                                                             default_args,
                                                                             schedule_interval,
                                                                             op_kwargs),
                                        task_id=task_dag_name,
                                        dag=sdag_cleansing)

            task_anterior >> task_atual
            task_anterior = task_atual

    return sdag_cleansing


def get_sub_dag_ingestao(parent_dag_name,
                         sub_dag_name,
                         default_args,
                         schedule_interval,
                         op_kwargs):

    sub_dag_full_name = '%s.%s' % (parent_dag_name, sub_dag_name)
    sdag_ingestao = DAG(sub_dag_full_name,
                        default_args=default_args,
                        schedule_interval=schedule_interval,
                        max_active_runs=1,
                        catchup=False,
                        )

    task_anterior = sdag_ingestao

    for index, origem in cd.obter_pipes_ingestao(**op_kwargs).iterrows():

        task_atual = PythonOperator(task_id='ingestao.%s.%s' % (origem.id_origem,
                                                                origem.objeto),
                                    provide_context=True,
                                    op_kwargs=op_kwargs,
                                    python_callable=cd.stub,
                                    dag=sdag_ingestao)

        task_anterior >> task_atual
        task_anterior = task_atual

    return sdag_ingestao


def get_sub_dag_processamento(parent_dag_name,
                              sub_dag_name,
                              default_args,
                              schedule_interval,
                              op_kwargs):

    sub_dag_full_name = '%s.%s' % (parent_dag_name, sub_dag_name)
    sdag_processamento = DAG(sub_dag_full_name,
                             default_args=default_args,
                             schedule_interval=schedule_interval,
                             max_active_runs=1,
                             catchup=False
                             )

    task_anterior = sdag_processamento

    for pipe_id in PIPES_PROCESSAMENTO:

        task_atual = PythonOperator(task_id='processamento.%s.%s' % (pipe_id,
                                                                     WATCHERS_PROCESSAMENTO[pipe_id]),
                                    provide_context=True,
                                    op_kwargs=op_kwargs,
                                    python_callable=cd.stub,
                                    dag=sdag_processamento)

        task_anterior >> task_atual
        task_anterior = task_atual

    return sdag_processamento


def get_main_dag():

    _id = 'carga_diaria'
    MAIN_DAG_NAME = 'dw.dag_%s' % _id
    INGESTAO_DAG_NAME = 'sub_dag_ingestao'
    CLEANSING_DAG_NAME = 'sub_dag_cleansing'
    PROCESSAMENTO_DAG_NAME = 'sub_dag_processamento'

    op_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__CONNECTION_STRING, }

    default_args = {
        'owner': 'SuperDW',
        'depends_on_past': False,
        'start_date': datetime(2019, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'catchup': False,
    }

    main_dag = DAG(MAIN_DAG_NAME,
                   description='DAG de Carga Diaria do DW da SuperDigital - Ingestao e Processamento.',
                   default_args=default_args,
                   schedule_interval='0 0 * * *',
                   max_active_runs=1,
                   catchup=False,
                   )

    sub_dag_ingestao = SubDagOperator(
        subdag=get_sub_dag_ingestao(MAIN_DAG_NAME,
                                    INGESTAO_DAG_NAME,
                                    default_args,
                                    main_dag.schedule_interval,
                                    op_kwargs),

        task_id=INGESTAO_DAG_NAME,
        dag=main_dag,
    )

    sub_dag_cleansing = SubDagOperator(
        subdag=get_sub_dag_cleansing(MAIN_DAG_NAME,
                                    CLEANSING_DAG_NAME,
                                    default_args,
                                    main_dag.schedule_interval,
                                    op_kwargs),

        task_id=CLEANSING_DAG_NAME,
        dag=main_dag,
    )

    sub_dag_processamento = SubDagOperator(
        subdag=get_sub_dag_processamento(MAIN_DAG_NAME,
                                         PROCESSAMENTO_DAG_NAME,
                                         default_args,
                                         main_dag.schedule_interval,
                                         op_kwargs),

        task_id=PROCESSAMENTO_DAG_NAME,
        dag=main_dag,
    )

    main_dag >> sub_dag_ingestao
    sub_dag_ingestao >> sub_dag_cleansing
    sub_dag_cleansing >> sub_dag_processamento

    return main_dag

main_dag = get_main_dag()
