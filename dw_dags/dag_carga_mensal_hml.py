import datetime

from airflow import DAG
from airflow.executors.local_executor import LocalExecutor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from superdigital_mdc.dw.etl.operadores_factory import operator_factory
from superdigital_mdc.dw.utils import Constantes

local_executor = LocalExecutor()

def new_subdag(dag_id):
    return DAG(
        dag_id=dag_id,
        default_args={'owner': 'SuperDW', 'depends_on_past': False, 'start_date': datetime.datetime(2019, 1, 1, 0, 0),
                      'email_on_failure': False, 'email_on_retry': False, 'catchup': False},
        max_active_runs=1,
        catchup=False
    )

def new_subdag_operator(task_id, trigger_rule, child_dag, parent_dag):
    subdag_op = SubDagOperator(
        task_id=task_id,
        provide_context=True,
        subdag=child_dag,
        default_args={'owner': 'SuperDW', 'depends_on_past': False, 'start_date': datetime.datetime(2019, 1, 1, 0, 0),
                      'email_on_failure': False, 'email_on_retry': False, 'catchup': False},
        dag=parent_dag,
        trigger_rule=trigger_rule,
        executor=local_executor
    )
    return subdag_op

def new_operator(task_id, op_args, trigger_rule):

    op_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__HML_CONNECTION_STRING, }

    return PythonOperator(
        task_id=task_id,
        provide_context=True,
        op_args=op_args,
        op_kwargs=op_kwargs,
        python_callable=operator_factory,
        dag=dag_parent[-1],
        trigger_rule=trigger_rule
    )

id_dag_principal = '_hml_dw__dag_carga_mensal'

main_dag = DAG(id_dag_principal,
               max_active_runs=1,
               schedule_interval=None,
               default_args={'owner': 'SuperDW', 'depends_on_past': False,
                                         'start_date': datetime.datetime(2019, 1, 1, 0, 0), 'email_on_failure': False,
                                         'email_on_retry': False, 'catchup': False},
               description="DAG que gerencia os passos da carga mensal.")

dag_parent = [main_dag]
last_ops = []

op = new_operator(task_id='RESET_MONITORIA',
                  op_args=['Processamento', {'procedure': 'dbo.SPR_CARGA_MENSAL_RESET_MONITORIA', 'parameters': []}, 0],
                  trigger_rule='all_success')

last_ops.append(op)

op = new_operator(task_id='ComportamentoDeUsoMensal', op_args=['Processamento', {
    'procedure': 'dbo.SP_PB_ProcessoComportamentoDeUsoMensal', 'parameters': []}, 1], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='TempoDeVida',
                  op_args=['Processamento', {'procedure': 'dbo.SP_PB_ProcessoTempoDeVida', 'parameters': []}, 1],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='Dash_Safras',
                  op_args=['Processamento', {'procedure': 'dbo.SP_PB_Safras_Mensal', 'parameters': []}, 1],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op, subdag, subdag_op = None, None, None