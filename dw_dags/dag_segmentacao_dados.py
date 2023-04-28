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

id_dag_principal = 'CM_n5__segmentacao_dados'

main_dag = DAG(id_dag_principal,
               schedule_interval='00 14 * * *',
               max_active_runs=1,
               default_args={'owner': 'SuperDW', 'depends_on_past': False,
                                         'start_date': datetime.datetime(2019, 1, 1, 0, 0), 'email_on_failure': False,
                                         'email_on_retry': False, 'catchup': False},
               description="DAG que segmenta dados para campanhas diariamente.")

dag_parent = [main_dag]
last_ops = []
old_subdag_op = None

subdag = new_subdag(dag_id='{0}.ingestao'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: ingestao

op = new_operator(task_id='AuditoriaLogin', op_args=['Ingestao', {'extracao': 1}, 1], trigger_rule='all_success')

last_ops.append(op)

op = new_operator(task_id='AuditoriaLoginComplemento', op_args=['Ingestao', {'extracao': 101}, 1],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='BilheteTransporteConfirmacaoPedidoRecarga', op_args=['Ingestao', {'extracao': 5}, 1],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='BilheteTransporteConfirmacaoPedidoRecargaStatus', op_args=['Ingestao', {'extracao': 6}, 1],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='BilheteTransportePedidoRecarga', op_args=['Ingestao', {'extracao': 7}, 1],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='BilheteTransporteTransacao', op_args=['Ingestao', {'extracao': 10}, 1],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='CartoesEventos', op_args=['Ingestao', {'extracao': 103}, 1], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='ContasCorrentesComplemento', op_args=['Ingestao', {'extracao': 110}, 1],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='DadosInterop', op_args=['Ingestao', {'extracao': 111}, 1], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='PreCadastro', op_args=['Ingestao', {'extracao': 108}, 1], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='PreCadastroBureaus', op_args=['Ingestao', {'extracao': 109}, 1], trigger_rule='all_success')

last_ops.pop() >> op

subdag = dag_parent.pop()  # subdag: ingestao

subdag_op = new_subdag_operator(task_id='ingestao', trigger_rule='all_success', child_dag=subdag,
                                parent_dag=dag_parent[-1])


dag_parent[-1] >> subdag_op
old_subdag_op = subdag_op

subdag = new_subdag(dag_id='{0}.processamento'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: processamento

op = new_operator(task_id='LowBalance',
                  op_args=['Processamento', {'procedure': 'SuperDW.corp.sp_Carrega_LowBalance_p4003', 'parameters': []},
                           0], trigger_rule='all_success')

last_ops.append(op)

op = new_operator(task_id='AuditoriaLoginSO', op_args=['Processamento',
                                                       {'procedure': 'SuperDW.corp.sp_Carrega_AuditoriaLoginSO_p4004',
                                                        'parameters': []}, 1], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='Bilhete_Unico', op_args=['Processamento',
                                                    {'procedure': 'SuperDW.corp.sp_Carrega_BilheteUnico_p4002',
                                                     'parameters': []}, 1], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='DadosInterop', op_args=['Processamento',
                                                   {'procedure': 'SuperDW.corp.sp_Carrega_DadosInterop_p4001',
                                                    'parameters': []}, 1], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='Grupo_Economico', op_args=['Processamento',
                                                      {'procedure': 'SuperDW.corp.sp_Carrega_GrupoEconomico_p4005',
                                                       'parameters': []}, 1], trigger_rule='all_success')

last_ops.pop() >> op

subdag = dag_parent.pop()  # subdag: processamento

subdag_op = new_subdag_operator(task_id='processamento', trigger_rule='all_success', child_dag=subdag,
                                parent_dag=dag_parent[-1])


old_subdag_op >> subdag_op
old_subdag_op = subdag_op

subdag = new_subdag(dag_id='{0}.segmentacao'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: segmentacao

op = new_operator(task_id='SegmentacaoDados',
                  op_args=['Processamento',{'procedure':'SuperCM.dbo.SegmentacaoDados','parameters':[]},1],
                  trigger_rule='all_success')

last_ops.append(op)

op = new_operator(task_id='SegmentacaoDadosValidacao',
                  op_args=['Processamento',{'procedure':'SuperCM.dbo.SegmentacaoDadosPosValidacao','parameters':[]},1],
                  trigger_rule='all_success')

last_ops.pop() >> op

subdag = dag_parent.pop()  # subdag: segmentacao

subdag_op = new_subdag_operator(task_id='segmentacao', trigger_rule='all_success', child_dag=subdag,
                                parent_dag=dag_parent[-1])

old_subdag_op >> subdag_op

op, subdag, subdag_op, old_subdag_op = None, None, None, None
