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
                      'email_on_failure': False, 'email_on_retry': False, 'catchup': False,
                      'pool':'pool_carga_diaria'},
        max_active_runs=1,
        catchup=False
    )

def new_subdag_operator(task_id, trigger_rule, child_dag, parent_dag):
    subdag_op = SubDagOperator(
        task_id=task_id,
        provide_context=True,
        subdag=child_dag,
        default_args={'owner': 'SuperDW', 'depends_on_past': False, 'start_date': datetime.datetime(2019, 1, 1, 0, 0),
                      'email_on_failure': False, 'email_on_retry': False, 'catchup': False,
                      'pool':'pool_carga_diaria'},
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

id_dag_principal = '_hml_dw__teste_estrutura_dag'

main_dag = DAG(id_dag_principal, default_args={'owner': 'SuperDW', 'depends_on_past': False,
                                         'start_date': datetime.datetime(2019, 1, 1, 0, 0), 'email_on_failure': False,
                                         'email_on_retry': False, 'catchup': False, 'pool':'pool_carga_diaria'},
               schedule_interval='00 05 * * *',
               max_active_runs=1,
               description="DAG que gerencia os passos da carga diaria.")

dag_parent = [main_dag]
last_ops = []

subdag = new_subdag(dag_id='{0}.ingestao'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: ingestao

subdag = new_subdag(dag_id='{0}.tabelas_dominio_paralelo'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: tabelas_dominio_paralelo

op = new_operator(task_id='Bancos', op_args=['Ingestao', {'extracao': 2}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='CartoesMotivoCancelamento', op_args=['Ingestao', {'extracao': 15}, 0],
                  trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='CartoesStatus', op_args=['Ingestao', {'extracao': 16}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='ClassesProfissionais', op_args=['Ingestao', {'extracao': 18}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='ClassificacaoEmpresa', op_args=['Ingestao', {'extracao': 19}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='CoafOperacao', op_args=['Ingestao', {'extracao': 20}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Estados', op_args=['Ingestao', {'extracao': 30}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='EstadosCivis', op_args=['Ingestao', {'extracao': 31}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Funcionalidade', op_args=['Ingestao', {'extracao': 106}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='FormasConstituicao', op_args=['Ingestao', {'extracao': 36}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='LocaisNascimento', op_args=['Ingestao', {'extracao': 40}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='MarcacoesOrigem', op_args=['Ingestao', {'extracao': 105}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='MarcacoesOrigemDetalhe', op_args=['Ingestao', {'extracao': 104}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Modalidades', op_args=['Ingestao', {'extracao': 46}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Moedas', op_args=['Ingestao', {'extracao': 47}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='MotivosBloqueioCartao', op_args=['Ingestao', {'extracao': 48}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='NivelResponsavel', op_args=['Ingestao', {'extracao': 50}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Paises', op_args=['Ingestao', {'extracao': 51}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Parametros', op_args=['Ingestao', {'extracao': 52}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='ParametrosFixosModalidades', op_args=['Ingestao', {'extracao': 53}, 0],
                  trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Parceiros', op_args=['Ingestao', {'extracao': 55}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Profissoes', op_args=['Ingestao', {'extracao': 70}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='RamosAtividade', op_args=['Ingestao', {'extracao': 71}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Status', op_args=['Ingestao', {'extracao': 77}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='StatusTransacao', op_args=['Ingestao', {'extracao': 79}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='SubRamosAtividade', op_args=['Ingestao', {'extracao': 81}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='TipoPortador', op_args=['Ingestao', {'extracao': 84}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='TiposCartoes', op_args=['Ingestao', {'extracao': 86}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='TiposIndividualizacao', op_args=['Ingestao', {'extracao': 88}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='TiposLancamento', op_args=['Ingestao', {'extracao': 89}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='TiposLogradouro', op_args=['Ingestao', {'extracao': 90}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='TiposMidias', op_args=['Ingestao', {'extracao': 92}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='TiposTelefone', op_args=['Ingestao', {'extracao': 93}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='TipoTransacao', op_args=['Ingestao', {'extracao': 94}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='TransacoesStatus', op_args=['Ingestao', {'extracao': 96}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

subdag = dag_parent.pop()  # subdag: tabelas_dominio_paralelo

subdag_op = new_subdag_operator(task_id='tabelas_dominio_paralelo', trigger_rule='all_done', child_dag=subdag,
                                parent_dag=dag_parent[-1])

dag_parent[-1] >> subdag_op
old_subdag_op = subdag_op

subdag = new_subdag(dag_id='{0}.outras_tabelas_paralelo'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: outras_tabelas_paralelo

op = new_operator(task_id='Artes', op_args=['Ingestao', {'extracao': 102}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='BlackListDocTed', op_args=['Ingestao', {'extracao': 11}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='CanalMarcacao', op_args=['Ingestao', {'extracao': 13}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Cartoes', op_args=['Ingestao', {'extracao': 14}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='CellCardCompras', op_args=['Ingestao', {'extracao': 17}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='DestinosViagem', op_args=['Ingestao', {'extracao': 28}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Dispositivos', op_args=['Ingestao', {'extracao': 29}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Favorecidos', op_args=['Ingestao', {'extracao': 34}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='GrupoEconomico', op_args=['Ingestao', {'extracao': 37}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='LotesArquivos', op_args=['Ingestao', {'extracao': 41}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='LotesArquivosEnvios', op_args=['Ingestao', {'extracao': 42}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='LotesCartoes', op_args=['Ingestao', {'extracao': 43}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='MarcacoesOrigemCampanha', op_args=['Ingestao', {'extracao': 45}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='PessoasFisicasContasCorrentes', op_args=['Ingestao', {'extracao': 57}, 0],
                  trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='PessoasFisicasTelefones', op_args=['Ingestao', {'extracao': 60}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='PessoasJuridicas', op_args=['Ingestao', {'extracao': 61}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='PessoasJuridicasContasCorrentes', op_args=['Ingestao', {'extracao': 62}, 0],
                  trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='PessoasJuridicasEmails', op_args=['Ingestao', {'extracao': 63}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='PessoasJuridicasEnderecos', op_args=['Ingestao', {'extracao': 64}, 0],
                  trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='PessoasJuridicasSocios', op_args=['Ingestao', {'extracao': 65}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='PessoasJuridicasTelefones', op_args=['Ingestao', {'extracao': 66}, 0],
                  trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='ProdutoMarcacao', op_args=['Ingestao', {'extracao': 68}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Responsaveis', op_args=['Ingestao', {'extracao': 72}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='ResultadoSaldoColunarFinanceiro', op_args=['Ingestao', {'extracao': 73}, 0],
                  trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='SegmentoMarcacao', op_args=['Ingestao', {'extracao': 74}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Transacoes', op_args=['Ingestao', {'extracao': 95}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='TransacoesTransferencias', op_args=['Ingestao', {'extracao': 97}, 0],
                  trigger_rule='all_done')

dag_parent[-1] >> op

op = new_operator(task_id='Usuarios', op_args=['Ingestao', {'extracao': 100}, 0], trigger_rule='all_done')

dag_parent[-1] >> op

subdag = dag_parent.pop()  # subdag: outras_tabelas_paralelo

subdag_op = new_subdag_operator(task_id='outras_tabelas_paralelo', trigger_rule='all_done', child_dag=subdag,
                                parent_dag=dag_parent[-1])

old_subdag_op >> subdag_op


op = new_operator(task_id='ContaCorrenteMarcacao', op_args=['Ingestao', {'extracao': 21}, 0], trigger_rule='all_done')

subdag_op >> op

last_ops.append(op)

op = new_operator(task_id='ContasCorrentes', op_args=['Ingestao', {'extracao': 22}, 0], trigger_rule='all_done')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='ContasCorrentesModalidades', op_args=['Ingestao', {'extracao': 23}, 0],
                  trigger_rule='all_done')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='Eventos', op_args=['Ingestao', {'extracao': 33}, 0], trigger_rule='all_done')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='Lancamentos', op_args=['Ingestao', {'extracao': 38}, 0], trigger_rule='all_done')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='PessoasFisicas', op_args=['Ingestao', {'extracao': 56}, 0], trigger_rule='all_done')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='PessoasFisicasEmails', op_args=['Ingestao', {'extracao': 58}, 0], trigger_rule='all_done')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='PessoasFisicasEnderecos', op_args=['Ingestao', {'extracao': 59}, 0], trigger_rule='all_done')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='PortadoresCartoes', op_args=['Ingestao', {'extracao': 67}, 0], trigger_rule='all_done')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='TCardsMC', op_args=['Ingestao', {'extracao': 82}, 0], trigger_rule='all_done')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='TCardsTB', op_args=['Ingestao', {'extracao': 83}, 0], trigger_rule='all_done')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='TTransactionsMC', op_args=['Ingestao', {'extracao': 98}, 0], trigger_rule='all_done')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='TTransactionsTB', op_args=['Ingestao', {'extracao': 99}, 0], trigger_rule='all_done')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='FuncionalidadeAtual', op_args=['Ingestao', {'extracao': 107}, 0], trigger_rule='all_done')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='Resumo_Ingestao', op_args=['Ingestao_Resumo', {}, 0], trigger_rule='all_done')

last_ops.pop() >> op

subdag = dag_parent.pop()  # subdag: ingestao

subdag_op = new_subdag_operator(task_id='ingestao', trigger_rule='all_success', child_dag=subdag,
                                parent_dag=main_dag)

dag_parent[-1] >> subdag_op
subdag_ingestao = subdag_op

subdag = new_subdag(dag_id='{0}.cleansing'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: cleansing

subdag = new_subdag(dag_id='{0}.cadastro'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: cadastro

op = new_operator(task_id='repopula_cadastro_PF',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_CADASTRO_REPOPULA', 'parameters': [1001]}, 0],
                  trigger_rule='all_success')

last_ops.append(op)

op = new_operator(task_id='repopula_cadastro_PJ',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_CADASTRO_REPOPULA', 'parameters': [1002]}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='pre_cleansing_cadastro',
                  op_args=['Cleansing', {'procedure': 'dim.spr_cleansing_nome_popula', 'parameters': []}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='cleansing_cadastro',
                  op_args=['Cleansing', {'procedure': 'mds.spr_Cleansing_Executa_BAT', 'parameters': ['nome.bat']}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='pos_cleansing_cadastro',
                  op_args=['Cleansing', {'procedure': 'dim.spr_cleansing_nome_atualiza', 'parameters': []}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

subdag = dag_parent.pop()  # subdag: cadastro

subdag_op = new_subdag_operator(task_id='cadastro', trigger_rule='all_success', child_dag=subdag,
                                parent_dag=dag_parent[-1])

old_subdag_op = subdag_op

subdag = new_subdag(dag_id='{0}.endereco'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: endereco

op = new_operator(task_id='repopula_endereco_PF',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_ENDERECO_REPOPULA', 'parameters': [1001]}, 0],
                  trigger_rule='all_success')

last_ops.append(op)

op = new_operator(task_id='repopula_endereco_PJ',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_ENDERECO_REPOPULA', 'parameters': [1002]}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='repopula_endereco_cadastro_santander',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_ENDERECO_REPOPULA', 'parameters': [1007]}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='pre_cleansing_endereco',
                  op_args=['Cleansing', {'procedure': 'dim.spr_cleansing_endereco_popula', 'parameters': []}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='cleansing_endereco',
                  op_args=['Cleansing', {'procedure': 'mds.spr_Cleansing_Executa_BAT', 'parameters': ['endereco.bat']},
                           0], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='pos_cleansing_endereco',
                  op_args=['Cleansing', {'procedure': 'dim.spr_cleansing_endereco_atualiza', 'parameters': []}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

subdag = dag_parent.pop()  # subdag: endereco

subdag_op = new_subdag_operator(task_id='endereco', trigger_rule='all_success', child_dag=subdag,
                                parent_dag=dag_parent[-1])

old_subdag_op >> subdag_op
old_subdag_op = subdag_op

subdag = new_subdag(dag_id='{0}.telefone'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: telefone

op = new_operator(task_id='repopula_telefone_PF',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_TELEFONE_REPOPULA', 'parameters': [1001]}, 0],
                  trigger_rule='all_success')

last_ops.append(op)

op = new_operator(task_id='repopula_telefone_PJ',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_TELEFONE_REPOPULA', 'parameters': [1002]}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='repopula_telefone_cadastro_santander',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_TELEFONE_REPOPULA', 'parameters': [1007]}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='pre_cleansing_telefone',
                  op_args=['Cleansing', {'procedure': 'dim.spr_cleansing_telefone_popula', 'parameters': []}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='cleansing_telefone',
                  op_args=['Cleansing', {'procedure': 'mds.spr_Cleansing_Executa_BAT', 'parameters': ['telefone.bat']},
                           0], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='pos_cleansing_telefone',
                  op_args=['Cleansing', {'procedure': 'dim.spr_cleansing_telefone_atualiza', 'parameters': []}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

subdag = dag_parent.pop()  # subdag: telefone

subdag_op = new_subdag_operator(task_id='telefone', trigger_rule='all_success', child_dag=subdag,
                                parent_dag=dag_parent[-1])

old_subdag_op >> subdag_op
old_subdag_op = subdag_op

subdag = new_subdag(dag_id='{0}.email'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: email

op = new_operator(task_id='repopula_email_PF',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_EMAIL_REPOPULA', 'parameters': [1001]}, 0],
                  trigger_rule='all_success')

last_ops.append(op)

op = new_operator(task_id='repopula_email_PJ',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_EMAIL_REPOPULA', 'parameters': [1002]}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='repopula_email_usuario_PF',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_EMAIL_REPOPULA', 'parameters': [1003]}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='repopula_email_usuario_PJ',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_EMAIL_REPOPULA', 'parameters': [1004]}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='repopula_email_conta_corrente_PF',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_EMAIL_REPOPULA', 'parameters': [1005]}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='repopula_email_conta_corrente_PJ',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_EMAIL_REPOPULA', 'parameters': [1006]}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='repopula_email_cadastro_santander',
                  op_args=['Cleansing', {'procedure': 'dim.SPR_AUX_DIM_EMAIL_REPOPULA', 'parameters': [1007]}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='pre_cleansing_email',
                  op_args=['Cleansing', {'procedure': 'dim.spr_cleansing_email_popula', 'parameters': []}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='cleansing_email',
                  op_args=['Cleansing', {'procedure': 'mds.spr_Cleansing_Executa_BAT', 'parameters': ['email.bat']}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='pos_cleansing_email',
                  op_args=['Cleansing', {'procedure': 'dim.spr_cleansing_email_atualiza', 'parameters': []}, 0],
                  trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)
last_ops.pop()

subdag = dag_parent.pop()  # subdag: email

subdag_op = new_subdag_operator(task_id='email', trigger_rule='all_success', child_dag=subdag,
                                parent_dag=dag_parent[-1])

old_subdag_op >> subdag_op
old_subdag_op = subdag_op

subdag = dag_parent.pop()  # subdag: cleansing

subdag_op = new_subdag_operator(task_id='cleansing', trigger_rule='all_success', child_dag=subdag,
                                parent_dag=dag_parent[-1])

subdag_ingestao >> subdag_op
subdag_cleansing = subdag_op

subdag = new_subdag(dag_id='{0}.processamento'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: processamento

op = new_operator(task_id='DadosCadastrais', op_args=['Processamento',
                                                      {'procedure': 'SuperDW.corp.sp_Atualiza_DadosCadastrais_p3001',
                                                       'parameters': []}, 0], trigger_rule='all_success')

last_ops.append(op)

op = new_operator(task_id='Gera_NUMDBM',
                  op_args=['Processamento', {'procedure': 'SuperDW.corp.sp_Atualiza_NUMDBM_p3002', 'parameters': []},
                           0], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='DBM_Cadastro', op_args=['Processamento',
                                                   {'procedure': 'dim.spr_aux_Superdigital_atualiza_DBM_CADASTRO',
                                                    'parameters': []}, 0], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='Contas_Correntes', op_args=['Processamento',
                                                       {'procedure': 'SuperDW.corp.sp_Carrega_ContaCorrente_p3004',
                                                        'parameters': []}, 0], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='Cartoes', op_args=['Processamento',
                                              {'procedure': 'SuperDW.corp.sp_Carrega_Cartao_p3005',
                                               'parameters': []}, 0], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='Lancamentos', op_args=['Processamento',
                                                  {'procedure': 'dbo.spr_prd_Superdigital_Carrega_Atualiza_Lancamentos',
                                                   'parameters': []}, 0], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='Relacao_Empresa', op_args=['Processamento',
                                                      {'procedure': 'SuperDW.corp.sp_Carrega_RelacaoEmpresa_p3007',
                                                       'parameters': []}, 0], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

op = new_operator(task_id='Assinatura', op_args=['Processamento',
                                                 {'procedure': 'SuperDW.corp.sp_Carrega_Assinaturas_p3009',
                                                  'parameters': []}, 0], trigger_rule='all_success')

last_ops.pop() >> op

last_ops.append(op)

subdag = new_subdag(dag_id='{0}.processamento_paralelo'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: processamento_paralelo

op = new_operator(task_id='Transacoes', op_args=['Processamento',
                                                 {'procedure': 'SuperDW.corp.sp_Carrega_Transacoes_p3010',
                                                  'parameters': []}, 0], trigger_rule='all_success')

dag_parent[-1] >> op

op = new_operator(task_id='Saldos_Diarios', op_args=['Processamento',
                                                     {'procedure': 'SuperDW.corp.sp_Carrega_SaldosDiarios_p3008',
                                                      'parameters': []}, 0], trigger_rule='all_success')

dag_parent[-1] >> op

op = new_operator(task_id='Tarifas_Devidas', op_args=['Processamento',
                                                      {'procedure': 'SuperDW.corp.sp_Carrega_TarifasDevidas_p3011',
                                                       'parameters': []}, 0], trigger_rule='all_success')

dag_parent[-1] >> op

op = new_operator(task_id='Cadastro_Email', op_args=['Processamento',
                                                     {'procedure': 'SuperDW.corp.sp_Carrega_CadastroEmail_p3012',
                                                      'parameters': []}, 0], trigger_rule='all_success')

dag_parent[-1] >> op

op = new_operator(task_id='Cadastro_Endereco', op_args=['Processamento',
                                                        {'procedure': 'SuperDW.corp.sp_Carrega_CadastroEndereco_p3013',
                                                         'parameters': []}, 0], trigger_rule='all_success')

dag_parent[-1] >> op

op = new_operator(task_id='Cadastro_Telefone', op_args=['Processamento',
                                                        {'procedure': 'SuperDW.corp.sp_Carrega_CadastroTelefone_p3014',
                                                         'parameters': []}, 0], trigger_rule='all_success')

dag_parent[-1] >> op

subdag = dag_parent.pop()  # subdag: processamento_paralelo

subdag_op = new_subdag_operator(task_id='processamento_paralelo', trigger_rule='all_success', child_dag=subdag,
                                parent_dag=dag_parent[-1])

last_ops.pop() >> subdag_op

subdag = dag_parent.pop()  # subdag: processamento

subdag_op = new_subdag_operator(task_id='processamento', trigger_rule='all_success', child_dag=subdag,
                                parent_dag=dag_parent[-1])

subdag_cleansing >> subdag_op
subdag_processamento = subdag_op

subdag = new_subdag(dag_id='{0}.carga_extendida'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: carga_extendida

op = new_operator(task_id='ContaCorrenteDiaria', op_args=['Processamento', {
    'procedure': 'dbo.spr_prd_Superdigital_Carrega_Atualiza_ContaCorrenteDiaria', 'parameters': []}, 0],
                  trigger_rule='all_done')

last_ops.append(op)

subdag = new_subdag(dag_id='{0}.dashboards_paralelo'.format(dag_parent[-1].dag_id))

dag_parent.append(subdag)  # subdag: dashboards_paralelo

op = new_operator(task_id='SP_PB_FastAnalysis',
                  op_args=['Processamento', {'procedure': 'dbo.SP_PB_FastAnalysis', 'parameters': []}, 0],
                  trigger_rule='all_success')

dag_parent[-1] >> op

op = new_operator(task_id='SP_PB_Batida_Diaria_Churn',
                  op_args=['Processamento', {'procedure': 'dbo.SP_PB_Batida_Diaria_Churn', 'parameters': []}, 0],
                  trigger_rule='all_success')

dag_parent[-1] >> op

op = new_operator(task_id='SP_PB_Base_Dinamica_90',
                  op_args=['Processamento', {'procedure': 'dbo.SP_PB_Base_Dinamica_90', 'parameters': []}, 0],
                  trigger_rule='all_success')

dag_parent[-1] >> op

op = new_operator(task_id='SP_PB_Inativacao_FOPA_Diaria',
                  op_args=['Processamento', {'procedure': 'dbo.SP_PB_Inativacao_FOPA_Diaria', 'parameters': []}, 0],
                  trigger_rule='all_success')

dag_parent[-1] >> op

op = new_operator(task_id='SP_PB_Saldo_Medio_Mes_Atual',
                  op_args=['Processamento', {'procedure': 'dbo.SP_PB_Saldo_Medio_Mes_Atual', 'parameters': []}, 0],
                  trigger_rule='all_success')

dag_parent[-1] >> op

op = new_operator(task_id='SP_PB_Volumes_Diarios',
                  op_args=['Processamento', {'procedure': 'dbo.SP_PB_Volumes_Diarios', 'parameters': []}, 0],
                  trigger_rule='all_success')

dag_parent[-1] >> op

op = new_operator(task_id='SP_PB_Resumo_Lancamento_Diario',
                  op_args=['Processamento', {'procedure': 'dbo.SP_PB_Resumo_Lancamento_Diario', 'parameters': []}, 0],
                  trigger_rule='all_success')

dag_parent[-1] >> op

subdag = dag_parent.pop()  # subdag: dashboards_paralelo

subdag_op = new_subdag_operator(task_id='dashboards_paralelo', trigger_rule='all_done', child_dag=subdag,
                                parent_dag=dag_parent[-1])

last_ops.pop() >> subdag_op

op = new_operator(task_id='SP_PB_Resumo_Saldo_Medio',
                  op_args=['Processamento', {'procedure': 'dbo.SP_PB_Resumo_Saldo_Medio', 'parameters': []}, 0],
                  trigger_rule='all_done')

subdag_op >> op

last_ops.append(op)

op = new_operator(task_id='SP_PB_Resumo_Lancamento_Diario_NDK_Ativacao', op_args=['Processamento', {
    'procedure': 'dbo.SP_PB_Resumo_Lancamento_Diario_NDK_Ativacao', 'parameters': []}, 0], trigger_rule='all_done')

last_ops.pop() >> op

subdag = dag_parent.pop()  # subdag: carga_extendida

subdag_op = new_subdag_operator(task_id='carga_extendida', trigger_rule='all_success', child_dag=subdag,
                                parent_dag=dag_parent[-1])

subdag_processamento >> subdag_op

op, subdag, subdag_op = None, None, None