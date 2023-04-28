# import logging
from superdigital_mdc.dw import utils
from superdigital_mdc.dw.etl.cleansing import executar_proc_cleansing
from superdigital_mdc.dw.etl.ingestao import executar_ingestao, resumo_extracao
from superdigital_mdc.dw.etl.processamento import executar_proc_processamento

from superdigital_mdc.dw import airflow_utils as airflow_utils

@utils.time_this
def operator_factory(type, parameters, active, **kwargs):
    if not active:
        return run_inactive()

    if type == "Cleansing":
        return run_cleansing(parameters, **kwargs)

    elif type == "Processamento":
        return run_processamento(parameters, **kwargs)

    elif type == "Ingestao":
        return run_ingestao(parameters, **kwargs)

    elif type == "Ingestao_Resumo":
        return run_ingestao_resumo(**kwargs)

    elif type == "Error":
        return run_error()

    print("Nenhuma execucao definida no factory")
    return -1

# Inactive operators
def run_inactive(**context):
    print("Operador inativo")

    return 0

# Raise errors for testing
def run_error():
    raise Exception('Generated Error')
    return -1


# Chamadas tem um padrao (parameters,**kwargs) ,mas cada funcao trata internamente os execs de funcoes e tratamento/passagem de parametros
def run_cleansing(parameters, **kwargs):
    procedure = parameters["procedure"]
    paramns = parameters["parameters"]

    return executar_proc_cleansing(procedure, paramns, **kwargs)


def run_processamento(parameters, **kwargs):
    procedure = parameters["procedure"]
    paramns = parameters["parameters"]

    return executar_proc_processamento(procedure, paramns, **kwargs)


def run_ingestao(parameters, **kwargs):
    extracao = parameters["extracao"]

    return executar_ingestao(extracao, **kwargs)


def run_ingestao_resumo(**context):
    print("Resumo Ingestao")
    if 'dag' in context:
        dag = context['dag']
        task = context['ti']

        stream_tasks = filter(lambda id: id != task.task_id , dag.task_ids)

        return resumo_extracao(stream_tasks,**context)


# TODO
# preencher metodo run_inactive corretamente
# padronizar logs
# Classes do factory utilizando estruturas padronizadas quando possivel
# tratamentos de erro (parametros faltando ou fora do formato esperado e etc)
