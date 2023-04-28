import logging
from datetime import datetime

from airflow import DAG
from airflow.executors.local_executor import LocalExecutor
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from allin.core import FallbackStrategy
from superdigital_mdc.dw import utils as utils


# *****************************
# AUXILIARY
# *****************************

class TaskException(Exception):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        self.message = args[0]


def get_task_execution(**context):
    _task_instance = context['ti']
    assert isinstance(_task_instance, TaskInstance)
    return _task_instance


def get_operators_return_value(operator_names, **context):
    _task_instance = get_task_execution(**context)
    _ret = _task_instance.xcom_pull(task_ids=operator_names)
    return _ret


def _get_xcom_token(**context):
    _task_instance = get_task_execution(**context)

    _token = _task_instance.xcom_pull(key=utils.Constantes.KEY__ALLIN_API_TOKEN)

    if _token:
        logging.info('Token AlliN RESGATADO [%s...%s]' % (_token[0:4], _token[-4:]))

    return _token


def _set_xcom_token(token, **context):
    _task_instance = get_task_execution(**context)

    _task_instance.xcom_push(key=utils.Constantes.KEY__ALLIN_API_TOKEN, value=token)

    logging.info('Token AlliN ATUALIZADO [%s...%s]' % (token[0:4], token[-4:]))

    return token


def get_api_from_context(**context):
    return context[utils.Constantes.KEY__ALLIN_API_RUNTIME_INSTANCE]


# *****************************
# DECORATORS
# *****************************

@utils.time_this
def api__as_is(funk, *args, **kwargs):
    """
    Decorator que constroi uma instancia da API AlliN e a deixa disponivel via
    `kwargs[utils.Constantes.KEY__ALLIN_API_RUNTIME_INSTANCE]`, utilizando a estrategia
    `FallbackStrategy.AutoReconnect()`
    :param fallback_strategy:
    :param kwargs:
    :return:
    """
    return _api__as_is(funk, 1, *args, **kwargs)


@utils.time_this
def apiv2__as_is(funk, *args, **kwargs):
    """
    Decorator que constroi uma instancia da API v2 AlliN e a deixa disponivel via
    `kwargs[utils.Constantes.KEY__ALLIN_API_RUNTIME_INSTANCE]`, utilizando a estrategia
    `FallbackStrategy.AutoReconnect()`
    :param fallback_strategy:
    :param kwargs:
    :return:
    """
    return _api__as_is(funk, 2, *args, **kwargs)


def _api__as_is(funk, version, *args, **kwargs):
    def as_is(*args, **context):
        logging.info("Injetando API V%s AlliN [api__as_is]..." % version)

        api = utils.MDCFactory.allin_api_by_version(version, fallback_strategy=FallbackStrategy.AutoReconnect())
        context.update({utils.Constantes.KEY__ALLIN_API_RUNTIME_INSTANCE: api})

        logging.info("API AlliN INJETADA! Funcao a executar [%s]" % funk)

        api.start_session(**{utils.Constantes.KEY__ALLIN_API_TOKEN: (_get_xcom_token(**context))})
        _ret = funk(*args, **context)
        api.end_session()

        _set_xcom_token(api.token, **context)

        return _ret

    return as_is


@utils.time_this
def api__list_retoken_retry(funk, *args, **kwargs):
    """
    Decorator que constroi uma instancia da API AlliN e a deixa disponivel via
    `kwargs[utils.Constantes.KEY__ALLIN_API_RUNTIME_INSTANCE]`, configurada com as estrategias
    `FallbackStrategy.WaitAndRetry(), FallbackStrategy.AutoReconnect()`
    :param fallback_strategy:
    :param kwargs:
    :return:
    """

    def list_retoken_retry(*args, **context):
        logging.info("Injetando API AlliN [api__list_retoken_retry]...")

        api = utils.MDCFactory.allin_api(fallback_strategy=FallbackStrategy.ListOf(
            strategies=[FallbackStrategy.WaitAndRetry(),
                        FallbackStrategy.AutoReconnect()]))
        context.update({utils.Constantes.KEY__ALLIN_API_RUNTIME_INSTANCE: api})

        logging.info("API AlliN INJETADA! Funcao a executar [%s]" % funk)

        api.start_session(**{utils.Constantes.KEY__ALLIN_API_TOKEN: (_get_xcom_token(**context))})
        _ret = funk(*args, **context)
        api.end_session()

        _set_xcom_token(api.token, **context)

        return _ret

    return list_retoken_retry

