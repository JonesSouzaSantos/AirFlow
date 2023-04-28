import json
import logging
from contextlib import closing
from datetime import datetime, date

import pandas as pd

from allin.api_v2 import BatchPushBuilder
from superdigital_de.di.repositorios import ExecutionLog, Canal, ParametroOperacional, Campanha, OutputSegmentadorPush, \
    DispositivoPlataforma, CampanhaContatoPush
from superdigital_mdc.dw import database, airflow_utils, utils, rabbit_utils
from superdigital_mdc.dw.airflow_utils import TaskException


def obter_execucoes_a_transmitir(**kwargs):
    with closing(database.db_connect(**kwargs)) as conn:
        bo = BusinessObject(api_v2=None, conn=conn)

        execution_logs = bo.obter_ExecutionLog_executadas_hoje()

        whitelist = bo.obter_envio_push_whitelist()
        _is_whitelist_exists = len(whitelist) > 0

        _ret = []
        if _is_whitelist_exists:
            _ret = [_ for _ in execution_logs if whitelist.count(str(_[ExecutionLog.ID_DEFINITION])) > 0]

        else:
            _ret = execution_logs

        logging.info('PUSH\n\tLista de execucoes [%s] '
                     '\n\tWhitelist [%s] '
                     '\n\tLista Final [%s]' % (execution_logs, whitelist, _ret))

        return _ret


@airflow_utils.apiv2__as_is
def transmitir_push_by_idDefinitionIdExecution(id_definition, id_execution, **context):
    logging.info('[transmitir_push_by_idDefinitionIdExecution] '
                 'id_definition [%s] id_execution [%s]' % (id_definition, id_execution))

    with closing(database.db_connect(**context)) as conn:
        api = airflow_utils.get_api_from_context(**context)
        bo = BusinessObject(api_v2=api, conn=conn)

        _campanha = bo.obter_campanha(id_definition)

        _ret = []
        for plataforma in DispositivoPlataforma:
            logging.info('[transmitir_push_by_idDefinitionIdExecution] '
                         'IdDefinition[%s] Dispositivo[%s]' % (id_definition, plataforma))

            _log = {'IdExecution': id_execution, 'campanha': _campanha, 'total': 0, 'exception': None}
            try:
                _log['plataforma'] = plataforma.name

                all_push = bo.disparar_push(_campanha, id_execution, plataforma)
                _log['total'] = len(all_push)

                bo.atualizar_dataTransmissao(id_definition, id_execution, plataforma)

                bo.persistir_all_contato_push(all_push)

                database.db_commit(conn=conn)

            except Exception as e:
                logging.error(e, e)
                _log['exception'] = 'Causa[%s]\nStacktrace\n%s' % (e, utils.stack_trace(e))

            finally:
                _ret.append(_log)

        return _ret


@utils.time_this
def resumo_transmitir_push(operator_names, **context):
    dt_ref = date.today()

    all_logs = airflow_utils.get_operators_return_value(operator_names, **context)
    log_exec = [__ if isinstance(__, dict) else _ for _ in all_logs for __ in _]
    print('\n\n# LOG COMPLETO DE EXECUCAO **************\n%s\n**************\n' % log_exec)

    if not log_exec:
        print('NAO HOUVE EXECUCOES!')
        return True

    df_log = pd.DataFrame([*log_exec])  # .sort_values(by=['IdDefinition'])
    df_ok = df_log[df_log.exception.isnull()] if 'exception' in df_log.columns else df_log
    df_nok = df_log[df_log.exception.notnull()] if 'exception' in df_log.columns else None

    _msg = '%s,'
    print('\n\n# TRANSMISSAO OK **************')
    if df_ok is not None:
        print(_msg * 5 % ('IdDefinition', 'IdExecution', 'Campanha', 'Plataforma', 'Quantidade'))
        print('\n'.join([_msg * 5 % (item['campanha']['IdDefinition'],
                                     item['IdExecution'],
                                     item['campanha']['Name'],
                                     item['plataforma'],
                                     item['total'])
                         for _idx, item in df_ok.iterrows()]))
    print('\n\n# TRANSMISSAO NOK **************')
    if df_nok is not None:
        print(_msg * 5 % ('IdDefinition', 'IdExecution', 'Campanha', 'Plataforma', 'ERRO'))
        print('\n'.join([_msg * 5 % (item['campanha']['IdDefinition'],
                                     item['IdExecution'],
                                     item['campanha']['Name'],
                                     item['plataforma'],
                                     item['exception'])
                         for _idx, item in df_nok.iterrows()]))
    print('\n\n# ******************************\n')

    _json = json.dumps([*log_exec])

    if df_nok.empty:
        return _json
    else:
        raise TaskException('Envio de Push contem erros!')


class BusinessObject(object):
    PARTIAL_COMMIT_INDEX = 500

    def __init__(self, api_v2, conn):
        self.api_v2 = api_v2
        self.conn = conn

    def obter_ExecutionLog_executadas_hoje(self):
        return ExecutionLog.findAll_minimum_by_Canal_DataExecucaoIsToday_SucessoIsTrue_IdExecutionNotNegativo(
                Canal.PUSH, conn=self.conn)

    def obter_envio_push_whitelist(self):
        _ret = ParametroOperacional.find_Valor_by_id_as_list(ParametroOperacional.P__DAG_ENVIOPUSH_CAMPANHA_WHITELIST,
                                                             conn=self.conn)
        return _ret

    def obter_campanha(self, id_definition):
        return Campanha.find_by_id(id_definition, conn=self.conn)

    def disparar_push(self, campanha, id_execution, dispositivo_plataforma):
        logging.info('[disparar_push] '
                     'id_execution [%s] '
                     'dispositivo_plataforma[%s]'
                     'Campanha [%s] ' % (id_execution, dispositivo_plataforma, campanha))

        id_definition = campanha[Campanha.ID_DEFINITION]

        b = BatchPushBuilder() \
            .set_def_nm_envio('[%s_%s]%s' % (id_definition, id_execution, campanha[Campanha.NAME])) \
            .set_def_url_scheme('superdigital://data'
                                '?pushContent=%s'
                                '&pushCampaign=%s'
                                '&pushSegment=%s'
                                '&pushScheme=%s'
                                '&pushHost=path' % (id_definition,
                                                    campanha[Campanha.NAME],
                                                    campanha[Campanha.PUBLICO],
                                                    campanha[Campanha.ID_SUBCATEGORIA])) \
            .set_def_nm_titulo('{%s}' % BatchPushBuilder.BIND_TITULO) \
            .set_def_nm_mensagem('{%s}' % BatchPushBuilder.BIND_MENSAGEM) \
            .set_def_html_id('1')

        _list_push = []
        output_push = self.obter_output_push(id_definition, id_execution, dispositivo_plataforma)
        logging.info('[disparar_push] Dispositivo [%s] Total [%s] ' % (dispositivo_plataforma, len(output_push)))
        for push in output_push:
            _list_push.append(push)
            _dipositivo = DispositivoPlataforma.by_value(push[OutputSegmentadorPush.DEVICE_PLATAFORMA]).value.lower()

            b.add_binds({BatchPushBuilder.BIND_PUSH_ID: push[OutputSegmentadorPush.CONTA_CORRENTE_ID],
                         BatchPushBuilder.BIND_ID_DEFINITION: campanha[Campanha.ID_DEFINITION],
                         BatchPushBuilder.BIND_ID_EXECUTION: id_execution,
                         BatchPushBuilder.BIND_PLATAFORMA: _dipositivo,
                         BatchPushBuilder.BIND_TITULO: push[OutputSegmentadorPush.ASSUNTO] or 'Superdigital',
                         BatchPushBuilder.BIND_MENSAGEM: push[OutputSegmentadorPush.MENSAGEM]})

        try:

            logging.info('[disparar_push] Envio Rabbit')
            _ret_rabbit = rabbit_utils.batch_send_push(b)
            logging.info('[disparar_push] LOG Rabbit [%s]' % _ret_rabbit)

        except Exception as e:
            logging.error('[disparar_push] Envio Rabbit com Falha \n', e)

        return _list_push

    def obter_output_push(self, id_definition, id_execution, device_plataforma):
        return OutputSegmentadorPush. \
            findAll_by_IdDefinition_IdExecution_DataTransmissaoIsNull_notGC_DevicePlataforma(id_definition,
                                                                                             id_execution,
                                                                                             device_plataforma,
                                                                                             conn=self.conn)

    def atualizar_dataTransmissao(self, id_definition, id_execution, device_plataforma):
        logging.info('[atualizar_dataTransmissao] '
                     'id_definition [%s] id_execution [%s] device_plataforma[%s]' % (id_definition,
                                                                                     id_execution,
                                                                                     device_plataforma))

        OutputSegmentadorPush. \
            updateAll_DataTransmissao_by_IdDefinition_IdExecution_DevicePlataforma(id_definition,
                                                                                   id_execution,
                                                                                   device_plataforma,
                                                                                   datetime.now(),
                                                                                   conn=self.conn)

    def persistir_all_contato_push(self, all_push):
        logging.info("\t[persistir_all_contato_push] Persistindo informacoes de Contato [%s] registros" % len(all_push))

        data_envio = datetime.now()
        for idx, push in enumerate(all_push):
            logging.info('\tcontato %s/%s' % (idx + 1, len(all_push)))

            campanha_contato_push = {
                CampanhaContatoPush.ID_DEFINITION: push[OutputSegmentadorPush.ID_DEFINITION],
                CampanhaContatoPush.ID_EXECUTION: push[OutputSegmentadorPush.ID_EXECUTION],
                CampanhaContatoPush.DEVICE_ID: push[OutputSegmentadorPush.DEVICE_ID],
                CampanhaContatoPush.DEVICE_PLATAFORMA: push[OutputSegmentadorPush.DEVICE_PLATAFORMA],
                CampanhaContatoPush.CONTA_CORRENTE_ID: push[OutputSegmentadorPush.CONTA_CORRENTE_ID],
                CampanhaContatoPush.DATA_ENVIO: data_envio
            }

            CampanhaContatoPush.insert(campanha_contato_push, conn=self.conn)

            is_time_for_partial_commit = idx % self.PARTIAL_COMMIT_INDEX == 0 and idx > 0
            if is_time_for_partial_commit:
                logging.info('<<commit parcial>>')
                database.db_commit(self.conn)
