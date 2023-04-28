import json
import logging
from contextlib import closing
from datetime import date, datetime

import pandas as pd

from allin.api_v2 import BatchSMSBuilder
from supercm_de.di.repositorios import ExecutionLog, ViewOutputSegmentadorSMS, Campanha, Canal, CampanhaContatoSMS, \
    ParametroOperacional, OutputSegmentadorSMS
from superdigital_mdc.dw import database, utils, airflow_utils
from superdigital_mdc.dw.airflow_utils import TaskException


def obter_idDefinitionIdExecution_4_transmissao(**kwargs):
    with closing(database.db_connect(**kwargs)) as conn:
        bo = BusinessObject(api=None, conn=conn)

        execution_logs = bo.obter_id_definition_executadas_hoje()

        whitelist = bo.obter_envio_sms_whitelist()
        _is_whitelist_exists = len(whitelist) > 0

        _ret = []
        if _is_whitelist_exists:
            _ret = [_ for _ in execution_logs if whitelist.count(str(_[ExecutionLog.ID_DEFINITION])) > 0]

        else:
            _ret = execution_logs

        logging.info('SMS\n\tLista de execucoes [%s] '
                     '\n\tWhitelist [%s] '
                     '\n\tLista Final [%s]' % (execution_logs, whitelist, _ret))

        return _ret


@airflow_utils.apiv2__as_is
def transmitir_sms_by_idDefinitionIdExecution(id_definition, id_execution, **context):
    logging.info('[transmitir_sms_by_idDefinitionIdExecution] '
                 'id_definition [%s] id_execution [%s]' % (id_definition, id_execution))

    with closing(database.db_connect(**context)) as conn:
        api = airflow_utils.get_api_from_context(**context)
        bo = BusinessObject(api=api, conn=conn)

        _campanha = bo.obter_campanha(id_definition)

        _ret = {'IdExecution': id_execution, 'log': None, 'exception': None, 'campanha': _campanha}
        try:
            _ret['log'], all_sms = bo.disparar_sms(_campanha, id_execution)

            logging.info("\t[insertAll_dadosTransmissao_by_IdDefinition_IdExecution] Persistindo informacoes de envio")
            bo.persistir_dadosEnvio(id_definition, id_execution)

            bo.atualizar_dataTransmissao(id_definition, id_execution)

            logging.info("\t[transmitir_sms_by_idDefinitionIdExecution] Persistindo informacoes de Contato")
            for _sms in all_sms:
                bo.persistir_sms_contato(_sms, id_execution)

            database.db_commit(conn=conn)

        except Exception as e:
            logging.error(e, e)
            _ret['exception'] = 'Causa[%s]\nStacktrace\n%s' % (e, utils.stack_trace(e))

        return _ret


@utils.time_this
def resumo_transmitir_sms(operator_names, **context):
    dt_ref = date.today()

    log_exec = airflow_utils.get_operators_return_value(operator_names, **context)
    print('\n\n# LOG COMPLETO DE EXECUCAO **************')
    print(log_exec)

    if not log_exec:
        print('NAO HOUVE EXECUCOES!')
        return True

    df_log = pd.DataFrame([*log_exec])  # .sort_values(by=['cd_lista'])
    df_ok = df_log[df_log.exception.isnull()]
    df_nok = df_log[df_log.exception.notnull()]

    _msg = '%s,'
    print('\n\n# TRANSMISSAO OK **************')
    print(_msg * 4 % ('IdDefinition', 'IdExecution', 'Campanha', 'LOG'))
    print('\n'.join([_msg * 4 % (
        item['campanha']['IdDefinition'], item['IdExecution'], item['campanha']['Name'], item['log'])
                     for _idx, item in df_ok.iterrows()]))
    print('\n\n# TRANSMISSAO NOK **************')
    print(_msg * 5 % ('IdDefinition', 'IdExecution', 'Campanha', 'LOG', 'ERRO'))
    print('\n'.join(
            [_msg * 5 % (item['campanha']['IdDefinition'], item['IdExecution'], item['campanha']['Name'], item['log'],
                         item['exception'])
             for _idx, item in df_nok.iterrows()]))
    print('\n\n# ******************************\n')

    _json = json.dumps([*log_exec])

    if df_nok.empty:
        return _json
    else:
        raise TaskException('Envio de SMS contem erros!')


class BusinessObject(object):
    def __init__(self, api, conn):
        self.api = api
        self.conn = conn

    def obter_id_definition_executadas_hoje(self):
        return ExecutionLog.findAll_minimum_by_Canal_DataExecucaoIsToday_SucessoIsTrue_IdExecutionNotNegativo(Canal.SMS, conn=self.conn)

    def obter_id_definition_executadas_maiorIgual_a_D(self, d):
        return ExecutionLog.findAll_minimum_By_Canal_DataExecucaoGT_SucessoIsTrue(Canal.SMS, d, conn=self.conn)

    def disparar_sms(self, campanha, id_execution):
        logging.info('[disparar_sms] Campanha [%s] id_execution [%s]' % (campanha, id_execution))

        id_definition = campanha[Campanha.ID_DEFINITION]

        b = BatchSMSBuilder() \
            .set_def_nm_envio('[%s_%s]%s' % (id_definition, id_execution, campanha[Campanha.NAME])) \
            .set_def_nm_mensagem('{%s}' % BatchSMSBuilder.BIND_MENSAGEM)

        _list_sms = []
        for _sms in self.obter_output_sms(id_definition, id_execution):
            _list_sms.append(_sms)
            b.add_binds({BatchSMSBuilder.BIND_CELULAR: _sms[ViewOutputSegmentadorSMS.TELEFONE],
                         BatchSMSBuilder.BIND_MENSAGEM: _sms[ViewOutputSegmentadorSMS.MENSAGEM]})

        _ret = self.api.batch_send_sms(b)

        return _ret, _list_sms

    def obter_campanha(self, id_definition):
        return Campanha.find_by_id(id_definition, conn=self.conn)

    def obter_output_sms(self, id_definition, id_execution):
        return ViewOutputSegmentadorSMS.findAll_by_IdDefinition_IdExecution_DataTransmissaoIsNull_notGC(id_definition,
                                                                                                        id_execution,
                                                                                                        conn=self.conn)

    def persistir_dadosEnvio(self, id_definition, id_execution):
        logging.info('[persistir_dadosEnvio] id_definition [%s] id_execution [%s]' % (id_definition, id_execution))

        OutputSegmentadorSMS.insertAll_dadosTransmissao_by_IdDefinition_IdExecution(id_definition, id_execution,
                                                                                    conn=self.conn)

    def atualizar_dataTransmissao(self, id_definition, id_execution):
        logging.info('[atualizar_dataTransmissao] id_definition [%s] id_execution [%s]' % (id_definition, id_execution))

        OutputSegmentadorSMS.updateAll_DataTransmissao_by_IdDefinition_IdExecution(id_definition,
                                                                                       id_execution,
                                                                                       datetime.now(),
                                                                                       conn=self.conn)

    def persistir_sms_contato(self, sms, id_execution):
        CampanhaContatoSMS.upsert_by_Telefone(sms[OutputSegmentadorSMS.TELEFONE],
                                              sms[OutputSegmentadorSMS.CONTA_CORRENTE_ID],
                                              sms[OutputSegmentadorSMS.ID_DEFINITION],
                                              id_execution,
                                              conn=self.conn)
        return True

    def obter_envio_sms_whitelist(self):
        _ret = ParametroOperacional.find_Valor_by_id_as_list(ParametroOperacional.P__DAG_ENVIOSMS_CAMPANHA_WHITELIST,
                                                             conn=self.conn)
        return _ret
