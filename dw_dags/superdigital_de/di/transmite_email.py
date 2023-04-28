import json
import logging
import os
from concurrent.futures import as_completed
from contextlib import closing
from datetime import date, datetime

import pandas as pd
import urllib3
from tribool import Tribool

from allin.api import API, UploadArquivoBuilder, BuscaListasBuilder, StatusUploadListaBuilder
from superdigital_de.di.repositorios import ExecutionLog, Campanha, OutputSegmentadorEmail, Canal, \
    CampanhaContatoEmail, ParametroOperacional
from superdigital_mdc.dw import database, utils, airflow_utils
from superdigital_mdc.dw.airflow_utils import TaskException, api__as_is


def obter_idDefinition_idExecution_4_transmissao(**kwargs):
    with closing(database.db_connect(**kwargs)) as conn:
        bo = BusinessObject(api=None, conn=conn)

        execution_logs = bo.obter_id_definition_executadas_hoje()

        whitelist = bo.obter_envio_email_whitelist()
        _is_whitelist_exists = len(whitelist) > 0

        _ret = []
        if _is_whitelist_exists:
            _ret = [_ for _ in execution_logs if whitelist.count(str(_[ExecutionLog.ID_DEFINITION])) > 0]

        else:
            _ret = execution_logs

        logging.info('EMAIL\n\tLista de execucoes [%s] '
                     '\n\tWhitelist [%s] '
                     '\n\tLista Final [%s]' % (execution_logs, whitelist, _ret))

        return _ret


@api__as_is
def transmitir_email_by_idDefinitionIdExecution(id_definition, id_execution, **context):
    """
        Com a dupla `id_definition e id_execution`, atualiza a respectiva lista na AlliN com a segmentacao gerada
        :param id_definition Chave da Campanha
        :param id_execution: Execucao da Campanha
        :param context:
        :return: Dicionario contendo informacoes sobre o envio conforme formato:
            {'id_definition': int,
             'cd_lista': int,
             'id_execution': int,
             'id_log': int,
             'nm_lista': str,
             'file_name': str,
             'contagem': int,
             'status': str,
             'is_ok': bool}
        """

    logging.info('[transmitir_email_by_id_definition] '
                 'id_definition [%s] id_execution [%s]' % (id_definition, id_execution))

    max_tentativas = context.get(utils.Constantes.KEY__MAX_TENTATIVAS_ENVIO_LISTA,
                                 utils.Constantes.DEF__MAX_TENTATIVAS_ENVIO_LISTA)

    _ret = []

    with closing(database.db_connect(**context)) as conn:
        api = airflow_utils.get_api_from_context(**context)
        bo = BusinessObject(api=api, conn=conn)

        try:

            _campanha = bo.obter_campanha(id_definition)

            log_envio = None
            for nm_lista in bo.obter_listas_a_atualizar(_campanha):
                logging.info('\t Iniciando para lista [%s]' % nm_lista)
                cd_lista = bo.buscar_cd_lista(nm_lista)

                if not cd_lista:
                    log_envio = {'id_definition': id_definition,
                                 'cd_lista': None,
                                 'id_execution': -1,
                                 'id_log': None,
                                 'nm_lista': None,
                                 'contagem': 0,
                                 'status': 'LISTA_INEXISTENTE',
                                 'is_ok': False}

                    # TODO: criar a lista na allin

                else:
                    basic_infos = {'id_definition': id_definition,
                                   'cd_lista': cd_lista,
                                   'nm_lista': nm_lista,
                                   'id_execution': id_execution}

                    logging.info("\tPreparando envio de Lista info[%s]" % (basic_infos))

                    id_log = None

                    try:
                        tribool_segmenta_ip = bo.obter_ind_segmenta_ip_by_nm_lista(nm_lista)

                        df = bo.buscar_dados_transmissao_as_df(id_definition, id_execution, tribool_segmenta_ip)
                        if df.empty:
                            logging.info('\t\tDataframe vazio para info[%s]' % basic_infos)
                            log_envio = {'id_definition': int(id_definition),
                                         'cd_lista': int(cd_lista),
                                         'id_execution': int(id_execution),
                                         'id_log': None,
                                         'nm_lista': nm_lista,
                                         'contagem': 0,
                                         'status': 'SEGMENTACAO_VAZIA',
                                         'is_ok': True}

                        else:
                            logging.info('\t\tDataframe sera transformado em arquivo! info[%s]' % basic_infos)
                            _header = df.iloc[0].HeaderCampanha.split(BusinessObject.FIELD_SEPARATOR)
                            _file_path = '%s%s/' % (utils.Constantes.DEF__FILESYSTEM_FOLDER, str(date.today()))

                            try:
                                os.mkdir(_file_path)
                            except FileExistsError:
                                pass  # ignore

                            _file_name = '%s_%s.csv' % (nm_lista, id_execution)
                            _file_full_path = '%s%s' % (_file_path, _file_name)

                            df_4_lista = df.drop(columns=[OutputSegmentadorEmail.EMAIL,
                                                          OutputSegmentadorEmail.CONTA_CORRENTE_ID])
                            bo.criar_arquivo_lista(df_4_lista, _file_full_path, _header)

                            qtd_tentativas = 0
                            msg_error = None
                            while qtd_tentativas < max_tentativas:
                                logging.info('\t\tTentativa #%s' % qtd_tentativas)
                                try:
                                    id_log = bo.executar_upload_lista(cd_lista,
                                                                      nm_lista,
                                                                      _header,
                                                                      _file_name,
                                                                      _file_path)
                                    break

                                except Exception as e:
                                    msg_error = e

                                finally:
                                    qtd_tentativas += 1  # em caso de sucesso, marca uma tentativa

                            _is_upload_ok = id_log is not None
                            _status = 'ENVIADO' if _is_upload_ok else 'NAO_ENVIADO[%s]' % msg_error
                            logging.info('\tEnvio da lista ok? [%s] id_log[%s] status[%s]' %
                                         (_is_upload_ok, id_log, _status))

                            _is_ok = True
                            if _is_upload_ok:
                                bo.atualizar_status_lista(id_definition, id_execution, tribool_segmenta_ip, msg_error)

                                logging.info("\t\tPersistindo informacoes de Contato")
                                dt_envio = datetime.now()
                                for _ in df.iterrows():
                                    try:
                                        bo.persistir_email_contato(_[1], id_definition, id_execution, dt_envio)
                                    except Exception as e:
                                        _is_ok = False
                                        _status = '%s|ERRO persistir_email_contato Causa[%s]' % (_status, e)
                                        logging.error('\t\tFALHA persistir_contato! Contato[%s] Causa[%s]' % (_[1], e))

                                database.db_commit(conn=conn)

                            else:
                                _is_ok = False
                                if msg_error:
                                    logging.error('\t\tFalha ao enviar arquivo. Causa[%s] \n\tStackTrace [%s]' % (
                                        msg_error,
                                        utils.stack_trace(msg_error)))

                            bo.realizar_expurgo_arquivos()

                            log_envio = {'id_definition': id_definition,
                                         'cd_lista': int(cd_lista),
                                         'id_execution': int(id_execution),
                                         'id_log': id_log if id_log else None,
                                         'nm_lista': nm_lista,
                                         'file_name': _file_name,
                                         'contagem': int(df.size / len(df.columns)),
                                         'status': _status,
                                         'is_ok': _is_ok}

                    except Exception as e:
                        logging.error('FALHA INEXPERADA! Causa[%s]' % str(e))
                        log_envio = {'id_definition': id_definition,
                                     'cd_lista': int(cd_lista),
                                     'id_execution': int(id_execution),
                                     'id_log': id_log,
                                     'nm_lista': nm_lista,
                                     'file_name': None,
                                     'contagem': 0,
                                     'status': 'ERRO [%s] \n\tStackTrace[%s]' % (e, utils.stack_trace(e)),
                                     'is_ok': False}

                _ret.append(log_envio)

        except Exception as e:
            _msg_erro = 'ERRO INESPERADO para Campanha [%s] Causa[%s] \n\tStackTrace[%s]' % (id_definition,
                                                                                             e,
                                                                                             utils.stack_trace(e))

            log_envio = {'id_definition': id_definition,
                         'cd_lista': None,
                         'id_execution': None,
                         'id_log': None,
                         'nm_lista': None,
                         'contagem': 0,
                         'status': _msg_erro,
                         'is_ok': False}

            _ret.append(log_envio)

    logging.info("Envio de lista para [%s] FINALIZADO! Listas atualizadas [%s]" % (id_definition, len(_ret)))

    return _ret


@utils.time_this
def resumo_transmissao_lista(operator_names, **context):
    try:
        all_logs = airflow_utils.get_operators_return_value(operator_names, **context)
        log_exec = [__ if isinstance(__, dict) else _ for _ in all_logs for __ in _]

        print('\n\n# LOG COMPLETO DE EXECUCAO **************\n%s\n**************\n' % log_exec)

        if not log_exec:
            print('NAO HOUVE EXECUCOES!')
            return True

        df_log = pd.DataFrame([*log_exec]).sort_values(by=['id_definition'])
        df_env = df_log[df_log.is_ok == True]
        df_nenv = df_log[df_log.is_ok == False]

        _msg = '%s,' * 7
        dt_ref = date.today()
        print('\n\n# CAMPANHAS LISTA ENVIADA **************\n')
        print(_msg % ('Dt_Ref', 'IdDefinition', 'Nm_Lista', 'Status', 'IdExecution', 'id_log', 'contagem'))
        print('\n'.join(
                [_msg % (
                    dt_ref, int(ok.id_definition), ok.nm_lista, ok.status, int(ok.id_execution), ok.id_log,
                    int(ok.contagem))
                 for _idx, ok in df_env.iterrows()]))
        print('\n\n# CAMPANHAS LISTA NAO ENVIADA **********\n')
        print(_msg % ('Dt_Ref', 'IdDefinition', 'Nm_Lista', 'Status', 'IdExecution', 'id_log', 'contagem'))
        print('\n'.join([_msg % (dt_ref, nok.id_definition, nok.nm_lista, nok.status, nok.id_execution, 0, nok.contagem)
                         for _idx, nok in df_nenv.iterrows()]))
        print('\n# ***********************************\n\n')

    except Exception as e:
        print('Falha inesperada! [%s]' % str(e))

    return json.dumps([*log_exec])

    # _json = json.dumps([*log_exec])
    #
    # if df_nenv.empty:
    #     return _json
    # else:
    #     raise TaskException('Envio de listas contem erros!')


@api__as_is
def check_status_upload(operator_names, **context):
    from allin.core import InterruptableRequestRetryError

    all_logs = airflow_utils.get_operators_return_value(operator_names, **context)
    log_exec = [__ if isinstance(__, dict) else _ for _ in all_logs if _ for __ in _ if __]

    if not log_exec:
        print('NAO HOUVE EXECUCOES!')
        return True

    df_log = pd.DataFrame([*log_exec]).sort_values(by=['id_definition'])
    df_env = df_log[df_log.is_ok == True]
    df_nenv = df_log[df_log.is_ok == False]

    _msg = '%s,' * 5
    print('\n\n# CHECK UPLOAD **************\n')
    print(_msg % ('IdDefinition', 'IdExecution', 'Nm_Lista', 'id_log', 'contagem'))
    print('\n'.join([_msg % (int(ok.id_definition), int(ok.id_execution), ok.nm_lista, ok.id_log, ok.contagem)
                     for _idx, ok in df_env.iterrows()]))

    api = airflow_utils.get_api_from_context(**context)

    builders = [StatusUploadListaBuilder(ok.id_log, 4200, 60) for idx, ok in df_env.iterrows()]
    futures = api.batch_check_status_upload_file(builders)

    _ret = []
    for future in as_completed(futures):
        try:
            resultado = future.result()
            _ret.append(resultado)
            print('SUCESSO ----- [%s]' % resultado)
        except InterruptableRequestRetryError as e:
            print('FALHA ------- [%s]' % str(e))
        except TimeoutError as e:
            print('TIMEOUT ----- [%s]' % str(e))
        except Exception as e:
            print('INESPERADO -- [%s]' % str(e))

    # check = {'%s|%s|%s' % (ok.id_definition, ok.id_execution, ok.id_log):
    #              {'sucesso': None, 'status': None}
    #          for idx, ok in df_env.iterrows()}

    if df_nenv.empty:
        return _ret
    else:
        raise TaskException('Envio de listas contem erros!')


class BusinessObject(object):
    """
    Classe que encapsula as regras de negocio referente a transmissao de listas por canal de email para a AlliN.
    """

    FIELD_SEPARATOR = ','

    IS_ALLIN_LISTA = True

    def __init__(self, api, conn):
        self.api = api
        self.conn = conn

        logging.basicConfig(level=logging.INFO)  # ERROR #INFO #DEBUG
        urllib3.disable_warnings()

    def obter_id_definition_executadas_hoje(self):
        return ExecutionLog.findAll_minimum_by_Canal_DataExecucaoIsToday_SucessoIsTrue_IdExecutionNotNegativo(
                Canal.EMAIL, conn=self.conn)

    def buscar_cd_lista(self, nm_lista):
        logging.info('[buscar_cd_lista] %s' % nm_lista)

        _b = BuscaListasBuilder().set_nm_lista(nm_lista)
        for _ in self.api.get_lists(_b):
            return _[BuscaListasBuilder.RETORNO_ID_LISTA]

        else:
            return None

    def buscar_dados_transmissao_as_df(self, id_definition, id_execution, tribool_segmenta_ip):
        logging.info('[buscar_dados_transmissao_as_df] '
                     '[%s][%s][%s]' % (id_definition, id_execution, tribool_segmenta_ip))

        _rs = OutputSegmentadorEmail. \
            find_minimum_by_IdDefinition_IdExecution_DataTransmissaoIsNull_GCIsFalse_TriboolSegmentaIp(
                id_definition=id_definition,
                id_execution=id_execution,
                tribool_segmenta_ip=tribool_segmenta_ip,
                conn=self.conn)

        has_dados = len(_rs) > 0
        if not has_dados:
            logging.info('[buscar_dados_transmissao_as_df] Nao ha dados para envio, verificando se houve envio no dia')
            count = OutputSegmentadorEmail. \
                count_by_IdDefinition_IdExecution_DataTransmissaoIsToday_GCIsFalse_TriboolSegmentaIp(id_definition,
                                                                                                     id_execution,
                                                                                                     tribool_segmenta_ip,
                                                                                                     conn=self.conn)

            has_envio_no_dia = count > 1
            if has_envio_no_dia:
                logging.info('[buscar_dados_transmissao_as_df] Envio ja realizado hoje!')

            else:
                logging.info(
                        '[buscar_dados_transmissao_as_df] Nao houve envio/Segmentacao nula e nao ha dados para enviar')

                campanha = Campanha.find_by_id(id_definition, conn=self.conn)
                _blank = {OutputSegmentadorEmail.EMAIL: '',
                          OutputSegmentadorEmail.CONTA_CORRENTE_ID: '',
                          OutputSegmentadorEmail.HEADER_CAMPANHA: campanha[Campanha.TX_CAMPOS],
                          OutputSegmentadorEmail.VALUES_CAMPANHA:
                              self.FIELD_SEPARATOR * campanha[Campanha.TX_CAMPOS].count(self.FIELD_SEPARATOR),
                          }
                _rs.append(_blank)

        df = pd.DataFrame(_rs)
        return df

    def criar_arquivo_lista(self, df, file_full_path, header):
        logging.info('[criar_arquivo_lista] Caminho completo [%s] header[%s]' % (file_full_path, header))

        df_split = pd.DataFrame(df.valuesCampanha.str.split(BusinessObject.FIELD_SEPARATOR).tolist(),
                                columns=header)

        df_split.to_csv(file_full_path,
                        index=False,
                        doublequote=False,
                        columns=header,
                        header=header,
                        encoding='latin1')

        logging.info('\t\tArquivo [%s] criado!' % file_full_path)

    def executar_upload_lista(self, cd_lista, nm_lista, header, file_name, file_path):
        logging.info(
                '[executar_upload_lista] Enviando arquivo... (%s, %s, %s, %s)' % (
                    cd_lista, nm_lista, header, file_name))
        assert isinstance(self.api, API)

        _header = header
        if BusinessObject.IS_ALLIN_LISTA:
            _header = self.montar_header_allin(header)

        b_upload = UploadArquivoBuilder() \
            .set_nm_lista(nm_lista) \
            .set_campos_arquivo(BusinessObject.FIELD_SEPARATOR.join(_header)) \
            .set_separador(UploadArquivoBuilder.SEPARADOR_COMMA) \
            .set_excluidos(UploadArquivoBuilder.EXCLUIDOS_SIM) \
            .set_acao_arquivo(UploadArquivoBuilder.ACAO_ARQUIVO_DELETE_REESCREVER) \
            .set_qualificador(UploadArquivoBuilder.QUALIFICADOR_NENHUM) \
            .set_file_path(file_path) \
            .set_file_name(file_name)

        _json = self.api.upload_file(b_upload)
        _id_log = _json["output"]["id_log"]

        logging.info('\t\tArquivo enviado com sucesso! id_log[%s]' % _id_log)
        return _id_log

    def atualizar_status_lista(self, id_definition, id_execution, tribool_segmenta_ip, msg_error):
        logging.info(
                '[atualizar_status_lista] IdDefinition[%s] IdExecution[%s] SegmentaIp [%s]' % (
                    id_definition, id_execution, tribool_segmenta_ip))

        OutputSegmentadorEmail.updateAll_DataTransmissao_by_IdDefinition_IdExecution_TriboolSegmentaIp(
                id_definition=id_definition,
                id_execution=id_execution,
                tribool_segmenta_ip=tribool_segmenta_ip,
                conn=self.conn)

    def persistir_email_contato(self, contato, id_definition, id_execution, dt_envio):
        if contato[CampanhaContatoEmail.EMAIL] != '':
            CampanhaContatoEmail.insert(contato[CampanhaContatoEmail.EMAIL],
                                        contato[CampanhaContatoEmail.CONTA_CORRENTE_ID],
                                        id_definition,
                                        id_execution,
                                        dt_envio,
                                        conn=self.conn)
        return True

    def realizar_expurgo_arquivos(self):
        # TODO: EVOLUCAO realizar delete dos arquivos antigos
        # REFERENCIA: atualiza_lista.apagar_arquivo(_file_name)
        return True

    def obter_campanha(self, id_definition):
        return Campanha.find_by_id(id_definition, conn=self.conn)

    def obter_listas_a_atualizar(self, campanha):
        _is_segmenta_ip = utils.from_int_2_bool(campanha[Campanha.IND_SEGMENTA_IP])

        _nm_lista_base = '%s_CM_%s' % (campanha[Campanha.NAME],
                                       campanha[Campanha.ID_DEFINITION])

        _ret = []
        if _is_segmenta_ip:
            _ret.append('%s_IP_0' % _nm_lista_base)
            _ret.append('%s_IP_1' % _nm_lista_base)

        else:
            _ret.append(_nm_lista_base)

        return _ret

    def obter_ind_segmenta_ip_by_nm_lista(self, nm_lista):
        _ip_index = nm_lista.find('_IP_')
        if _ip_index > 0:
            return Tribool(utils.from_int_2_bool(nm_lista[_ip_index + 4:_ip_index + 5]))

        return Tribool(None)

    def obter_envio_email_whitelist(self):
        _ret = ParametroOperacional.find_Valor_by_id_as_list(ParametroOperacional.P__DAG_ENVIOEMAIL_CAMPANHA_WHITELIST,
                                                             conn=self.conn)
        return _ret

    def montar_header_allin(self, header):
        de_para = {'email': 'nm_email',
                   'Nome': 'nome_completo',
                   'PrimeiroNome': 'nome'}

        _header = [de_para[_] if de_para.get(_) else _ for _ in header]
        return _header
