import logging
import traceback
from contextlib import closing

import pandas as pd
import urllib3

from allin.api import DadosEntregaPorDataBuilder, AnaliticoAberturaBuilder, ErrosPermanentesBuilder, \
    AnaliticoEntregaBuilder, AnaliticoClickBuilder, RelatorioEntregaBuilder
from superdigital_de.di.repositorios import ExecutionLog, CampanhaRetornoEmail, CampanhaContatoEmail, \
    ParametroOperacional
from superdigital_mdc.dw import database, airflow_utils
from superdigital_mdc.dw.utils import to_int, time_this


def get_dias_a_considerar(**kwargs):
    with closing(database.db_connect(**kwargs)) as conn:
        bo = BusinessObject(api=None, conn=conn)

        return bo.obter_dias_a_analisar_campanhas()


def get_dias_a_reprocessar(**kwargs):
    with closing(database.db_connect(**kwargs)) as conn:
        bo = BusinessObject(api=None, conn=conn)

        return bo.obter_dias_a_reprocessar()


@airflow_utils.api__list_retoken_retry
def analisa_retorno_campanhas(dt_ref, **context):
    """
        Com base da data de referencia informada, busca por Campanhas encerradas na data, extrai as informacoes via API
        AlliN e as popula nas tabelas de controle Yes Campaign
        :param dt_ref:
        :param context:
        :return: Array contendo dicionarios no seguinte formato:
            {
                'id_definition': int,
                'id_execution': int,
                'id_campanha': int,
                'nm_campanha': str,
                'dt_inicio': date,
                'dt_encerramento': date,
                'is_once': boolean,
                'total_envio': int,
                'total_entregues': int,
                'total_abertura': int,
                'total_clique': int,
                'runtime_error': str,
                'runtime_msg_error': str,
                'allin_envio': boolean,
                'once_envio_lista': boolean,
                'once_enviados':{'total': int, 'sucesso': int, 'falha': int, 'duplicado': int},
                'once_bounce': {'total': int, 'sucesso': int, 'falha': int, 'duplicado': int},
                'always_click': {'total': int, 'sucesso': int, 'falha': int, 'duplicado': int},
                'always_abertura': {'total': int, 'sucesso': int, 'falha': int, 'duplicado': int},
                'contato_clique': {'total': int, 'sucesso': int, 'falha': int},
                'contato_abertura': {'total': int, 'sucesso': int, 'falha': int},
            }

        """

    logging.info('\tAnalise do dia [%s]. Buscando campanhas...' % dt_ref)

    with closing(database.db_connect(**context)) as conn:

        api = airflow_utils.get_api_from_context(**context)
        bo = BusinessObject(api, conn)

        envios = bo.buscar_entregas_encerradas(dt_ref)

        _ident = '\t\t'
        if not envios:
            logging.info('%sNAO HA DADOS A PROCESSAR PARA [%s]' % (_ident, dt_ref))

        else:
            logging.info('%sProcessando [%s] listas para data[%s]' % (_ident, len(envios), dt_ref))

            for _ in envios:
                id_campanha_externo = _['itensConteudo_id_campanha']
                nm_campanha_externo = _['itensConteudo_nm_campanha']

                id_definition, id_execution = bo.buscar_execucao_from_campanha_encerrada(nm_campanha_externo, dt_ref)

                if not (id_definition and id_execution):
                    logging.info('%sNAO foi possivel determinar DEFINITION/EXECUTION para lista [%s] info[%s]' %
                                 (_ident, nm_campanha_externo, _))
                    continue

                basic_infos = {'id_definition': id_definition,
                               'id_execution': id_execution,
                               'allin_id_campanha': id_campanha_externo,
                               'allin_nm_campanha': nm_campanha_externo}

                logging.info('%sIniciando Lista info[%s]' % (_ident, basic_infos))
                try:
                    campanha_retorno_email = bo.iniciar_analise_retorno(id_definition, id_execution, _)
                    campanha_retorno_email = bo.persistir_campanha_retorno_email(campanha_retorno_email)

                    if campanha_retorno_email:
                        logging.info('%sLista PERSISTIDA COM SUCESSO [%s]' % (_ident, basic_infos))

                        bo.execute_once(campanha_retorno_email)
                        bo.execute_always(campanha_retorno_email)

                        database.db_commit(conn)

                    else:
                        logging.info('%sLista NAO FOI PERSISTIDA [%s]' % (_ident, basic_infos))

                except Exception as e:
                    _stacktrace = ''.join(traceback.format_tb(e.__traceback__))
                    logging.error('%sRollback sera chamado para Lista [%s] Cause[%s]'
                                  '\n\tStacktrace[%s]' % (_ident,
                                                          basic_infos,
                                                          e,
                                                          _stacktrace))
                    bo.notificar_erro(e)
                    database.db_rollback(conn)

    return bo.obter_log_execucao()


@time_this
def resumo_retorno_campanhas(operator_names, **context):
    log_exec = airflow_utils.get_operators_return_value(operator_names, **context)
    log_exploded = []
    for log in log_exec:
        for _ in log:
            log_exploded.append(_)

    print('\nLog Execucao:\n%s\n\n' % log_exploded)

    if not log_exploded:
        print('NAO HA DADOS A SUMARIZAR!')
        return log_exploded

    df_log = pd.DataFrame(log_exploded).sort_values(by=['id_campanha'])

    df_success = df_log[df_log.runtime_error.isnull()]
    df_fail = df_log[df_log.runtime_error.notnull()]

    _msg_ok = 'IdDefinition[%s] IdExecution[%s] Campanha [%s] data [%s] once[%s] \n%s ' \
              '\n\tEnviados [%s]\n\tBounce [%s]\n\t Click [%s]\n\tAbertura [%s]'

    _msg_nok = 'IdDefinition[%s] IdExecution[%s] Campanha [%s] data [%s] once[%s] \n%s ' \
               '\n\tEnviados [%s]\n\tBounce [%s]\n\t Click [%s]\n\tAbertura [%s]\nErro:%s'

    print('\n# SUCESSO AO PROCESSAR **************')
    print('\n\n'.join([_msg_ok % (ok.id_definition,
                                  ok.id_execution,
                                  ok.id_campanha,
                                  ok.dt_inicio,
                                  ok.is_once,
                                  ok.nm_campanha,
                                  ok.once_enviados,
                                  ok.once_bounce,
                                  ok.always_click,
                                  ok.always_abertura
                                  ) for _idx, ok in df_success.iterrows()]))
    print('\n# FALHA AO PROCESSAR ****************')
    print('\n\n'.join([_msg_nok % (nok.id_definition,
                                   nok.id_execution,
                                   nok.id_campanha,
                                   nok.dt_inicio,
                                   nok.is_once,
                                   nok.nm_campanha,
                                   nok.once_enviados,
                                   nok.once_bounce,
                                   nok.always_click,
                                   nok.always_abertura,
                                   ('\n%s (...) %s' % (nok.runtime_msg_error[0:50], nok.runtime_msg_error[-20:]))
                                   ) for _idx, nok
                       in df_fail.iterrows()]))

    print('\n\n# ***********************************\n')

    print('\n# Falhas detalhadas **************')
    df_err = df_log[df_log.runtime_error.notnull()]
    for _idx, nok in df_err.iterrows():
        print('-' * 10)
        print('Erro campanha[%s]\n-----\n*Error message\n%s\n-----\n*Full error\n[%s]' % (
            nok.id_campanha, nok.runtime_msg_error, nok.runtime_error))
        print('\n' * 2)

    print('\n# ***********************************\n\n')

    if df_err.empty:
        return log_exploded
    else:
        raise Warning('Retorno de Campanha contem erros!')


class BusinessObject(object):
    """
    Classe que encapsula as regras de negocio referente ao retorno de campanha AlliN.
    """

    PARTIAL_COMMIT_INDEX = 500


    def __init__(self, api, conn):
        self.api = api
        self.conn = conn
        self.log_exec = []

        logging.basicConfig(level=logging.INFO)  # ERROR #INFO #DEBUG
        urllib3.disable_warnings()

    # ****************************************************
    # Variaveis e funcoes para log de execucao
    # ****************************************************
    def obter_log_execucao(self):
        return self.log_exec

    def log_exec__append(self, id_definition, id_execution, campanha):
        self.log_exec.append({
            'id_definition': id_definition,
            'id_execution': id_execution,
            'id_campanha': campanha['itensConteudo_id_campanha'],
            'nm_campanha': campanha['itensConteudo_nm_campanha'],
            'dt_inicio': campanha['itensConteudo_dt_inicio'],
            'dt_encerramento': campanha['itensConteudo_dt_encerramento'],
            'is_once': None,
            'total_envio': campanha['itensConteudo_total_envio'],
            'total_entregues': campanha['itensConteudo_total_entregues'],
            'total_abertura': campanha['itensConteudo_total_abertura'],
            'total_clique': campanha['itensConteudo_total_clique'],
            'runtime_error': None,
            'runtime_msg_error': None
        })

    def log_exec__update(self, k_v):
        self.log_exec_atual().update({k_v[0]: k_v[1]})

    def log_exec_atual(self):
        return self.log_exec[-1]

    # ****************************************************

    def obter_dias_a_analisar_campanhas(self):
        _ret = ParametroOperacional.find_ValorParametro_by_id(ParametroOperacional.P__CAMPANHA_RETORNO_DIAS_A_ANALISAR,
                                                              conn=self.conn)
        return int(_ret)

    def buscar_entregas_encerradas(self, dt_ref):
        _b = DadosEntregaPorDataBuilder(dt_ref)
        infos = self.api.get_delivery_info_by_date(_b)
        return infos

    def buscar_execucao_from_campanha_encerrada(self, nm_campanha_allin, dt_ref):
        logging.info('[buscar_execucao_from_campanha_encerrada] campanha[%s] dt_ref[%s]' % (nm_campanha_allin, dt_ref))

        try:
            _idx_CM = nm_campanha_allin.index('_CM_')

            id_definition = int(nm_campanha_allin[_idx_CM + 4:nm_campanha_allin.find('_', _idx_CM + 4)])
            id_execution = ExecutionLog.find_MAX_IdExecution_by_IdDefinition_DataExecucao_SucessoIsTrue(id_definition,
                                                                                                        dt_ref,
                                                                                                        conn=self.conn)
            return id_definition, id_execution

        except:
            return None, None

    def iniciar_analise_retorno(self, id_definition, id_execution, campanha):
        self.log_exec__append(id_definition, id_execution, campanha)

        _ret = {CampanhaRetornoEmail.ID_DEFINITION: id_definition,
                CampanhaRetornoEmail.ID_EXECUTION: id_execution,
                CampanhaRetornoEmail.ID_CAMPANHA_EXTERNO: (campanha['itensConteudo_id_campanha']),
                CampanhaRetornoEmail.NOME_CAMPANHA_EXTERNO: (campanha['itensConteudo_nm_campanha']),
                CampanhaRetornoEmail.TOTAL_ENVIO: (campanha['itensConteudo_total_envio']),
                CampanhaRetornoEmail.TOTAL_ENTREGUE: (campanha['itensConteudo_total_entregues']),
                CampanhaRetornoEmail.TOTAL_ABERTURA: (campanha['itensConteudo_total_abertura']),
                CampanhaRetornoEmail.TOTAL_CLIQUE: (campanha['itensConteudo_total_clique']),
                }

        return _ret

    def persistir_campanha_retorno_email(self, campanha_retorno_email):
        try:
            id_definition = campanha_retorno_email[CampanhaRetornoEmail.ID_DEFINITION]
            id_execution = campanha_retorno_email[CampanhaRetornoEmail.ID_EXECUTION]
            id_campanha_externo = campanha_retorno_email[CampanhaRetornoEmail.ID_CAMPANHA_EXTERNO]

            totais = self.buscar_totais_atualizados(campanha_retorno_email)
            campanha_retorno_email[CampanhaRetornoEmail.TOTAL_ENVIO] = to_int(totais['itensConteudo_tot_envio'])
            campanha_retorno_email[CampanhaRetornoEmail.TOTAL_ENTREGUE] = to_int(totais['itensConteudo_tot_entregues'])
            campanha_retorno_email[CampanhaRetornoEmail.TOTAL_ABERTURA] = to_int(totais['itensConteudo_tot_aberto'])
            campanha_retorno_email[CampanhaRetornoEmail.TOTAL_CLIQUE] = to_int(totais['itensConteudo_nr_total_click'])

            CampanhaRetornoEmail.upsert(campanha_retorno_email, conn=self.conn)

            self.log_exec__update(['allin_envio', True])

            _ret = CampanhaRetornoEmail.find_by_id(id_definition, id_execution, id_campanha_externo, conn=self.conn)

            return _ret

        except Exception as e:
            logging.error('Falha inesperada [%s]' % e)
            logging.error(e)
            self.log_exec__update(['allin_envio', False])
            self.log_exec__update(['runtime_error', e])
            self.log_exec__update(['runtime_msg_error', str(e)])

        return None

    def buscar_totais_atualizados(self, campanha_retorno_email):
        _ident = '\t\t\t\t'
        logging.info('%s[buscar_totais_atualizados] info[%s]' % (_ident, campanha_retorno_email))

        id_campanha_externo = campanha_retorno_email[CampanhaRetornoEmail.ID_CAMPANHA_EXTERNO]

        b_report = RelatorioEntregaBuilder(campanha_id=id_campanha_externo)
        _ret = self.api.get_delivery_report(b_report)

        return _ret

    # **********
    # ONCE
    # **********
    def execute_once(self, campanha_retorno_email):
        _ident = '\t\t\t'

        _is_once = self.is_cenario_once(campanha_retorno_email)

        self.log_exec__update(['is_once', _is_once])

        logging.info('%sONCE [%s] Lista [%s] ' % (_ident, _is_once, campanha_retorno_email))
        if _is_once:
            if not (self.do_once_enviados(campanha_retorno_email) and
                    self.do_once_bounce(campanha_retorno_email)):
                raise self.log_exec_atual()['runtime_error']

        else:
            self.log_exec__update(['once_envio_lista', False])
            self.log_exec__update(('once_enviados', {'total': 0, 'sucesso': 0, 'falha': 0, 'duplicado': 0}))
            self.log_exec__update(['once_bounce', {'total': 0, 'sucesso': 0, 'falha': 0, 'duplicado': 0}])

    def is_cenario_once(self, campanha_retorno_email):
        return campanha_retorno_email[CampanhaRetornoEmail.DATA_ATUALIZACAO] is None

    def do_once_enviados(self, campanha_retorno_email):
        _ident = '\t\t\t\t'

        id_campanha_externo = campanha_retorno_email[CampanhaRetornoEmail.ID_CAMPANHA_EXTERNO]

        logging.info('%sONCE ENVIADOS [%s]' % (_ident, campanha_retorno_email))

        b_analytic = AnaliticoEntregaBuilder(campanha_id=id_campanha_externo)
        analytic = self.api.get_delivery_analytic(b_analytic)

        self.log_exec__update(['once_enviados', {'total': 0, 'sucesso': 0, 'falha': 0, 'duplicado': 0}])

        for idx, entrega in enumerate(analytic):
            logging.debug('%s entrega %s/%s' % (_ident, idx + 1, analytic.count()))
            _email = entrega['email']
            _status = entrega['status']
            _message = entrega['message_provider']
            _status = _status if len(_message) < 1 else '%s (%s)' % (entrega['status'], entrega['message_provider'])

            self.atualiza_status__contato_email(campanha_retorno_email, _email, _status)
            self.log_exec_atual()['once_enviados']['total'] = analytic.count()

            is_time_for_partial_commit = idx % self.PARTIAL_COMMIT_INDEX == 0 and idx > 0
            if is_time_for_partial_commit:
                logging.info('<<commit parcial em %s/%s>>' % (idx + 1, analytic.count()))
                database.db_commit(self.conn)

        return True

    def atualiza_status__contato_email(self, campanha_retorno_email, email, status):

        _count = self.log_exec_atual()['once_enviados']
        try:
            id_definition = campanha_retorno_email[CampanhaRetornoEmail.ID_DEFINITION]
            id_execution = campanha_retorno_email[CampanhaRetornoEmail.ID_EXECUTION]
            id_campanha_externo = campanha_retorno_email[CampanhaRetornoEmail.ID_CAMPANHA_EXTERNO]

            CampanhaContatoEmail.update_StatusEnvio_by_Email_IdDefinition_IdExecution(status,
                                                                                      id_campanha_externo,
                                                                                      email,
                                                                                      id_definition,
                                                                                      id_execution,
                                                                                      conn=self.conn)

            _count['sucesso'] = _count['sucesso'] + 1

        except Exception as e:
            _count['falha'] = _count['falha'] + 1
            logging.error('FALHA atualiza_status__contato_email [%s]' % e)

        finally:
            return True

    def do_once_bounce(self, campanha_retorno_email):
        _ident = '\t\t\t\t'
        id_campanha_externo = campanha_retorno_email[CampanhaRetornoEmail.ID_CAMPANHA_EXTERNO]

        logging.info('%sONCE BOUNCE [%s]' % (_ident, campanha_retorno_email))

        self.log_exec__update(['once_bounce', {'total': 0, 'sucesso': 0, 'falha': 0, 'duplicado': 0}])

        _b = ErrosPermanentesBuilder(campanha_id=id_campanha_externo, pagination_offset=200)
        pages = self.api.get_permanent_errors(_b)

        _count = 0
        for idx, bounce in enumerate(pages):
            logging.debug('%s bounce %s/%s' % (_ident, idx + 1, pages.count()))

            _email = bounce['itensConteudo_nm_email']
            _motivo_bounce = bounce['itensConteudo_msg']

            self.atualiza_motivo_bounce__contato_email(_email, _motivo_bounce, campanha_retorno_email)
            _count += 1

            is_time_for_partial_commit = idx % self.PARTIAL_COMMIT_INDEX == 0 and idx > 0
            if is_time_for_partial_commit:
                logging.info('<<commit parcial em %s/%s>>' % (idx + 1, pages.count()))
                database.db_commit(self.conn)

        self.log_exec_atual()['once_bounce']['total'] = _count

        return True

    def atualiza_motivo_bounce__contato_email(self, email, motivo_bounce, campanha_retorno_email):
        _count = self.log_exec_atual()['once_bounce']

        try:
            id_definition = campanha_retorno_email[CampanhaRetornoEmail.ID_DEFINITION]
            id_execution = campanha_retorno_email[CampanhaRetornoEmail.ID_EXECUTION]

            CampanhaContatoEmail.update_MotivoBounce_by_Email_IdDefinition_IdExecution(motivo_bounce,
                                                                                       email,
                                                                                       id_definition,
                                                                                       id_execution,
                                                                                       conn=self.conn)
            _count['sucesso'] = _count['sucesso'] + 1

        except Exception as e:
            _count['falha'] = _count['falha'] + 1
            logging.error('FALHA atualiza_motivo_bounce_contato_email [%s]' % e)

        finally:
            return True

    # **********
    # ALWAYS
    # **********
    def execute_always(self, campanha_retorno_email):
        _ident = '\t\t\t'

        logging.info('%sLista [%s] ALWAYS' % (_ident, campanha_retorno_email))

        self.do_always_clique(campanha_retorno_email)
        self.do_always_abertura(campanha_retorno_email)

    def do_always_clique(self, campanha_retorno_email):
        _ident = '\t\t\t\t'

        id_campanha_externo = campanha_retorno_email[CampanhaRetornoEmail.ID_CAMPANHA_EXTERNO]
        logging.info('%sALWAYS CLIQUE [%s]' % (_ident, campanha_retorno_email))

        self.log_exec__update(['always_click', {'total': 0, 'sucesso': 0, 'falha': 0, 'duplicado': 0}])
        self.log_exec__update(['contato_clique', {'total': 0, 'sucesso': 0, 'falha': 0}])

        _b = AnaliticoClickBuilder(campanha_id=id_campanha_externo, pagination_offset=1000)
        pages = self.api.get_click_analytic(_b)

        for idx, clique in enumerate(pages):
            logging.debug('%s clique %s/%s (estimado)' % (_ident, idx + 1, pages.count()))

            _email = clique['itensConteudo_nm_email']
            _data_clique = clique['itensConteudo_dt_click']
            _link_clique = clique['itensConteudo_nm_link']

            self.atualiza_clique__contato_email(_email, _data_clique, _link_clique, campanha_retorno_email)

            is_time_for_partial_commit = idx % self.PARTIAL_COMMIT_INDEX == 0 and idx > 0
            if is_time_for_partial_commit:
                logging.info('<<commit parcial em %s/%s>>' % (idx + 1, pages.count()))
                database.db_commit(self.conn)

    def atualiza_clique__contato_email(self, email, data_clique, link_clique, campanha_retorno_email):

        id_definition = campanha_retorno_email[CampanhaRetornoEmail.ID_DEFINITION]
        id_execution = campanha_retorno_email[CampanhaRetornoEmail.ID_EXECUTION]
        id_campanha_externo = campanha_retorno_email[CampanhaRetornoEmail.ID_CAMPANHA_EXTERNO]

        _count = self.log_exec_atual()['always_click']
        try:
            CampanhaContatoEmail.update_DataClique_LinkClique_by_Email_IdDefinition_IdExecution(data_clique,
                                                                                                link_clique,
                                                                                                id_campanha_externo,
                                                                                                email,
                                                                                                id_definition,
                                                                                                id_execution,
                                                                                                conn=self.conn)
            _count['sucesso'] = _count['sucesso'] + 1

        except Exception as e:
            _count['falha'] = _count['falha'] + 1
            logging.error('FALHA atualiza_clique__contato_email [%s]' % e)

        finally:
            _count['total'] = _count['total'] + 1

        return True

    def do_always_abertura(self, campanha_retorno_email):
        _ident = '\t\t\t\t'
        logging.info('%sALWAYS ABERTURA [%s]' % (_ident, campanha_retorno_email))

        self.log_exec__update(['always_abertura', {'total': 0, 'sucesso': 0, 'falha': 0, 'duplicado': 0}])
        self.log_exec__update(['contato_abertura', {'total': 0, 'sucesso': 0, 'falha': 0}])

        id_campanha_externo = campanha_retorno_email[CampanhaRetornoEmail.ID_CAMPANHA_EXTERNO]
        _b = AnaliticoAberturaBuilder(campanha_id=id_campanha_externo, pagination_offset=200)
        pages = self.api.get_opening_analytic(_b)

        for idx, abertura in enumerate(pages):
            logging.debug('%s abertura %s/%s' % (_ident, idx + 1, pages.count()))
            _email = abertura['itensConteudo_nm_email']
            _data_abertura = abertura['itensConteudo_dt_view']

            self.atualiza_abertura__contato_email(_email, _data_abertura, campanha_retorno_email)

            is_time_for_partial_commit = idx % self.PARTIAL_COMMIT_INDEX == 0 and idx > 0
            if is_time_for_partial_commit:
                logging.info('<<commit parcial em %s/%s>>' % (idx + 1, pages.count()))
                database.db_commit(self.conn)

    def atualiza_abertura__contato_email(self, email, data_abertura, campanha_retorno_email):
        _count = self.log_exec_atual()['always_abertura']

        try:
            id_definition = campanha_retorno_email[CampanhaRetornoEmail.ID_DEFINITION]
            id_execution = campanha_retorno_email[CampanhaRetornoEmail.ID_EXECUTION]
            id_campanha_externo = campanha_retorno_email[CampanhaRetornoEmail.ID_CAMPANHA_EXTERNO]

            CampanhaContatoEmail.update_DataAbertura_by_Email_IdDefinition_IdExecution(data_abertura,
                                                                                       id_campanha_externo,
                                                                                       email,
                                                                                       id_definition,
                                                                                       id_execution,
                                                                                       conn=self.conn)

            _count['sucesso'] = _count['sucesso'] + 1

        except Exception as e:
            _count['falha'] = _count['falha'] + 1
            logging.error('FALHA persiste__allin_allin_envio_contato_abertura [%s]' % e)

        finally:
            _count['total'] = _count['total'] + 1

        return True

    def notificar_erro(self, e):
        self.log_exec__update(['runtime_error', e])
        self.log_exec__update(['runtime_msg_error', str(e)])

    def obter_dias_a_reprocessar(self):
        _ret = ParametroOperacional.find_Valor_by_id_as_list(
                ParametroOperacional.P__DAG_ENVIOEMAIL_CAMPANHA_REPROCESSAR,
                conn=self.conn)
        return _ret
