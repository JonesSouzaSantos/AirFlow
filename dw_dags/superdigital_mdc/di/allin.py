import json
import logging
import os
import traceback
from contextlib import closing
from datetime import date

import pandas as pd

import superdigital_mdc.dw.database as database
import superdigital_mdc.dw.utils as utils
from superdigital_mdc.di import atualiza_lista, retorno_campanha
from superdigital_mdc.dw.airflow_utils import TaskException, get_operators_return_value, _set_xcom_token, \
    get_api_from_context, api__as_is, api__list_retoken_retry


# *****************************
# PythonOperator funks
# *****************************

@utils.time_this
def busca_allin_token(**context):
    """
    Inicializa um token para acesso a API AlliN. Deve ser usado como um operator pois grava no xcom o token de acesso
    que será utilizado por qualquer outro processor (function) na sequencia que seja decorado com `@api_$strategy`
    :param context:
    :return:
    """
    return _busca_allin_token_por_versao(1, **context)


@utils.time_this
def busca_allin_token_v2(**context):
    """
    Inicializa um token para acesso a API 2 AlliN. Deve ser usado como um operator pois grava no xcom o token de acesso
    que será utilizado por qualquer outro processor (function) na sequencia que seja decorado com `@api_$strategy`
    :param context:
    :return:
    """
    return _busca_allin_token_por_versao(2, **context)


def _busca_allin_token_por_versao(versao, **context):
    api = utils.MDCFactory.allin_api_by_version(versao)
    return _set_xcom_token(api.start_session(), **context)


@api__as_is
def envia_lista_by_id_definition(id_definition, **context):
    """
    Atraves do `id_definition` informado, busca uma execucao de segmentacao no YesCampaign e atualiza a respectiva lista na AlliN
    :param id_definition:
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

    api = get_api_from_context(**context)

    max_tentativas = context.get(utils.Constantes.KEY__MAX_TENTATIVAS_ENVIO_LISTA,
                                 utils.Constantes.DEF__MAX_TENTATIVAS_ENVIO_LISTA)

    log_envio = None

    with closing(database.db_connect(**context)) as conn:
        try:

            for _infos in database.do_db_query_fetchall(atualiza_lista.buscar_execucao_info(id_definition), conn=conn):
                if not _infos:
                    log_envio = {'id_definition': id_definition,
                                 'cd_lista': None,
                                 'id_execution': -1,
                                 'id_log': None,
                                 'nm_lista': None,
                                 'contagem': 0,
                                 'status': 'EXECUCAO_INEXISTENTE',
                                 'is_ok': True}
                    break

                cd_lista, nm_lista, id_execution = _infos

                basic_infos = {'id_definition': id_definition,
                               'cd_lista': cd_lista,
                               'nm_lista': nm_lista,
                               'id_execution': id_execution}

                logging.info("\tPreparando envio de Lista info[%s]" % (basic_infos))

                id_log = None

                try:
                    df = atualiza_lista.buscar_dados_envio_as_df(cd_lista, id_execution, conn)
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
                        break

                    _header = df.iloc[0]['tx_campos']
                    _file_path = '%s%s/' % (utils.Constantes.DEF__FILESYSTEM_FOLDER, str(date.today()))

                    try:
                        os.mkdir(_file_path)
                    except FileExistsError as e:
                        pass  # ignore

                    _file_name = 'lista_emails_%s__%s__%s.%s.csv' % (str(date.today()),
                                                                     id_definition,
                                                                     cd_lista,
                                                                     id_execution)

                    _file_full_path = '%s%s' % (_file_path, _file_name)

                    df_4_lista = df.drop(columns='contaCorrenteId')
                    atualiza_lista.criar_arquivo_lista(df_4_lista, _file_full_path, _header)

                    qtd_tentativas = 0
                    msg_error = None
                    while qtd_tentativas < max_tentativas:
                        logging.info('\t\tTentativa #%s' % qtd_tentativas)
                        try:
                            id_log = atualiza_lista.executar_upload_lista(api, cd_lista, nm_lista, _header, _file_name,
                                                                          _file_path)

                            break

                        except Exception as e:
                            msg_error = e

                        finally:
                            qtd_tentativas += 1  # em caso de sucesso, marca uma tentativa

                    _is_upload_ok = id_log is not None

                    _status = 'ENVIADO' if _is_upload_ok else 'NAO_ENVIADO[%s]' % msg_error
                    atualiza_lista.atualizar_status_lista(conn, _is_upload_ok, cd_lista, id_execution, msg_error)

                    if _is_upload_ok:
                        logging.info("\t\tPersistindo informacoes de Contato")
                        for _ in df.iterrows():
                            atualiza_lista.persistir_contato(_[1], id_definition, conn=conn)

                    database.db_commit(conn=conn)

                    # atualiza_lista.apagar_arquivo(_file_name)

                    log_envio = {'id_definition': id_definition,
                                 'cd_lista': int(cd_lista),
                                 'id_execution': int(id_execution),
                                 'id_log': int(id_log),
                                 'nm_lista': nm_lista,
                                 'file_name': _file_name,
                                 'contagem': int(df.size / len(df.columns)),
                                 'status': _status,
                                 'is_ok': True}

                except Exception as e:
                    log_envio = {'id_definition': id_definition,
                                 'cd_lista': int(cd_lista),
                                 'id_execution': int(id_execution),
                                 'id_log': id_log or None,
                                 'nm_lista': nm_lista,
                                 'file_name': None,
                                 'contagem': 0,
                                 'status': 'ERRO [%s] \n\tStackTrace[%s]' % (e, utils.stack_trace(e)),
                                 'is_ok': False}

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

    logging.info("Envio de lista para [%s] FINALIZADO! Status execucao [%s]" % (id_definition, log_envio))

    if log_envio and not log_envio['is_ok']:
        raise TaskException('Execucao com FALHA! %s' % log_envio)

    return log_envio


@utils.time_this
def resumo_atualiza_lista(operator_names, **context):
    dt_ref = date.today()

    log_exec = get_operators_return_value(operator_names, **context)

    df_log = pd.DataFrame([*log_exec]).sort_values(by=['cd_lista'])
    df_env = df_log[df_log.is_ok == True]
    df_nenv = df_log[df_log.is_ok == False]

    _msg = '%s,' * 6
    print('\n\n# CAMPANHAS LISTA ENVIADA **************\n')
    print(_msg % ('Dt_Ref', 'IdDefinition', 'Nm_Lista', 'Status', 'IdExecution', 'contagem'))
    print('\n'.join([_msg % (dt_ref, int(ok.id_definition), ok.nm_lista, ok.status, int(ok.id_execution), int(ok.contagem))
                     for _idx, ok in df_env.iterrows()]))
    print('\n\n# CAMPANHAS LISTA NAO ENVIADA **********\n')
    print(_msg % ('Dt_Ref', 'IdDefinition', 'Nm_Lista', 'Status', 'IdExecution', 'contagem'))
    print('\n'.join([_msg % (dt_ref, nok.id_definition, nok.nm_lista, nok.status, nok.id_execution, int(nok.contagem))
                     for _idx, nok in df_nenv.iterrows()]))
    print('\n# ***********************************\n\n')

    print(log_exec)

    _json = json.dumps([*log_exec])

    if df_nenv.empty:
        return _json
    else:
        print(_json)
        raise Warning('Envio de listas contem erros!')


@api__list_retoken_retry
def analisa_retorno_campanhas(dt_ref, **context):
    """
    Com base da data de referencia informada, busca por Campanhas encerradas na data, extrai as informacoes via API
    AlliN e as popula nas tabelas de controle Yes Campaign
    :param dt_ref:
    :param context:
    :return: Array contendo dicionarios no seguinte formato:
        {
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

    api = get_api_from_context(**context)

    with closing(database.db_connect(**context)) as conn:

        bo = retorno_campanha.BusinessObject(api, conn)

        envios = bo.buscar_entregas_encerradas(dt_ref)

        _ident = '\t\t'
        if not envios:
            logging.info('%sNAO HA DADOS A PROCESSAR PARA [%s]' % (_ident, dt_ref))

        else:
            logging.info('%sProcessando [%s] listas para data[%s]' % (_ident, len(envios), dt_ref))

            for _ in envios:
                id_campanha_allin = _['itensConteudo_id_campanha']
                logging.info('%sIniciando Lista [%s]' % (_ident, id_campanha_allin))

                try:
                    if bo.persistir__allin_envio(_):
                        logging.info('%sLista PERSISTIDA COM SUCESSO [%s]' % (_ident, id_campanha_allin))
                        bo.execute_once(_)
                        bo.execute_always(_)

                        database.db_commit(conn)

                    else:
                        logging.info('%sLista NAO FOI PERSISTIDA [%s]' % (_ident, id_campanha_allin))

                except Exception as e:
                    _stacktrace = ''.join(traceback.format_tb(e.__traceback__))
                    logging.error('%sRollback sera chamado para Lista [%s] Cause[%s]\n\tStacktrace[%s]' % (_ident,
                                                                                                           id_campanha_allin,
                                                                                                           e,
                                                                                                           _stacktrace))
                    bo.notificar_erro(e)
                    database.db_rollback(conn)

    return bo.obter_log_execucao()


@utils.time_this
def resumo_retorno_campanhas(operator_names, **context):
    log_exec = get_operators_return_value(operator_names, **context)
    log_exploded = []
    for log in log_exec:
        for _ in log:
            log_exploded.append(_)

    print('\nLog Execucao:\n%s\n\n' % log_exploded)

    df_log = pd.DataFrame(log_exploded).sort_values(by=['id_campanha'])

    df_success = df_log[df_log.runtime_error.isnull()]
    df_fail = df_log[df_log.runtime_error.notnull()]

    _msg_ok = 'Campanha [%s] data [%s] once[%s] \n%s \n\tLista criada? [%s]' \
              '\n\tEnviados [%s]\n\tBounce [%s]\n\t Click [%s]\n\tAbertura [%s]'

    _msg_nok = 'Campanha [%s] data [%s] once[%s] \n%s \n\tLista criada? [%s]' \
               '\n\tEnviados [%s]\n\tBounce [%s]\n\t Click [%s]\n\tAbertura [%s]\nErro:%s'

    print('\n# SUCESSO AO PROCESSAR **************')
    print('\n\n'.join([_msg_ok % (ok.id_campanha,
                                  ok.dt_inicio,
                                  ok.is_once,
                                  ok.nm_campanha,
                                  ok.once_envio_lista,
                                  ok.once_enviados,
                                  ok.once_bounce,
                                  ok.always_click,
                                  ok.always_abertura
                                  ) for _idx, ok in df_success.iterrows()]))
    print('\n# FALHA AO PROCESSAR ****************')
    print('\n\n'.join([_msg_nok % (nok.id_campanha,
                                   nok.dt_inicio,
                                   nok.is_once,
                                   nok.nm_campanha,
                                   nok.once_envio_lista,
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
        print('*Stack trace\n-----')
        print(''.join(traceback.format_tb(nok.runtime_error.__traceback__)))
        print('\n' * 2)

    print('\n# ***********************************\n\n')

    if df_err.empty:
        return log_exploded
    else:
        raise Warning('Retorno de Campanha contem erros!')
