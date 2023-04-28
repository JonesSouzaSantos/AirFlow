import logging
from contextlib import closing
from datetime import datetime, date

import pandas as pd

from superdigital_de.di.repositorios import Extracao, FonteDados, ExtracaoExecucao, Sucesso, TipoDelta, Procedures, \
    ExecExtracao
from superdigital_mdc.dw import database, utils, airflow_utils
from superdigital_mdc.dw.utils import Constantes


def obter_extracao_diaria_ativa(**kwargs):
    with closing(database.db_connect(**kwargs)) as conn:
        bo = BusinessObject(conn)
        return bo.obter_full_extracao_diaria_ativa()


def executar_ingestao(id_extracao, **kwargs):
    print('[executar_ingestao] iniciando ingestao para extracao [%s]' % id_extracao)

    extracao_execucao = None

    with closing(database.db_connect(**kwargs)) as conn:
        bo = BusinessObject(conn)

        extracao, origem, destino = bo.obter_extracao(id_extracao)

        bo.verifica_execucao_em_andamento()

        is_permitido, extracao_execucao_anterior = bo.is_permite_N_execucao_no_dia(extracao)

        if is_permitido:
            particao = str(utils.get_now_utc_posix())

            extracao_execucao, valor_delta_ref, valor_delta_prox, id_log_carga = bo.configurar_extracao_execucao(particao,
                                                                                                   extracao, origem,
                                                                                                   destino,
                                                                                                   extracao_execucao_anterior)

            extracao_execucao = bo.executar_extracao(extracao_execucao, origem, destino, valor_delta_ref,
                                                     valor_delta_prox,id_log_carga)

            # extracao_execucao = bo.contabiliza_extracao(is_sucesso, particao, extracao_execucao, destino,
            #                                             valor_delta_ref,
            #                                             valor_delta_prox)

        else:
            extracao_execucao = extracao_execucao_anterior

        extracao_execucao.update({'origem': origem[FonteDados.NOME], 'destino': destino[FonteDados.NOME]})

    return extracao_execucao


def iniciar_ingestao(a, b, c, **kwargs):
    raise NotImplementedError


@utils.time_this
def resumo_extracao(operator_names, **context):
    log_exec = None

    is_error = False
    try:
        log_exec = airflow_utils.get_operators_return_value(operator_names, **context)

        print('\n\n# LOG COMPLETO DE EXECUCAO **************\n%s\n**************\n' % str(log_exec))

        if not log_exec:
            print('NAO HOUVE EXECUCOES!')
            return True

        df_log = pd.DataFrame([_ for _ in log_exec if isinstance(_, dict)]).sort_values(by=['idExtracao'])
        df_ok = df_log[df_log.sucesso == Sucesso.OK.value]
        df_nok = df_log[df_log.sucesso == Sucesso.NOK.value]
        df_unk = df_log[df_log.sucesso == Sucesso.INDEFINIDO.value]

        _msg = '%s,' * 8
        titulo = _msg % ('Fonte', 'idExtracao', 'idExtracaoExecucao',
                         'inicio', 'termino',
                         'registrosCriados', 'registrosAlterados',
                         'LOG')

        print('\n\n# EXTRACAO COM SUCESSO **************\n')
        print(titulo)
        if not df_ok:
            print('\n'.join([_msg % (ok.origem,
                                     ok.idExtracao,
                                     ok.idExtracaoExecucao,
                                     ok.inicio,
                                     ok.termino,
                                     ok.registrosCriados,
                                     ok.registrosAlterados,
                                     ok.log,
                                     '\n')
                             for _idx, ok in df_ok.iterrows()]))

        print('\n\n# EXTRACAO SEM SUCESSO **********\n')
        print(titulo)
        if not df_nok.empty:
            print('\n'.join([_msg % (nok.origem,
                                     nok.idExtracao,
                                     nok.idExtracaoExecucao,
                                     nok.inicio,
                                     nok.termino,
                                     nok.registrosCriados,
                                     nok.registrosAlterados,
                                     nok.log,
                                     '\n')
                             for _idx, nok in df_nok.iterrows()]))

        print('\n\n# EXTRACAO SEM DEFINICAO **********\n')
        print(titulo)
        if not df_unk.empty:
            print('\n'.join([_msg % (unk.origem,
                                     unk.idExtracao,
                                     unk.idExtracaoExecucao,
                                     unk.inicio,
                                     unk.termino,
                                     unk.registrosCriados,
                                     unk.registrosAlterados,
                                     unk.log,
                                     '\n')
                             for _idx, unk in df_unk.iterrows()]))

        print('\n# ***********************************\n\n')

        is_error = not df_nok.empty

    except Exception as e:
        print('Falha inesperada! [%s]\n\n\n' % str(e))
        print(e)

    if is_error:
        from superdigital_mdc.dw.airflow_utils import TaskException
        raise TaskException('Extracao de dados com falha, favor validar!')

    return log_exec


class BusinessObject(object):
    CHECK_INTERVAL = 10
    CHECK_TIMEOUT = 10000
    COLUNA_PARTICAO = 'ED_particao'
    COLUNA_IDLOGCARGA = 'idLogCarga'
    COLUNA_SCD = 'SCD_registro_atual'
    COLUNA_SCD_VALOR = '1'

    def __init__(self, conn):
        self.conn = conn
        self.logger = logging.getLogger('extracao.BusinessObject')

    def obter_extracao(self, id_extracao):
        self.logger.info('[obter_extracao] Extracao [%s]' % id_extracao)
        extracao, origem, destino = Extracao.find_by_id_with_FKs(id_extracao, conn=self.conn)
        return extracao, origem, destino

    def obter_full_extracao_diaria_ativa(self):
        self.logger.info('[obter_full_extracao_diaria_ativa] Buscando Extracoes ativas a serem realizadas')

        extracao_origem_destino = Extracao.find_by_PeriodicidadeIsD_AtivoIsTrue_OrderByPrioridade_with_FKs(
                conn=self.conn)
        return extracao_origem_destino

    def verifica_execucao_em_andamento(self):
        is_permite_paralelismo = True
        self.logger.info('[verifica_execucao_em_andamento] Buscando Execucoes fantasmas/pendende de execucao. [%s]' %
                         is_permite_paralelismo)

        if not is_permite_paralelismo:
            count = ExtracaoExecucao.has_SucessoIsINDEFINIDO_TerminoIsNull(conn=self.conn)
            if count and count > 0:
                self.logger.info('[verifica_execucao_em_andamento] Fantasmas existem! count[%s]' % count)

                from superdigital_mdc.dw.airflow_utils import TaskException
                raise TaskException('Ja existe uma ExtracaoExecucao executando/a ser executada!')

    def is_permite_N_execucao_no_dia(self, extracao):
        is_2a_exec_ok = extracao[Extracao.PERMITE_N_EXECUCOES_DIA]

        self.logger.info('[is_permite_N_execucao_no_dia] [%s]' % is_2a_exec_ok)

        extracao_execucao_anterior = ExtracaoExecucao. \
            find_last_by_IdExtracao_SucessoIsTrue(extracao[Extracao.ID_EXTRACAO], conn=self.conn)

        self.logger.info('[is_permite_N_execucao_no_dia] Consultando extracao_execucao ANTERIOR ->\n[%s]' %
                         extracao_execucao_anterior)

        is_permitido = True

        if extracao_execucao_anterior:
            if date.today() == extracao_execucao_anterior[ExtracaoExecucao.INICIO].date():
                is_permitido = is_2a_exec_ok

        self.logger.info('[is_permite_N_execucao_no_dia] Permitido? [%s]' % is_permitido)

        return is_permitido, extracao_execucao_anterior

    def configurar_extracao_execucao(self, particao, extracao, origem, destino, extracao_execucao_anterior):
        self.logger.info('[configurar_extracao_execucao] Cria uma Execucao de Extracao para \n'
                         '\tExtracao[%s] \n\tOrigem[%s] \n\tDestino[%s] particao[%s]' % (extracao[Extracao.ID_EXTRACAO],
                                                                                         origem[FonteDados.NOME],
                                                                                         destino[FonteDados.NOME],
                                                                                         particao))

        tipo_delta = origem[FonteDados.DELTA_TIPO]

        extracao_execucao = ExtracaoExecucao.as_entity()
        extracao_execucao[ExtracaoExecucao.ID_EXTRACAO] = extracao[Extracao.ID_EXTRACAO]
        extracao_execucao[ExtracaoExecucao.SUCESSO] = Sucesso.INDEFINIDO.value

        valor_delta_ref, valor_delta_prox = self.obtem_delta_ref_prox(extracao_execucao_anterior, origem)

        delta_destino = self.preparar_destino(extracao_execucao, destino, valor_delta_ref)

        self.logger.info('[configurar_extracao_execucao] Parametros calculado da Execucao\n'
                         'Delta Tipo[%s] \n'
                         'Delta Ref[%s] \n'
                         'Prox Delta [%s] \n'
                         'Delta Destino[\n%s\n]' % (tipo_delta,
                                                    valor_delta_ref,
                                                    valor_delta_prox,
                                                    delta_destino))

        extracao_execucao[ExtracaoExecucao.QUERY_FONTE_DADOS] = 'SELECT 1'
        extracao_execucao[ExtracaoExecucao.SQL_DELTA_DESTINO] = delta_destino
        extracao_execucao[ExtracaoExecucao.VALOR_DELTA] = self.get_delta_value_for_db(valor_delta_ref, tipo_delta)
        extracao_execucao[ExtracaoExecucao.VALOR_PROXIMO_DELTA] = self.get_delta_value_for_db(valor_delta_prox,
                                                                                              tipo_delta)

        extracao_execucao = ExtracaoExecucao.create(extracao_execucao, conn=self.conn)

        id_extracao_execucao = extracao_execucao[ExtracaoExecucao.ID_EXTRACAO_EXECUCAO]
        query, id_log_carga = self.criar_query_origem(particao, valor_delta_ref, origem, id_extracao_execucao)

        ExtracaoExecucao.update(dict_update={ExtracaoExecucao.QUERY_FONTE_DADOS: query},
                                dict_where={ExtracaoExecucao.ID_EXTRACAO_EXECUCAO: id_extracao_execucao},
                                conn=self.conn)

        self.logger.info('[configurar_extracao_execucao] Query na Origem [%s]' % query)

        database.db_commit(conn=self.conn)

        return ExtracaoExecucao.find_by_id(id_extracao_execucao, conn=self.conn), valor_delta_ref, valor_delta_prox, id_log_carga

    def criar_query_origem(self, particao, valor_delta_ref, origem, id_extracao_execucao):
        self.logger.info('[criar_query_origem] Montando query para Origem [%s] Deltas[%s|%s]' %
                         (origem[FonteDados.ENDERECO],
                          origem[FonteDados.DELTA_CRIACAO],
                          origem[FonteDados.DELTA_ALTERACAO]))

        id_log_carga = ExtracaoExecucao.__ID_LOG_CARGA__
        id_log_carga += int(id_extracao_execucao)

        alias_origem = '_origem_'
        col_cri = origem[FonteDados.DELTA_CRIACAO]
        col_alt = origem[FonteDados.DELTA_ALTERACAO]
        tipo_delta = TipoDelta(origem[FonteDados.DELTA_TIPO]) if origem[FonteDados.DELTA_TIPO] else None
        def_where = origem[FonteDados.CLAUSULA_WHERE]

        all_columns = "%s, " \
                      "%s AS %s, " \
                      "%s as %s, " \
                      "'%s' as %s" % (origem[FonteDados.COLUNAS],
                                      id_log_carga, self.COLUNA_IDLOGCARGA,
                                      self.COLUNA_SCD_VALOR, self.COLUNA_SCD,
                                      particao, self.COLUNA_PARTICAO)

        query = 'SELECT %s  FROM %s %s (nolock) WHERE 1 = 1' % (all_columns,
                                                                origem[FonteDados.ENDERECO],
                                                                alias_origem)

        # Adicionando clausulaWhere da Origem
        if def_where:
            query = '%s AND (%s) ' % (query, def_where)

        # Adicionando clausulas Delta
        has_delta = col_cri or col_alt
        if has_delta:
            if valor_delta_ref:
                deltas = []
                if col_cri:
                    deltas.append(self.get_delta_clause(col_cri, tipo_delta, valor_delta_ref))
                if col_alt:
                    deltas.append(self.get_delta_clause(col_alt, tipo_delta, valor_delta_ref))

                # WHERE de deltas entre parenteses e com OR
                query = '%s AND (%s)' % (query, ' or '.join(deltas))

        # OBRIGATORIO nao ter quebra de linha
        return query.replace('\n', ' ') , id_log_carga

    def preparar_destino(self, extracao_execucao, destino, valor_delta_ref):
        is_sanitizacao = True
        self.logger.info('[preparar_destino] Sanitizar destino?[%s]' % is_sanitizacao)

        if not is_sanitizacao:
            return 'SELECT 1'

        self.logger.info('[preparar_destino] Destino sera sanitizado para ingestao: Destino[%s] Delta[%s]' % (
            destino[FonteDados.ENDERECO], extracao_execucao[ExtracaoExecucao.VALOR_DELTA]))

        col_cri = destino[FonteDados.DELTA_CRIACAO]
        col_alt = destino[FonteDados.DELTA_ALTERACAO]
        tipo_delta = TipoDelta(destino[FonteDados.DELTA_TIPO]) if destino[FonteDados.DELTA_TIPO] else None
        destino_where = destino[FonteDados.CLAUSULA_WHERE]
        has_delta_or_where = valor_delta_ref or destino_where

        sql = None
        if has_delta_or_where:
            sql = 'DELETE FROM %s WHERE 1 = 1 ' % destino[FonteDados.ENDERECO]

            # restricao WHERE default
            if destino_where:
                sql = '%s AND (%s) ' % (sql, destino_where)

            # restricao DELTAS
            if valor_delta_ref:
                deltas = []
                if col_cri:
                    deltas.append(self.get_delta_clause(col_cri, tipo_delta, valor_delta_ref))
                if col_alt:
                    deltas.append(self.get_delta_clause(col_alt, tipo_delta, valor_delta_ref))

                # WHERE de deltas entre parenteses e com OR
                sql = '%s AND (%s)' % (sql, ' or '.join(deltas))

        else:
            sql = 'TRUNCATE TABLE %s' % destino[FonteDados.ENDERECO]

        self.logger.info('[preparar_destino] Metodo de sanitizacao do Destino [%s]\n[%s]' % (sql.split()[0], sql))

        return sql

    def executar_extracao(self, extracao_execucao, origem, destino, valor_delta_ref, valor_delta_prox,id_log_carga):
        id_extracao_execucao = extracao_execucao[ExtracaoExecucao.ID_EXTRACAO_EXECUCAO]
        self.logger.info('[executar_extracao] Iniciando Execucao de EXTRACAO [%s]' % id_extracao_execucao)

        retorno = None
        try:
            retorno = Procedures.ExecExtracao.exec(entidade=origem[FonteDados.NOME],
                                                   origem_conn=Constantes.DEF__ORIGEM_EXTRACAO_CONNECTION_STRING,
                                                   origem_cmd=extracao_execucao[ExtracaoExecucao.QUERY_FONTE_DADOS],
                                                   destino_conn=Constantes.DEF__DESTINO_EXTRACAO_CONNECTION_STRING,
                                                   destino_tbl=destino[FonteDados.ENDERECO],
                                                   destino_sanitizacao=extracao_execucao[
                                                       ExtracaoExecucao.SQL_DELTA_DESTINO],
                                                   conn=self.conn)
            assert isinstance(retorno, ExecExtracao.Retorno)

            self.logger.info('[executar_extracao] Sucesso?[%s] \nLog Retorno[%s]' % (retorno.is_sucesso(), retorno))

            update = {ExtracaoExecucao.TERMINO: datetime.now(),
                      ExtracaoExecucao.SUCESSO: Sucesso(retorno.is_sucesso()).value,
                      ExtracaoExecucao.REGISTROS_CRIADOS: retorno.total_registros}
            where = {ExtracaoExecucao.ID_EXTRACAO_EXECUCAO: id_extracao_execucao}

            if retorno.is_sucesso():

                # Atualiza proximo valor de delta se necessario
                if valor_delta_ref:
                    valor_delta_prox = extracao_execucao[ExtracaoExecucao.INICIO]

                    update[ExtracaoExecucao.VALOR_PROXIMO_DELTA] = self.calcula_novo_delta(destino,
                                                                                           valor_delta_ref,
                                                                                           valor_delta_prox)

                if destino[FonteDados.COLUNA_DEDUP] and  destino[FonteDados.DELTA_ALTERACAO]:
                    registros_atualizados = self.limpa_duplicatas_delta(destino, id_log_carga)
                    update[ExtracaoExecucao.REGISTROS_ALTERADOS] = registros_atualizados

            ExtracaoExecucao.update(update, where, conn=self.conn)

        except Exception as e:
            self.logger.error('Erro inesperado na Execucao da Extracao! Marcando Extracao como NOK! Causa[%s]' % e, e)
            update = {ExtracaoExecucao.TERMINO: datetime.now(),
                      ExtracaoExecucao.SUCESSO: Sucesso.NOK.value,
                      ExtracaoExecucao.REGISTROS_CRIADOS: None,
                      ExtracaoExecucao.REGISTROS_ALTERADOS: None}
            where = {ExtracaoExecucao.ID_EXTRACAO_EXECUCAO: id_extracao_execucao}
            ExtracaoExecucao.update(update, where, conn=self.conn)
            raise ##Adicionado por Jones Santos em 2022-09-14

        database.db_commit(self.conn)

        extracao_execucao = ExtracaoExecucao.find_by_id(id_extracao_execucao, conn=self.conn)
        extracao_execucao.update({'log': retorno.as_dict() if retorno else None})
        return extracao_execucao

    # def executar_extracao(self, extracao_execucao):
    #     self.logger.info('[executar_extracao] Iniciando Execucao de EXTRACAO [%s]' % extracao_execucao[
    #         ExtracaoExecucao.ID_EXTRACAO_EXECUCAO])
    #
    #     Procedures.ExtracaoJob.exec(conn=self.conn)
    #
    #     try:
    #         inicio = datetime.now()
    #
    #         with ThreadPoolExecutor(thread_name_prefix='check_extracao_execucao') as executor:
    #             futures = [executor.submit(self.check_extracao_execucao, extracao_execucao, inicio)]
    #             for future in as_completed(futures):
    #                 try:
    #                     resultado = future.result()
    #                     self.logger.info('[executar_extracao] FINALIZADO ----- sucesso?[%s]' % resultado)
    #
    #                     return resultado
    #                 except TimeoutError as e:
    #                     self.logger.error('[executar_extracao] TIMEOUT ----- [%s]' % str(e))
    #                     raise e
    #
    #                 except Exception as e:
    #                     self.logger.error('[executar_extracao] INESPERADO -- [%s]' % str(e))
    #                     raise e
    #
    #     except Exception as e:
    #         self.logger.error('Erro ao iniciar extracao! Causa[%s]' % e)
    #
    #     return False
    #
    # def check_extracao_execucao(self, extracao_execucao, inicio):
    #     id_extracao_execucao = extracao_execucao[ExtracaoExecucao.ID_EXTRACAO_EXECUCAO]
    #     self.logger.info('[check_extracao_execucao] Status do Job da ExtracaoExecucao [%s] \n[%s]' % (
    #         id_extracao_execucao, extracao_execucao))
    #
    #     count = 0
    #     for _extracao_execucao in iter(partial(ExtracaoExecucao.find_by_id, id_extracao_execucao, conn=self.conn), ''):
    #         self.logger.info(
    #                 '[check_extracao_execucao] ExtracaoExecucao id[%s] sucesso[%s] #[%s]' % (
    #                     id_extracao_execucao, _extracao_execucao[ExtracaoExecucao.SUCESSO], count))
    #
    #         sucesso = Sucesso(_extracao_execucao[ExtracaoExecucao.SUCESSO])
    #
    #         if Sucesso.INDEFINIDO == sucesso:
    #             self.logger.info('Extracao [%s] ainda em execucao!' % id_extracao_execucao)
    #
    #             agora = datetime.now()
    #             time_diff = agora - inicio
    #
    #             is_timeout = self.CHECK_TIMEOUT < time_diff.seconds
    #             if is_timeout:
    #                 raise TimeoutError
    #             else:
    #                 self.logger.info('Novo check em %s segundos' % self.CHECK_INTERVAL)
    #
    #             count += 1
    #             time.sleep(self.CHECK_INTERVAL)
    #
    #         else:
    #             time.sleep(self.CHECK_INTERVAL)  # sleep para aguardar finalizar o job
    #             if Sucesso.NOK == sucesso:
    #                 return False
    #
    #             elif Sucesso.OK == sucesso:
    #                 return True

    # def contabiliza_extracao(self, is_sucesso, particao, extracao_execucao, destino, valor_delta_ref, valor_delta_prox):
    #     # TODO FIXME fluxo de acao de extracao - Definir se fica ou nao
    #     is_determinar_acao_extracao = False
    #
    #     self.logger.info('[contabiliza_extracao] Sucesso?[%s] '
    #                      'Atualizar quantidades?[%s]' % (is_sucesso, is_determinar_acao_extracao))
    #
    #     if is_sucesso:
    #         self.logger.info(
    #                 '[contabiliza_extracao] Atualiza quantidades da Extracao Destino[%s] Delta[%s]' % (
    #                     destino[FonteDados.ENDERECO], extracao_execucao[ExtracaoExecucao.VALOR_DELTA]))
    #
    #         if is_determinar_acao_extracao:
    #             col_cri = destino[FonteDados.DELTA_CRIACAO]
    #             col_alt = destino[FonteDados.DELTA_ALTERACAO]
    #             tipo_delta = TipoDelta(destino[FonteDados.DELTA_TIPO]) if destino[FonteDados.DELTA_TIPO] else None
    #             destino_where = destino[FonteDados.CLAUSULA_WHERE]
    #
    #             params = [particao]
    #             sql = """
    #                 UPDATE %s
    #                 SET ED_acaoExtracao = ?,
    #                 WHERE %s = ?
    #             """ % (destino[FonteDados.ENDERECO], self.COLUNA_PARTICAO)
    #
    #             # restricao WHERE default
    #             if destino_where:
    #                 sql = '%s AND (%s) ' % (sql, destino_where)
    #
    #             if valor_delta_ref:
    #                 self.logger.info(
    #                         '[contabiliza_extracao] Colunas Delta criacao[%s] alteracao[%s] valor_delta[%s] where[%s]' % (
    #                             col_cri,
    #                             col_alt,
    #                             valor_delta_ref,
    #                             destino_where))
    #
    #                 # DELTAS por tipo
    #                 if col_cri:
    #                     sql_cri = '%s AND (%s) ' % (sql, self.get_delta_clause(col_cri, tipo_delta, valor_delta_ref))
    #
    #                     params_cri = [AcaoExtracao.INSERT.value]
    #                     params_cri.extend(params)
    #                     self.logger.info('[contabiliza_extracao] CRIACAO [%s] [%s]' % (sql_cri, params_cri))
    #
    #                     qtd_criados = database.do_db_execute(sql_cri, parameters=params_cri, conn=self.conn)
    #                     self.logger.info('[contabiliza_extracao] CRIACAO [%s] registros' % qtd_criados)
    #                     extracao_execucao[ExtracaoExecucao.REGISTROS_CRIADOS] = qtd_criados
    #
    #                 if col_alt:
    #                     sql_alt = '%s AND (%s)' % (sql, self.get_delta_clause(col_alt, tipo_delta, valor_delta_ref))
    #
    #                     params_alt = [AcaoExtracao.UPDATE.value]
    #                     params_alt.extend(params)
    #                     self.logger.info('[contabiliza_extracao] ALTERACAO [%s] [%s]' % (sql_alt, params_alt))
    #
    #                     qtd_alterados = database.do_db_execute(sql_alt, parameters=params_alt, conn=self.conn)
    #                     self.logger.info('[contabiliza_extracao] ALTERACAO [%s] registros' % qtd_alterados)
    #                     extracao_execucao[ExtracaoExecucao.REGISTROS_ALTERADOS] = qtd_alterados
    #
    #             else:
    #                 params.insert(0, AcaoExtracao.INDETERMINADO.value)
    #                 self.logger.info('[contabiliza_extracao] FULL [%s] [%s]' % (sql, params))
    #
    #                 qtd = database.do_db_execute(sql, parameters=params, conn=self.conn)
    #                 self.logger.info('[contabiliza_extracao] FULL [%s] registros' % qtd)
    #
    #                 extracao_execucao[ExtracaoExecucao.REGISTROS_CRIADOS] = qtd
    #                 extracao_execucao[ExtracaoExecucao.REGISTROS_ALTERADOS] = 0
    #         # fim [if is_determinar_acao_extracao:]
    #
    #         # Atualiza proximo valor de delta se necessario
    #         if valor_delta_ref:
    #             valor_delta_prox = self.calcula_novo_delta(destino, valor_delta_ref, valor_delta_prox)
    #             extracao_execucao[ExtracaoExecucao.VALOR_PROXIMO_DELTA] = valor_delta_prox
    #
    #             # Atualiza entidade
    #             ExtracaoExecucao.update_ValorProximoDelta_RegistrosCriados_RegistrosAlterados(
    #                     extracao_execucao[ExtracaoExecucao.ID_EXTRACAO_EXECUCAO],
    #                     extracao_execucao[ExtracaoExecucao.VALOR_PROXIMO_DELTA],
    #                     extracao_execucao[ExtracaoExecucao.REGISTROS_CRIADOS],
    #                     extracao_execucao[ExtracaoExecucao.REGISTROS_ALTERADOS], conn=self.conn)
    #
    #         database.db_commit(conn=self.conn)
    #
    #         self.logger.info('[contabiliza_extracao] Quantidades: Full/Criados[%s] Alterados[%s] ProximoDelta[%s]' % (
    #             extracao_execucao[ExtracaoExecucao.REGISTROS_CRIADOS],
    #             extracao_execucao[ExtracaoExecucao.REGISTROS_ALTERADOS],
    #             valor_delta_prox))
    #
    #     return ExtracaoExecucao.find_by_id(extracao_execucao[ExtracaoExecucao.ID_EXTRACAO_EXECUCAO], conn=self.conn)

    def calcula_novo_delta(self, destino, valor_delta_ref, valor_delta_prox):
        self.logger.info('[calcula_novo_delta] Verifica se necessita atualiza valor do proximo delta')

        if not destino[FonteDados.DELTA_TIPO]:
            self.logger.info('[calcula_novo_delta] Nao utiliza delta![%s]' % destino[FonteDados.DELTA_TIPO])
            return valor_delta_prox

        is_atualiza_delta = [TipoDelta.INT, ].count(TipoDelta(destino[FonteDados.DELTA_TIPO])) > 0

        self.logger.info('[calcula_novo_delta] Atualizar proximo valor delta?[%s]' % is_atualiza_delta)
        if not is_atualiza_delta:
            return valor_delta_prox

        sql = '''
            SELECT MAX(%s)
            FROM %s (nolock)
        ''' % (destino[FonteDados.DELTA_CRIACAO], destino[FonteDados.ENDERECO])

        self.logger.info('[calcula_novo_delta] Buscando novo delta[%s]' % sql)
        valor_delta_prox = database.do_db_query_fetchone(sql, conn=self.conn)[0]

        self.logger.info('[calcula_novo_delta] Novo Delta[%s]' % valor_delta_prox)
        return valor_delta_prox

    def get_delta_value_for_db(self, valor_delta, tipo_delta):
        if not valor_delta:
            return None

        try:
            tipo_delta = TipoDelta(tipo_delta)
        except:
            return None

        delta_value = None

        if TipoDelta.INT == tipo_delta:
            delta_value = valor_delta

        elif TipoDelta.DATE == tipo_delta:
            delta_value = utils.from_date_to_str(valor_delta)

        elif TipoDelta.DATETIME == tipo_delta:
            delta_value = utils.from_datetime_to_str(valor_delta)

        return delta_value

    def get_delta_value_from_db(self, valor_delta, tipo_delta):
        if not valor_delta:
            return None

        try:
            tipo_delta = TipoDelta(tipo_delta)
        except:
            return None

        delta_value = None

        if TipoDelta.INT == tipo_delta:
            delta_value = valor_delta

        elif TipoDelta.DATE == tipo_delta:
            delta_value = utils.from_str_to_date(valor_delta)

        elif TipoDelta.DATETIME == tipo_delta:
            delta_value = utils.from_str_to_datetime(valor_delta)

        return delta_value

    def get_delta_clause(self, coluna, tipo_delta, valor_delta):
        if not valor_delta:
            raise ValueError('Era esperado atribuicao de Valor de Delta!')

        where = None

        if TipoDelta.INT == tipo_delta:
            where = ' %s > %s' % (coluna, valor_delta)

        elif TipoDelta.DATE == tipo_delta:
            where = " %s >= CAST('%s' AS DATE)" % (coluna, utils.from_date_to_str(valor_delta))

        elif TipoDelta.DATETIME == tipo_delta:
            where = " %s >= CAST('%s' AS DATETIME)" % (coluna, utils.from_datetime_to_str(valor_delta))

        return where

    def get_delta_next(self, coluna, tipo_delta):
        if not (coluna and tipo_delta):
            raise ValueError('Especificacao de Delta necessario!')

        next_delta = None

        # deve ser ATUALIZADO ao fim da ingestao via MAX
        if TipoDelta.INT == tipo_delta:
            next_delta = None

        # hoje
        elif TipoDelta.DATE == tipo_delta:
            next_delta = date.today()

        # agora
        elif TipoDelta.DATETIME == tipo_delta:
            next_delta = datetime.now()

        return next_delta

    def obtem_delta_ref_prox(self, extracao_execucao_anterior, origem):
        tipo_delta_origem = origem[FonteDados.DELTA_TIPO]

        self.logger.info('[obtem_delta_ref_prox] Definindo o delta ref e proximo delta (se possivel).'
                         'Tipo[%s]' % tipo_delta_origem)

        # Valor Delta de referencia para execucao
        valor_delta_ref = None
        valor_delta_prox = None

        if tipo_delta_origem:
            # Caso haja exec anterior com sucesso, o delta ref serah o jah populado em valorProximoDelta
            if extracao_execucao_anterior:
                valor_delta_ref = self.get_delta_value_from_db(
                        extracao_execucao_anterior[ExtracaoExecucao.VALOR_PROXIMO_DELTA],
                        tipo_delta_origem)

            # Proximo Valor Delta
            valor_delta_prox = self.get_delta_next(origem[FonteDados.DELTA_CRIACAO],
                                                   TipoDelta(tipo_delta_origem))

            self.logger.info(
                    '[obtem_delta_ref_prox] Valores delta REF[%s] PROX[%s]' % (valor_delta_ref, valor_delta_prox))

        return valor_delta_ref, valor_delta_prox

    def limpa_duplicatas_delta(self, destino, id_log_carga):
        self.logger.info('[limpa_duplicatas_delta] Apos insert usando o delta, busca e limpa os possiveis registros duplicados')

        dedup = destino[FonteDados.COLUNA_DEDUP]
        endereco = destino[FonteDados.ENDERECO]
        nome = destino[FonteDados.NOME]

        if not id_log_carga:
            self.logger.info('[limpa_duplicatas_delta] Limpeza nao executada:  Nao existe valor de referencia para o delta')
            return 0

        self.logger.info('[limpa_duplicatas_delta] Limpando a tabela auxiliar')

        sqlTruncate = """
            IF OBJECT_ID('SuperStage.aux.Dedup_%s', 'U') IS NOT NULL 
                            DROP TABLE SuperStage.aux.Dedup_%s ;
        """ % (nome,nome)
        cmdRetorno = database.do_db_execute(sqlTruncate, conn=self.conn)

        self.logger.info('[limpa_duplicatas_delta] Recarregando a tabela auxiliar')

        sqlCarregaAux =  """
            SELECT tb.%s as DedupKey, MAX(idLogCarga) as maxIdLogCarga
            INTO SuperStage.aux.Dedup_%s
            FROM %s tb
            INNER JOIN (
                SELECT %s 
                FROM %s
                WHERE idLogCarga = '%s'
            ) lastLoad ON lastLoad.%s = tb.%s
            GROUP BY tb.%s ;
        """ % (dedup,nome,endereco,dedup,endereco,id_log_carga,dedup,dedup,dedup)
        cmdRetorno = database.do_db_execute(sqlCarregaAux, conn=self.conn)

        self.logger.info('[limpa_duplicatas_delta] Realiza DEDUP')

        sqlDedup =  """
            DELETE tb 
            FROM %s tb
            INNER JOIN SuperStage.aux.Dedup_%s  ddp
            ON tb.%s = ddp.DedupKey
            WHERE tb.idLogCarga <> ddp.maxIdLogCarga ;
        """ % (endereco,nome,dedup)
        cmdRetorno = database.do_db_execute(sqlDedup, conn=self.conn)


        return cmdRetorno
