import logging
import pyodbc

import urllib3

from allin.api import DadosEntregaPorDataBuilder, ErrosPermanentesBuilder, RelatorioEntregaBuilder, \
    AnaliticoEntregaBuilder, AnaliticoClickBuilder, AnaliticoAberturaBuilder
from superdigital_mdc.dw import database


# ******************************
# Funcoes AUXILIARES de negocio
# ******************************
def get_dias_a_considerar(**kwargs):
    _sql = '''
        select tx_valor
        from [Superdigital_YC].[dbo].ALLIN_PARAM
        where ALLIN_PARAM.TX_CHAVE = 'DIAS_RETORNO_ENCERRADO'
    '''
    tx_valor = database.do_db_query_fetchone(_sql, **kwargs)
    logging.info('DIAS_RETORNO_ENCERRADO [%s]' % tx_valor)
    return int(tx_valor[0]) if tx_valor and len(tx_valor) > 0 else 10


# ******************************
# Funcoes PRINCIPAIS de negocio
# ******************************
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

    def log_exec__append(self, campanha):
        self.log_exec.append({
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

    def buscar_entregas_encerradas(self, dt_ref):
        _b = DadosEntregaPorDataBuilder(dt_ref)
        infos = self.api.get_delivery_info_by_date(_b)
        return infos

    def persistir__allin_envio(self, campanha):
        self.log_exec__append(campanha)

        _params = [campanha['itensConteudo_id_campanha'],
                   campanha['itensConteudo_nm_campanha'],
                   campanha['itensConteudo_total_envio'],
                   campanha['itensConteudo_total_entregues'],
                   campanha['itensConteudo_total_abertura'],
                   campanha['itensConteudo_total_clique'],
                   campanha['itensConteudo_dt_inicio'],
                   campanha['itensConteudo_dt_encerramento'], ]

        _insert = ''' 
            INSERT INTO [Superdigital_YC].[dbo].allin_envio 
            (CD_ALLIN_CAMPANHA, NM_CAMPANHA, QT_TOTAL_ENVIO, QT_TOTAL_ENTREGUE, QT_TOTAL_ABERTURA, QT_TOTAL_CLIQUE, DT_INICIO, DT_ENCERRAMENTO)
            VALUES 
            (?, ?, ?, ?, ?, ?, CONVERT(date, ?,103), CONVERT(datetime, ?,103))
        '''

        try:
            database.do_db_execute(_insert, parameters=_params, conn=self.conn)
            self.log_exec__update(['allin_envio', True])
            return True

        except pyodbc.IntegrityError as dup:
            logging.debug('Duplicate Key ignored [allin_evento]')
            self.log_exec__update(['allin_envio', False])
            return True

        except Exception as e:
            logging.error('Falha inesperada [%s]' % e)
            self.log_exec__update(['allin_envio', False])
            self.log_exec__update(['runtime_error', e])
            self.log_exec__update(['runtime_msg_error', str(e)])

        return False

    # **********
    # ONCE
    # **********
    def execute_once(self, campanha):
        _ident = '\t\t\t'

        id_campanha_allin = campanha['itensConteudo_id_campanha']
        _is_once = self.is_cenario_once(id_campanha_allin)

        self.log_exec__update(['is_once', _is_once])

        logging.info('%sLista [%s] ONCE [%s]' % (_ident, id_campanha_allin, _is_once))
        if _is_once:
            if not (self.do_once_lista(id_campanha_allin) and
                    self.do_once_enviados(id_campanha_allin) and
                    self.do_once_bounce(id_campanha_allin)):
                raise self.log_exec_atual()['runtime_error']

        else:
            self.log_exec__update(['once_envio_lista', False])
            self.log_exec__update(('once_enviados', {'total': 0, 'sucesso': 0, 'falha': 0, 'duplicado': 0}))
            self.log_exec__update(['once_bounce', {'total': 0, 'sucesso': 0, 'falha': 0, 'duplicado': 0}])

    def is_cenario_once(self, id_campanha_allin):
        _sql = '''
            SELECT COUNT(*)
            FROM [Superdigital_YC].[dbo].allin_envio_contato
            WHERE cd_allin_campanha = %s
            GROUP BY cd_allin_campanha
        ''' % id_campanha_allin

        _count = database.do_db_query_fetchone(_sql, conn=self.conn)
        _is = True
        if _count and len(_count) > 0:
            _is = int(_count[0]) < 1

        return _is

    def do_once_lista(self, id_campanha_allin):
        _ident = '\t\t\t\t'
        logging.info('%sONCE LISTA [%s]' % (_ident, id_campanha_allin))

        b_report = RelatorioEntregaBuilder(campanha_id=id_campanha_allin)
        report = self.api.get_delivery_report(b_report)

        return self.persiste__allin_envio_lista(report)

    def persiste__allin_envio_lista(self, lista):

        _params = [lista['itensConteudo_nm_lista'],
                   lista['itensConteudo_id_campanha']]

        _insert = ''' 
            INSERT INTO [Superdigital_YC].[dbo].allin_envio_lista 
            (nm_lista, cd_allin_campanha)
            VALUES 
            (?, ?)
        '''

        try:
            database.do_db_execute(_insert, parameters=_params, conn=self.conn)
            self.log_exec__update(['once_envio_lista', True])

            return True

        except Exception as e:
            logging.error('FALHA persiste__allin_envio_lista [%s]' % e)
            self.log_exec__update(['once_envio_lista', False])
            self.log_exec__update(['runtime_error', e])
            self.log_exec__update(['runtime_msg_error', e])

        return False

    def do_once_enviados(self, id_campanha_allin):
        _ident = '\t\t\t\t'
        logging.info('%sONCE ENVIADOS [%s]' % (_ident, id_campanha_allin))

        b_analytic = AnaliticoEntregaBuilder(campanha_id=id_campanha_allin)
        analytic = self.api.get_delivery_analytic(b_analytic)

        self.log_exec__update(['once_enviados', {'total': 0, 'sucesso': 0, 'falha': 0, 'duplicado': 0}])

        for idx, entrega in enumerate(analytic):
            logging.debug('%s entrega %s/%s' % (_ident, idx + 1, analytic.count()))
            self.persiste__allin_envio_contato(entrega)
            self.log_exec_atual()['once_enviados']['total'] = analytic.count()

            is_time_for_partial_commit = idx % self.PARTIAL_COMMIT_INDEX == 0 and idx > 0
            if is_time_for_partial_commit:
                logging.info('<<commit parcial em %s/%s>>' % (idx + 1, analytic.count()))
                database.db_commit(self.conn)

        return True

    def persiste__allin_envio_contato(self, enviado):
        _params = [enviado['email'],
                   enviado['id_campaign'],
                   enviado['status']]

        _insert = ''' 
            INSERT INTO [Superdigital_YC].[dbo].allin_envio_contato
            (tx_email, cd_allin_campanha, IN_STATUS)
            VALUES 
            (?, ?, ?)
        '''

        _count = self.log_exec_atual()['once_enviados']
        try:
            database.do_db_execute(_insert, parameters=_params, conn=self.conn)
            _count['sucesso'] = _count['sucesso'] + 1

        except pyodbc.IntegrityError as dup:
            logging.debug('Duplicate Key ignored [allin_envio_contato]')
            _count['duplicado'] = _count['duplicado'] + 1

        except Exception as e:
            _count['falha'] = _count['falha'] + 1
            logging.error('FALHA persiste__allin_envio_contato [%s]' % e)

        finally:
            return True

    def do_once_bounce(self, id_campanha_allin):
        _ident = '\t\t\t\t'
        logging.info('%sONCE BOUNCE [%s]' % (_ident, id_campanha_allin))

        self.log_exec__update(['once_bounce', {'total': 0, 'sucesso': 0, 'falha': 0, 'duplicado': 0}])

        _b = ErrosPermanentesBuilder(campanha_id=id_campanha_allin, pagination_offset=200)
        pages = self.api.get_permanent_errors(_b)

        _count = 0
        for idx, bounce in enumerate(pages):
            logging.debug('%s bounce %s/%s' % (_ident, idx + 1, pages.count()))

            self.persiste__allin_envio_contato_bounce(bounce, id_campanha_allin)
            _count += 1

            is_time_for_partial_commit = idx % self.PARTIAL_COMMIT_INDEX == 0 and idx > 0
            if is_time_for_partial_commit:
                logging.info('<<commit parcial em %s/%s>>' % (idx + 1, pages.count()))
                database.db_commit(self.conn)

        self.log_exec_atual()['once_bounce']['total'] = _count

        return True

    def persiste__allin_envio_contato_bounce(self, bounce, id_campanha_allin):
        _params = [bounce['itensConteudo_nm_email'],
                   id_campanha_allin,
                   bounce['itensConteudo_msg']]

        _insert = ''' 
            INSERT INTO [Superdigital_YC].[dbo].allin_envio_contato_bounce 
            (tx_email, cd_allin_campanha, tx_erro)  
            VALUES
            (?, ?, ?)
        '''

        _count = self.log_exec_atual()['once_bounce']
        try:
            database.do_db_execute(_insert, parameters=_params, conn=self.conn)
            _count['sucesso'] = _count['sucesso'] + 1

        except pyodbc.IntegrityError as dup:
            logging.debug('Duplicate Key ignored [allin_envio_contato_bounce]')
            _count['duplicado'] = _count['duplicado'] + 1

        except Exception as e:
            _count['falha'] = _count['falha'] + 1
            logging.error('FALHA persiste__allin_envio_contato_bounce [%s]' % e)

        finally:
            return True

    # **********
    # ALWAYS
    # **********
    def execute_always(self, campanha):
        _ident = '\t\t\t'

        id_campanha_allin = campanha['itensConteudo_id_campanha']

        logging.info('%sLista [%s] ALWAYS' % (_ident, id_campanha_allin))

        self.do_always_clique(id_campanha_allin)
        self.do_always_abertura(id_campanha_allin)

    def do_always_clique(self, id_campanha_allin):
        _ident = '\t\t\t\t'
        logging.info('%sALWAYS CLIQUE [%s]' % (_ident, id_campanha_allin))

        self.log_exec__update(['always_click', {'total': 0, 'sucesso': 0, 'falha': 0, 'duplicado': 0}])
        self.log_exec__update(['contato_clique', {'total': 0, 'sucesso': 0, 'falha': 0}])

        _b = AnaliticoClickBuilder(campanha_id=id_campanha_allin, pagination_offset=1000)
        pages = self.api.get_click_analytic(_b)

        for idx, clique in enumerate(pages):
            logging.debug('%s clique %s/%s (estimado)' % (_ident, idx + 1, pages.count()))
            self.persiste__allin_evento_contato_clique(clique)
            self.persiste__email_contato_clique(clique)

            is_time_for_partial_commit = idx % self.PARTIAL_COMMIT_INDEX == 0 and idx > 0
            if is_time_for_partial_commit:
                logging.info('<<commit parcial em %s/%s>>' % (idx + 1, pages.count()))
                database.db_commit(self.conn)

    def persiste__allin_evento_contato_clique(self, clique):
        _params = [clique['itensConteudo_nm_email'],
                   clique['itensConteudo_id_campanha'],
                   clique['itensConteudo_dt_click'],
                   clique['itensConteudo_dt_click'],
                   clique['itensConteudo_nm_link']]

        _insert = ''' 
            INSERT INTO [Superdigital_YC].[dbo].allin_envio_contato_clique 
            (tx_email, cd_allin_campanha, dt_clique, dt_clique_texto, tx_link)  
            VALUES
            (%s)
        ''' % ('?,' * len(_params))[:-1]

        _count = self.log_exec_atual()['always_click']
        try:
            database.do_db_execute(_insert, parameters=_params, conn=self.conn)
            _count['sucesso'] = _count['sucesso'] + 1

        except pyodbc.IntegrityError as dup:
            logging.debug('Duplicate Key ignored [allin_envio_contato_clique]')
            _count['duplicado'] = _count['duplicado'] + 1

        except Exception as e:
            _count['falha'] = _count['falha'] + 1
            logging.error('FALHA persiste__allin_evento_contato_clique [%s]' % e)

        finally:
            _count['total'] = _count['total'] + 1

        return True

    def persiste__email_contato_clique(self, clique):
        _upsert = """
            MERGE [Superdigital_YC].[dbo].MDC_email_contato AS [Target] 
            USING (SELECT 
                        ? AS [tx_email], 
                        ? AS [cd_allin_campanha_clique],
                        ? AS [dt_clique]
                ) AS [Source]  ON [Target].tx_email = [Source].tx_email

            WHEN MATCHED THEN 
                UPDATE SET 
                    [Target].[dt_clique] = [Source].[dt_clique],
                    [Target].[cd_allin_campanha_clique] = [Source].[cd_allin_campanha_clique]

            WHEN NOT MATCHED THEN 
                INSERT ([tx_email], [dt_clique], [cd_allin_campanha_clique]) 
                VALUES ([Source].[tx_email], [Source].[dt_clique],  [Source].[cd_allin_campanha_clique]);
        """  # ponto-e-virgula necessario!

        _params = [clique['itensConteudo_nm_email'],
                   clique['itensConteudo_id_campanha'],
                   clique['itensConteudo_dt_click']]

        _count = self.log_exec_atual()['contato_clique']
        try:
            database.do_db_execute(_upsert, parameters=_params, conn=self.conn)
            _count['sucesso'] = _count['sucesso'] + 1

        except Exception as e:
            _count['falha'] = _count['falha'] + 1
            logging.error('FALHA persiste__contato_clique [%s]' % e)

        finally:
            _count['total'] = _count['total'] + 1

        return True

    def do_always_abertura(self, id_campanha_allin):
        _ident = '\t\t\t\t'
        logging.info('%sALWAYS ABERTURA [%s]' % (_ident, id_campanha_allin))

        self.log_exec__update(['always_abertura', {'total': 0, 'sucesso': 0, 'falha': 0, 'duplicado': 0}])
        self.log_exec__update(['contato_abertura', {'total': 0, 'sucesso': 0, 'falha': 0}])

        _b = AnaliticoAberturaBuilder(campanha_id=id_campanha_allin, pagination_offset=200)
        pages = self.api.get_opening_analytic(_b)

        for idx, abertura in enumerate(pages):
            logging.debug('%s abertura %s/%s' % (_ident, idx + 1, pages.count()))
            self.persiste__allin_allin_envio_contato_abertura(abertura)
            self.persiste__email_contato_abertura(abertura)

            is_time_for_partial_commit = idx % self.PARTIAL_COMMIT_INDEX == 0 and idx > 0
            if is_time_for_partial_commit:
                logging.info('<<commit parcial em %s/%s>>' % (idx + 1, pages.count()))
                database.db_commit(self.conn)

    def persiste__allin_allin_envio_contato_abertura(self, abertura):
        _params = [abertura['itensConteudo_nm_email'],
                   abertura['itensConteudo_id_campanha'],
                   abertura['itensConteudo_dt_view'],
                   abertura['itensConteudo_dt_view']]

        _insert = ''' 
            INSERT INTO [Superdigital_YC].[dbo].allin_envio_contato_abertura 
            (tx_email, CD_ALLIN_CAMPANHA, dt_view, DT_VIEW_TEXTO)
            VALUES
            (%s)
        ''' % ('?,' * len(_params))[:-1]

        _count = self.log_exec_atual()['always_abertura']
        try:
            database.do_db_execute(_insert, parameters=_params, conn=self.conn)
            _count['sucesso'] = _count['sucesso'] + 1

        except pyodbc.IntegrityError as dup:
            logging.debug('Duplicate Key ignored [allin_envio_contato_abertura]')
            _count['duplicado'] = _count['duplicado'] + 1

        except Exception as e:
            _count['falha'] = _count['falha'] + 1
            logging.error('FALHA persiste__allin_allin_envio_contato_abertura [%s]' % e)

        finally:
            _count['total'] = _count['total'] + 1

        return True

    def persiste__email_contato_abertura(self, abertura):
        _upsert = """
            MERGE [Superdigital_YC].[dbo].MDC_email_contato AS [Target] 
            USING (SELECT 
                        ? AS [tx_email], 
                        ? AS [cd_allin_campanha_abertura],
                        ? AS [dt_abertura]
                ) AS [Source]  ON [Target].tx_email = [Source].tx_email

            WHEN MATCHED THEN 
                UPDATE SET 
                    [Target].[dt_abertura] = [Source].[dt_abertura],
                    [Target].[cd_allin_campanha_abertura] = [Source].[cd_allin_campanha_abertura]

            WHEN NOT MATCHED THEN 
                INSERT ([tx_email], [dt_abertura], [cd_allin_campanha_abertura]) 
                VALUES ([Source].[tx_email], [Source].[dt_abertura],  [Source].[cd_allin_campanha_abertura]);
        """  # ponto-e-virgula necessario!

        _params = [abertura['itensConteudo_nm_email'],
                   abertura['itensConteudo_id_campanha'],
                   abertura['itensConteudo_dt_view']]

        _count = self.log_exec_atual()['contato_abertura']
        try:
            database.do_db_execute(_upsert, parameters=_params, conn=self.conn)
            _count['sucesso'] = _count['sucesso'] + 1

        except Exception as e:
            _count['falha'] = _count['falha'] + 1
            logging.error('FALHA persiste__contato_abertura [%s]' % e)

        finally:
            _count['total'] = _count['total'] + 1

        return True

    def notificar_erro(self, e):
        self.log_exec__update(['runtime_error', e])
        self.log_exec__update(['runtime_msg_error', str(e)])
