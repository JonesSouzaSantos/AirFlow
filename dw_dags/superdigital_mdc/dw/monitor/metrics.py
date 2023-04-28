import logging
from contextlib import closing

import superdigital_mdc.dw.database as database
import superdigital_mdc.dw.utils as utils

@utils.time_this
def prepara_repositorio_carga_duracao(**context):
    with closing(database.db_connect(**context)) as conn:
        bo = BusinessObject(conn)
        bo.expurgo_carga_duracao()
        database.db_commit(conn)

        # bo.assert_tabela_vazia()

    return True


@utils.time_this
def calcula_indicadores_D1(**context):
    with closing(database.db_connect(**context)) as conn:
        bo = BusinessObject(conn)
        bo.insert_carga_duracao('COALESCE(a.inicio, GETDATE()-1)', 2, 1)
        database.db_commit(conn)

    return True


@utils.time_this
def calcula_indicadores_D7(**context):
    with closing(database.db_connect(**context)) as conn:
        bo = BusinessObject(conn)
        bo.insert_carga_duracao('getdate()-7', 7, 0)
        database.db_commit(conn)

    return True


@utils.time_this
def calcula_indicadores_D30(**context):
    with closing(database.db_connect(**context)) as conn:
        bo = BusinessObject(conn)
        bo.insert_carga_duracao('getdate()-30', 30, 0)
        database.db_commit(conn)

    return True


class BusinessObject(object):
    __DB_NAME = '[Superdigital_work]'
    __TABLE_NAME = '%s.[dbo].[MDC_carga_duracao]' % __DB_NAME

    def __init__(self, conn):
        self.conn = conn

        logging.basicConfig(level=logging.INFO)  # ERROR #INFO #DEBUG

    def remover_carga_duracao(self):
        logging.info('[remover_carga_duracao] Dropando tabela %s' % self.__TABLE_NAME)

        _stm = '''
                USE %s;
                DROP TABLE %s;
        ''' % (self.__DB_NAME, self.__TABLE_NAME)

        database.do_db_execute(_stm, conn=self.conn)

        logging.info('\t[remover_carga_duracao] SUCESSO')

        return True

    def criar_carga_duracao(self):
        _stm = '''
                CREATE TABLE Superdigital_Work.[dbo].[MDC_carga_duracao]
                (
                    [chave_watcher] [varchar](70),
                    [dt_referencia] [date],
                    [idWatcher] [int],
                    [nome] [varchar](100) NULL,
                    [minima] [float] NULL,
                    [media] [float] NULL,
                    [maxima] [float] NULL,
                    [universo] [int] NULL
                    CONSTRAINT [PK_mdc_carga_duracao] PRIMARY KEY CLUSTERED 
                    (
                        [chave_watcher] ASC, dt_referencia ASC
                    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                ) ON [PRIMARY]
        ''' % self.__TABLE_NAME

        database.do_db_execute(_stm, conn=self.conn)

        return True

    def truncar_carga_duracao(self):
        logging.info('[truncar_carga_duracao] Truncando tabela %s' % self.__TABLE_NAME)

        _stm = 'TRUNCATE TABLE %s' % self.__TABLE_NAME

        database.do_db_execute(_stm, conn=self.conn)

        logging.info('\t[truncar_carga_duracao] SUCESSO')

        return True

    def expurgo_carga_duracao(self):
        logging.info('[expurgo_carga_duracao] Expurgo tabela %s' % self.__TABLE_NAME)

        _stm = 'DELETE FROM %s WHERE dt_referencia  < GETDATE() - 90' % self.__TABLE_NAME

        database.do_db_execute(_stm, conn=self.conn)

        logging.info('\t[expurgo_carga_duracao] SUCESSO')

        return True

    def insert_carga_duracao(self, str_dt_ref, ini, fim):
        logging.info('[insert_carga_duracao] Inserindo dados [data: %s] [ini: %s] [fim: %s]' % (str_dt_ref, ini, fim))

        _stm = '''
                INSERT INTO %s
                    ([chave_watcher], [dt_referencia], [idWatcher], [nome], [minima], [media], [maxima], [universo] )
                (
                    SELECT chave_watcher
                            , dt_referencia 
                            , idWatcher
                            , nome
                            , min(duration_min) as minima 
                            , avg(duration_min) as media 
                            , max(duration_min) as maxima 
                            , count(duration_min) as universo
                    FROM (
                        SELECT a.idWatcher
                                , CASE
                                    WHEN a.parametro <> '' THEN CONCAT(a.idWatcher, '|', a.parametro)
                                    ELSE cast(a.idWatcher as varchar)
                                END AS chave_watcher
                                , b.nome
                                , CAST(DATEDIFF(minute,a.inicio,a.termino) AS float) AS duration_min
                                , CAST(%s AS DATE) AS dt_referencia
                        FROM Superdigital._meta.watcher_evento a (nolock)
                        INNER JOIN Superdigital._meta.watcher b (nolock)
                            ON a.idWatcher = b.idWatcher 
                        WHERE CAST(a.registro AS DATE) BETWEEN GETDATE()-%s AND GETDATE()-%s
                    ) A
                    GROUP BY chave_watcher, dt_referencia, idWatcher, nome
                )
        ''' % (self.__TABLE_NAME, str_dt_ref, ini, fim)

        database.do_db_execute(_stm, conn=self.conn)

        logging.info('\t[insert_carga_duracao] SUCESSO')

        return True

    def assert_tabela_vazia(self):
        logging.info('[assert_tabela_vazia] Verifica se tabela %s esta vazia' % self.__TABLE_NAME)
        _stm = 'SELECT count(*) as total FROM %s' % self.__TABLE_NAME

        _count = database.do_db_query_fetchone(_stm, conn=self.conn)
        if _count:
            _tot = int(_count[0])
            logging.info('\t[assert_tabela_vazia] Total encontrado [%s]' % _tot)
            assert _tot < 1

        return True
