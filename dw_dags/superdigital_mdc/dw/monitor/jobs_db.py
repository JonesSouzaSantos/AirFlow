import logging
from contextlib import closing
from datetime import datetime, timedelta

import pysftp

import superdigital_mdc.dw.database as database
import superdigital_mdc.dw.utils as utils
from allin.api_v2 import SMSBuilder
from superdigital_de.di.repositorios import ParametroOperacional
from superdigital_mdc.dw import airflow_utils
from superdigital_mdc.dw.airflow_utils import apiv2__as_is


@utils.time_this
def expurge__holos_dashboarddevendas(**context):
    with closing(database.db_connect(**context)) as conn:
        bo = BusinessObject(conn)
        bo.execute_db(bo.get_expurge_holos_dashboarddevendas())
        bo.commit()
    return True


@utils.time_this
def call__holos_dashboarddevendas(**context):
    with closing(database.db_connect(**context)) as conn:
        bo = BusinessObject(conn)
        #bo.execute_db(bo.get_sql_create_temp_table())
        bo.execute_db(bo.get_sql_holos_dashboarddevendas_new())
        #bo.execute_db(bo.get_sql_drop_temp_table())
        bo.commit()
    return True


@utils.time_this
def assert__holos_dashboarddevendas(**context):
    with closing(database.db_connect(**context)) as conn:
        bo = BusinessObject(conn)
        bo.assert_holos_dashboarddevendas()


@utils.time_this
def ftp__holos_dashboarddevendas(**context):
    with closing(database.db_connect(**context)) as conn:
        bo = BusinessObject(conn)
        bo.ftp_holos_dashboarddevendas()


@apiv2__as_is
def notificar_interessados__holos_dashboarddevendas(**context):
    bo = BusinessObject()

    api_v2 = airflow_utils.get_api_from_context(**context)
    return bo.informar_interessados(api_v2)


class BusinessObject(utils.AbstractBusinessObject):
    DB__DB_NAME = 'Superdigital_Work'
    DB__HOLOS_DASHBOARDDEVENDAS = '%s.dbo.MDC_holos_dashboarddevendas' % DB__DB_NAME
    FTP__HOLOS_DASHBOARDDEVENDAS = '%s%s' % (utils.Constantes.DEF__FILESYSTEM_FOLDER, 'Dashboarddevendas_%s.txt')
    DB__TB_CONTAPRODUTOALAVANCA = '#ContaProdutoAlavanca'

    def get_expurge_holos_dashboarddevendas(self):
        return '''
            DELETE FROM %s 
            WHERE dt_ref < GETDATE()-30
                    or dt_ref = CAST(GETDATE() AS DATE)
        ''' % self.DB__HOLOS_DASHBOARDDEVENDAS

    def get_sql_create_temp_table(self):
        list_numdbm_pj = ParametroOperacional.find_ValorParametro_by_id(
                ParametroOperacional.P__HOLOS_DASHBOARDVENDAS_NUMDBM_PJ, conn=self.conn)

        _stm = """
            SELECT  A.ContaCorrenteID
                    ,D.CPF_CNPJ
                    ,T.CanalAgrupado
                    ,CASE
                        WHEN(B.MarcacoesOrigemID = 2356)                    THEN 'Coletivo - Hinode'
                        WHEN(B.MarcacoesOrigemID = 2099)                    THEN 'Incl. Financ. - Superget'
                        WHEN(B.Produto = 'MicroCredito' or
                             B.Dominio in('MicroCredito', 'Natura'))        THEN 'Natura'
                        WHEN(B.Dominio = 'Venda na Agencia' and    
                             A.ProdutoAgrupadoAtual <> 'PF')                THEN 'Comercial Rede'
                        WHEN(A.ProdutoAgrupadoAtual <> 'PF')                THEN 'Comercial Super'
                        ELSE 'Gestao Online'
                    END AS Alavanca_new
                    ,CASE 
                        WHEN(C.ContaIdPF IS NOT NULL)                       THEN 'FOPA-TEMP.'
                        WHEN(B.Produto = 'MicroCredito' AND
                             B.Dominio in('MicroCredito', 'Natura'))        THEN 'Natura'
                        ELSE a.ProdutoAgrupadoAtual
                    END AS Produto_Final_new
            INTO %s
            FROM [Superdigital].[dbo].[Conta] A (nolock)
                LEFT JOIN (SELECT DISTINCT CONTAID
                                          ,Produto
                                          ,Canal
                                          ,Dominio
                                          ,MarcacoesOrigemID
                                from Superdigital_ETL.[dbo].[CanalParceiros] (nolock)) B ON(A.ContaCorrenteID = B.ContaID)
                LEFT JOIN Superdigital_Work.dbo.MDC_TAB_CANAL T (nolock) ON(B.CANAL = T.CANAL)
                LEFT JOIN (SELECT * 
                            FROM [Superdigital].[dbo].[RelacaoEmpresa] A (NOLOCK) 
                            WHERE [Status] = 1 
                                  AND NUMDBM_PJ IN(%s)) C ON(A.ContaCorrenteID = C.ContaIdPF)
                LEFT JOIN (SELECT DISTINCT NUMDBM, CPF_CNPJ
                            FROM Superdigital.dbo.DBM_CADASTRO (nolock)) D on(a.NUMDBM = D.NUMDBM)
            WHERE A.ContaPaiId IS NULL
                  AND A.TipoConta = 'P'
                  AND A.StatusContaCodigo IN ('A','B','C')    
                  AND A.ProdutoAgrupadoAtual IS NOT NULL
                  AND A.NUMDBM IS NOT NULL
                  AND A.SegmentoDescricao NOT LIKE '%%precada%%' 
                  AND A.ProdutoInicial <> 'Onibus'
                  AND DATEDIFF(D, CONVERT(DATE, A.DataAberturaConta), CONVERT(DATE,GETDATE())) > 0
                  AND (CONVERT(VARCHAR(6), CONVERT(DATETIME, A.dataAberturaConta), 112) = CONVERT(VARCHAR(6), CONVERT(DATETIME, GETDATE()-1), 112)
                       OR CONVERT(VARCHAR(6), CONVERT(DATETIME, A.dataAtivacaoConta), 112) = CONVERT(VARCHAR(6), CONVERT(DATETIME, GETDATE()-1), 112))
        """ % (self.DB__TB_CONTAPRODUTOALAVANCA, list_numdbm_pj)

        return _stm

    def get_sql_drop_temp_table(self):
        return " DROP TABLE %s " % self.DB__TB_CONTAPRODUTOALAVANCA

    def get_sql_holos_dashboarddevendas(self):
        return """
            INSERT INTO %s
            (ContaCorrenteID, ContaIDCripto, DataAtivacao, Status, DescricaoStatus, DataCriacaoDaConta, CestaComercial, 
                Canal, DescricaoCanal, DescricaoProduto, DescricaoSegmento, DescricaoProdutoAtual, 
                DescricaoSegmentoAtual, TipoConta, dt_ref)

                SELECT  A.ContaCorrenteID                                               AS ContaCorrenteID
                        , isnull(D.CPF_CNPJ, '99999999999')                             AS ContaIDCripto
                        , CONVERT(DATE, a.DataAtivacaoConta)                            AS DataAtivacao
                        , A.StatusContaCodigo                                           AS 'Status'
                        , A.StatusContaDescricao                                        AS DescricaoStatus
                        , CONVERT(DATE, A.DataAberturaConta)                            AS DataCriacaoDaConta
                        , A.AssinaturaDescricao                                         AS CestaComercial
                        , COALESCE(D.CanalAgrupado, 'Não encontrado')                   AS Canal
                        , A.CanalAquisicaoDescricao                                     AS DescricaoCanal
                        , D.Alavanca_new                                                AS DescricaoProduto
                        , A.SegmentoDescricao                                           AS DescricaoSegmento
                        , D.Produto_Final_new                                           AS DescricaoProdutoAtual
                        , ''                                                            AS DescricaoSegmentoAtual
                        , A.TipoConta                                                   AS TipoConta
                        , CAST(GETDATE() AS DATE)                                       AS dt_ref
                FROM Superdigital.dbo.Conta A (nolock)
                    LEFT JOIN Superdigital_Work.dbo.ContasCorrentesCripto B (nolock) ON (A.ContaCorrenteID = B.ContaID)
                    LEFT JOIN %s D on(A.ContaCorrenteID = D.ContaCorrenteID)
                WHERE A.ContaPaiId IS NULL
                        AND A.TipoConta = 'P'
                        AND A.StatusContaCodigo IN ('A','B','C')
                        AND A.ProdutoAgrupadoAtual IS NOT NULL
                        AND A.NUMDBM IS NOT NULL
                        AND A.SegmentoDescricao NOT LIKE '%%precada%%'
                        AND A.ProdutoInicial <> 'Onibus'
                        AND DATEDIFF(D, CONVERT(DATE, A.DataAberturaConta), CONVERT(DATE,GETDATE())) > 0
                        AND (CONVERT(VARCHAR(6), CONVERT(DATETIME, A.dataAberturaConta), 112) = CONVERT(VARCHAR(6), CONVERT(DATETIME, GETDATE()-1), 112)
                             OR CONVERT(VARCHAR(6), CONVERT(DATETIME, A.dataAtivacaoConta), 112) = CONVERT(VARCHAR(6), CONVERT(DATETIME, GETDATE()-1), 112))
        """ % (self.DB__HOLOS_DASHBOARDDEVENDAS, self.DB__TB_CONTAPRODUTOALAVANCA)

    #Ajuste para ler os dados do 10.1.50.85
    def get_sql_holos_dashboarddevendas_new(self):
        return """INSERT INTO Superdigital_Work.dbo.MDC_holos_dashboarddevendas 
                (ContaCorrenteID, ContaIDCripto, DataAtivacao, Status, DescricaoStatus, DataCriacaoDaConta, CestaComercial, 
                Canal, DescricaoCanal, DescricaoProduto, DescricaoSegmento, DescricaoProdutoAtual, 
                DescricaoSegmentoAtual, TipoConta, dt_ref)
                select distinct
                         B.ContaCorrenteID 
                        ,isnull(E.CPF_CNPJ, '99999999999') AS ContaIDCripto
                        ,CONVERT(DATE, B.DataAtivacaoConta) AS DataAtivacao
                        ,B.StatusContaCodigo AS Status
                        ,X.StatusContaDescricao AS DescricaoStatus
                        ,CONVERT(DATE, B.DataAberturaConta) AS DataCriacaoDaConta
                        ,F.AssinaturaDescricaoMacro AS CestaComercial
                        ,T.CanalAgrupado AS Canal
                        ,T.Dominio AS DescricaoCanal
                        ,CASE
                                WHEN(G.Natura = 1 or G.hinode = 1 or G.Superget = 1) then 'Parcerias'
                                WHEN(G.Funcionalidade in('FOPA','EX-FOPA','Conta PJ FOPA')) then 'Comercial Super'
                                WHEN(G.Funcionalidade in('Conta PF','MEI')) then 'Gestao Online'
                                ELSE 'n/a'
                        END as DescricaoProduto
                        ,B.SegmentoDescricao AS DescricaoSegmento
                        ,CASE
                                WHEN(G.funcionalidade = 'FOPA') THEN 'FOPA' 
                                WHEN(G.funcionalidade = 'Conta PF') THEN 'PF' 
                                WHEN(G.funcionalidade = 'MEI') THEN 'MEI' 
                                WHEN(G.funcionalidade = 'EX-FOPA') THEN 'EX-FOPA' 
                                WHEN(G.funcionalidade = 'Conta PJ FOPA') THEN 'PJ' 
                                ELSE 'n/a'
                        END AS DescricaoProdutoAtual
                        ,'' AS DescricaoSegmentoAtual
                        ,X.TipoConta AS TipoConta
                        ,CAST(GETDATE() AS DATE)  as dt_ref
                from SuperDW.corp.Conta B (nolock)
                    left join (SELECT DISTINCT NUMDBM, CPF_CNPJ
                                FROM SuperDW.corp.cadastro (nolock)) E   on(B.NUMDBM = E.NUMDBM)
                    left join Superdigital_Work.dbo.MDC_TAB_ASSINATURA F        ON(F.AssinaturaId = B.AssinaturaId)
                    left join SuperStage.raw.FuncionalidadeAtual G              ON(B.ContaCorrenteID = G.ContaCorrenteID)
                    left join Superdigital_Work.dbo.MDC_TAB_CANAL T             ON(G.CanalID = T.CANAL)
                    left join (select ContaCorrenteID 
                                    ,StatusContaDescricao
                                    ,TipoConta
                                    from SuperDW.corp.Conta (nolock)) X ON(B.ContaCorrenteID = X.ContaCorrenteID)
                Where G.Funcionalidade in('Conta PF','MEI')
                    and G.Natura = 0
                    and G.Hinode = 0
                    and G.Superget = 0
                    and B.ContaPaiId IS NULL
                    AND B.TipoConta = 'P'
                    AND B.StatusContaCodigo IN ('A','B','C')
                    AND B.NUMDBM IS NOT NULL
                    AND DATEDIFF(D, CONVERT(DATE, B.DataAberturaConta), CONVERT(DATE,GETDATE())) > 0
                    AND (CONVERT(VARCHAR(6), CONVERT(DATETIME, B.dataAberturaConta), 112) = CONVERT(VARCHAR(6), CONVERT(DATETIME, GETDATE()-1), 112)
                        OR CONVERT(VARCHAR(6), CONVERT(DATETIME, B.dataAtivacaoConta), 112) = CONVERT(VARCHAR(6), CONVERT(DATETIME, GETDATE()-1), 112)) """

    def assert_holos_dashboarddevendas(self):
        logging.info('[assert_holos_dashboarddevendas] Executando count em %s' % self.DB__HOLOS_DASHBOARDDEVENDAS)

        _stm = 'SELECT count(*) FROM %s WHERE dt_ref = CAST(GETDATE() AS DATE)' % self.DB__HOLOS_DASHBOARDDEVENDAS
        _count = database.do_db_query_fetchone(_stm, conn=self.conn)
        logging.info('\t[assert_holos_dashboarddevendas] Count[%s]' % _count)
        assert _count[0] > 1

    def ftp_holos_dashboarddevendas(self):
        logging.info('[ftp_holos_dashboarddevendas] Inicio')

        _fullname = self.FTP__HOLOS_DASHBOARDDEVENDAS % (datetime.now().strftime('%Y%m%d_%H%M%S'))
        _name = _fullname.split('/')[-1]

        self._create_ftp_file(_fullname)

        logging.info(
                '\t[ftp_holos_dashboarddevendas] Conectando com host:port [%s:%s]' % (
                    utils.Constantes.DEF__HOLOS_FTP_HOST,
                    utils.Constantes.DEF__HOLOS_FTP_PORT))

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection(utils.Constantes.DEF__HOLOS_FTP_HOST,
                               username=utils.Constantes.DEF__HOLOS_FTP_USER,
                               password=utils.Constantes.DEF__HOLOS_FTP_PASS,
                               port=utils.Constantes.DEF__HOLOS_FTP_PORT,
                               cnopts=cnopts) as sftp:
            with sftp.cd(utils.Constantes.DEF__HOLOS_FTP_HOMEDIR):
                logging.info('\t[ftp_holos_dashboarddevendas] Subindo arquivo [%s]' % _name)
                sftp.put(_fullname)

        logging.info('\t[ftp_holos_dashboarddevendas] SUCESSO!')

        return True

    def _create_ftp_file(self, filename):
        logging.info('[_create_ftp_file] Criando arquivo %s' % filename)
        _stm = """
            SELECT ContaIDCripto
                    , DataAtivacao 
                    , Status 
                    , DescricaoStatus 
                    , DataCriacaoDaConta 
                    , CestaComercial 
                    , Canal 
                    , DescricaoCanal 
                    , DescricaoProduto 
                    , DescricaoSegmento 
                    , DescricaoProdutoAtual 
                    , DescricaoSegmentoAtual 
                    , TipoConta 
            FROM %s 
            WHERE dt_ref = CAST(GETDATE() AS DATE)
        """ % self.DB__HOLOS_DASHBOARDDEVENDAS

        df = database.do_db_query_2_pandas(_stm, conn=self.conn)

        import hashlib
        import pandas as pd
        assert isinstance(df, pd.DataFrame)

        # ContaIDCripto eh um CPF que DEVE ser encriptado com MD5
        df['ContaIDCripto'] = df['ContaIDCripto'].apply(lambda x:
                                                        hashlib.md5('{}.{}.{}-{}'.format(str(x)[:3],
                                                                                         str(x)[3:6],
                                                                                         str(x)[6:9],
                                                                                         str(x)[9:]).encode())
                                                        .hexdigest().upper()).astype(str)

        df.to_csv(path_or_buf=filename, sep=';', index=False, encoding='UTF-8', doublequote=False)

        logging.info('\t[_create_ftp_file] SUCESSO!')

        return True

    def informar_interessados(self, api_v2):

        interessados = [{'nome': 'Marcelo', 'nm_celular': '5511982819820'},
                        {'nome': 'Raul', 'nm_celular': '556181571970'},
                        {'nome': 'Arthur', 'nm_celular': '5511983947959'},
                        {'nome': 'Jeferson', 'nm_celular': '5511947728309'},
                        {'nome': 'Leonardo', 'nm_celular': '5511988315149'},
                        {'nome': 'Luciano', 'nm_celular': '5511976461112'},
                        {'nome': 'Francisco', 'nm_celular': '5511999355902'},
                        ]

        dh_envio = datetime.now() + timedelta(seconds=10)
        data, hora = str(dh_envio).split()

        msg = 'Envio do arquivo Dashboarddevendas Holos do ' \
              'dia %s realizado com sucesso!' % dh_envio.strftime('%d/%m/%Y')

        b = SMSBuilder() \
            .set_nm_envio('[HOLOS] Dashboarddevendas') \
            .set_nm_mensagem(msg) \
            .set_dt_envio(data) \
            .set_hr_envio(hora[:8])

        all_success = True

        for interessado in interessados:
            b.set_nm_celular(interessado['nm_celular'])

            _ret = None
            _msg_ret = ''
            try:
                _ret = api_v2.send_sms(b)
            except Exception as e:
                all_success = False

            if _ret:
                logging.info('%s notificado!' % interessado['nome'])
            else:
                logging.error('%s NAO notificado!' % interessado['nome'])

        return all_success
