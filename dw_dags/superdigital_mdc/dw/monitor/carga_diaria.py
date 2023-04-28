
import os

from datetime import date, datetime

from contextlib import closing

from superdigital_mdc.dw.database import *
from superdigital_mdc.dw.utils import time_this

from superdigital_mdc.dw.utils import Constantes


# Os processos de cleansing sao agendados de acordo com a origem dos dados. Uma origem de ETL pode ter uma ou mais
# origens no banco "Superdigital". As origens superdigital ficam armazenadas na tabela [Superdigital]._meta.origem.
ORIGENS_SUPER = {'1001': 'Cadastro_PF',
                 '1002': 'Cadastro_PJ',
                 '1003': 'Email_PF',
                 '1004': 'Email_PJ',
                 '1005': 'Email_Conta_PF',
                 '1006': 'Email_Conta_PJ',
                 '1007': 'Cadastro_Santander'}

# O watcher 204 eh chamado 4 vezes para cada origem de dados, uma para cada idParametro encontrado
# na tabela [Superdigital].aux.cleansing_parametros:
PARAMS_CLEANSING = {1: 'Nome',
                    2: 'Endereco',
                    3: 'Telefone',
                    4: 'Email'}

# Os watchers que compoe a subdag de processamento (DBM) e estao ativos
# sao os que compoe a lista abaixo.
PIPES_PROCESSAMENTO = [209,
                       214,
                       215,
                       216,
                       217,
                       218,
                       219,
                       220,
                       221,
                       222,
                       223,
                       224,
                       231,
                       229,
                       225,
                       226,
                       232,
                       233,
                       235,
                       234,
                       236]

WATCHERS_PROCESSAMENTO = {209: "Dimensoes_DBM",
                          214: "Gera_NUMDBM",
                          215: "DBM_Cadastro",
                          216: "Contas_Correntes",
                          217: "Cartoes",
                          218: "Lancamentos",
                          219: "Transferencias",
                          220: "Transacoes",
                          221: "Grupo_Economico",
                          222: "Relacao_Empresa",
                          223: "Bilhete_Unico",
                          224: "Saldos_Diarios",
                          231: "Assinatura",
                          229: "Marcacoes_Campanha",
                          225: "Tarifas_Devidas",
                          226: "Push",
                          232: "DBM_Email",
                          233: "DBM_Endereco",
                          235: "DBM_Telefone",
                          234: "Fato_YC",
                          236: "Fato_YC_N2"}


@time_this
def stub(**context):
    logging.info('Execucao de metodo vazio para teste. Eh preciso substituir por um metodo com funcao real')
    print(context)

    return True


def obter_pipes_ingestao(**kwargs):
    """
    Retorna todas as origens de dados dos sistemas OLTPs da SuperDigital que devem ser ingeridos na camada de Stage
    para que o processamento do DW seja possivel
    """

    _sql = """
    SELECT
        c.objeto,
        c.observacao,

        CONCAT('idOrigem','|', a.idOrigem, '|',
                'idOrigem_Interface', '|', a.idOrigemInterface
        ) as parametro,

        CASE
            WHEN b.fonte_externa = 1 THEN 10
        END AS id_watcher_var,

        b.fonte_externa,

        LEFT(CAST(a.hora AS VARCHAR(50)),8) AS hora_processamento,

        a.idOrigem AS id_origem,
        a.idOrigemInterface AS id_origem_interface,
        a.idAgendamento_ETL AS id_agendamento_etl

    FROM _meta.agendamento_ETL (nolock) a

    JOIN _meta.origem_interface (nolock) b
    ON a.idOrigemInterface = b.idOrigemInterface

    JOIN _meta.origem c (nolock)
    ON a.idOrigem = c.idOrigem


    WHERE b.ativo = 1 -- Interface Ativa
    AND a.ativo = 1 -- Agendamento Ativo
    AND a.tipo = 'D' AND a.valor = 0 -- Agendamento diario
    AND b.fonte_externa = 1 -- Exclui as Origens de Orcamento e Teste

    ORDER BY id_origem,
             id_agendamento_etl,
             id_watcher_var,
             id_origem_interface
    """

    df_pipes_ing = do_db_query_2_pandas(_sql, **kwargs)
    return df_pipes_ing


def obter_pipes_processamento(**kwargs):
    """
    Retorna todas as origens de dados dos sistemas OLTPs da SuperDigital que devem ser ingeridos na camada de Stage
    para que o processamento do DW seja possivel
    """

    _sql = """
    SELECT
        c.objeto,
        c.observacao,

        CONCAT('idOrigem','|', a.idOrigem, '|',
                'idOrigem_Interface', '|', a.idOrigemInterface
        ) as parametro,

        CASE
            WHEN b.fonte_externa = 1 THEN 10
        END AS id_watcher_var,

        b.fonte_externa,

        LEFT(CAST(a.hora AS VARCHAR(50)),8) AS hora_processamento,

        a.idOrigem AS id_origem,
        a.idOrigemInterface AS id_origem_interface,
        a.idAgendamento_ETL AS id_agendamento_etl

    FROM _meta.agendamento_ETL (nolock) a

    JOIN _meta.origem_interface (nolock) b
    ON a.idOrigemInterface = b.idOrigemInterface

    JOIN _meta.origem c (nolock)
    ON a.idOrigem = c.idOrigem


    WHERE b.ativo = 1 -- Interface Ativa
    AND a.ativo = 1 -- Agendamento Ativo
    AND a.tipo = 'D' AND a.valor = 0 -- Agendamento diario
    AND b.fonte_externa = 1 -- Exclui as Origens de Orcamento e Teste

    ORDER BY id_origem,
             id_agendamento_etl,
             id_watcher_var,
             id_origem_interface
    """

    df_pipes_ing = do_db_query_2_pandas(_sql, **kwargs)
    return df_pipes_ing


def persist_dag_run(dag_id,
                    execution_date,
                    state,
                    run_id_prefix,
                    external_trigger,
                    start_date,
                    end_date=None,
                    conf=None
                    ):
    start_date = "'%s'" % start_date if start_date else 'NULL'
    end_date = "'%s'" % end_date if end_date else 'NULL'
    conf = "'%s'" % conf if conf else 'NULL'

    _sql = """
    MERGE airflow.dag_run as t1
    USING (
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ) AS t2 (dag_id, execution_date, state, run_id,
             external_trigger, conf, start_date, end_date)

    ON t1.dag_id = t2.dag_id
    AND t1.execution_date = t2.execution_date

    WHEN MATCHED THEN
       UPDATE SET t1.state = t2.state,
                   t1.end_date = t2.end_date

    WHEN NOT MATCHED THEN
       INSERT (dag_id, execution_date, state, run_id,
               external_trigger, conf, start_date, end_date)

       VALUES (t2.dag_id, t2.execution_date, t2.state,
               t2.run_id, t2.external_trigger, t2.conf,
               t2.start_date, t2.end_date)
    ;
    """ % ("'%s'" % dag_id,
           "'%s'" % execution_date,
           "'%s'" % state,
           "'%s%s.001+00:00'" % (run_id_prefix, execution_date),
           "%s" % external_trigger,
           "%s" % conf,
           "%s" % start_date,
           "%s" % end_date)

    # print(_sql)

    db_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__CONNECTION_STRING, }

    with closing(db_connect(**db_kwargs)) as conn:
        do_db_execute(_sql, conn=conn)
        db_commit(conn=conn)


def persist_task_instance(task_id,
                          dag_id,
                          execution_date,
                          state,
                          start_date,
                          end_date=None,
                          operator=None):

    if start_date is None or start_date is '':
        start_date = 'NULL'
    else:
        start_date = "'%s'" % start_date

    if end_date is None or end_date is '':
        end_date = 'NULL'
    else:
        end_date = "'%s'" % end_date

    if operator is None or operator is '':
        operator = "'PythonOperator'"
    else:
        operator = "'%s'" % operator

    _sql = """
    MERGE airflow.task_instance as t1
    USING (
        SELECT
        %s as task_id,
        %s as dag_id,
        %s as execution_date,
        %s as start_date,
        %s as end_date,
        CAST(DATEDIFF(MILLISECOND, %s, ISNULL(%s, GETDATE())) / 1000.0 AS FLOAT) as duration,
        %s as state,
        1 as try_number,
        'SL10VPYTHON.superdigital.com.br' as hostname,
        'airflow' as unixname,
        'default' as queue,
        %s as operator,
        0 as max_tries,
        0x80049503000000000000007D942E as executor_config
    ) as t2

    ON t1.task_id = t2.task_id
    AND t1.dag_id = t2.dag_id
    AND t1.execution_date = t2.execution_date

    WHEN MATCHED THEN
           UPDATE SET t1.state = t2.state,
                      t1.start_date = t2.start_date,
                      t1.end_date = t2.end_date,
                      t1.duration = t2.duration,
                      t1.operator = t2.operator

    WHEN NOT MATCHED THEN
    INSERT (task_id,
            dag_id,
            execution_date,
            start_date,
            end_date,
            duration,
            state,
            try_number,
            hostname,
            unixname,
            queue,
            operator,
            max_tries,
            executor_config)

    VALUES (t2.task_id,
            t2.dag_id,
            t2.execution_date,
            t2.start_date,
            t2.end_date,
            t2.duration,
            t2.state,
            t2.try_number,
            t2.hostname,
            t2.unixname,
            t2.queue,
            t2.operator,
            t2.max_tries,
            t2.executor_config)
    ;
    """ % ("'%s'" % task_id,
           "'%s'" % dag_id,
           "'%s'" % execution_date,

           "%s" % start_date,
           "%s" % end_date,
           "%s" % start_date,
           "%s" % end_date,

           "'%s'" % state,
           "%s" % operator
           )

    # print(_sql)

    db_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__CONNECTION_STRING, }

    with closing(db_connect(**db_kwargs)) as conn:
        do_db_execute(_sql, conn=conn)
        db_commit(conn=conn)


def get_df_status_ingestao(ref_date=None):
    if ref_date is None:
        ref_date = 'CAST(GETDATE() AS DATE)'
    else:
        ref_date = "'%s'" % ref_date

    _sql = """
    SELECT
        t1.idOrigem,

        CONCAT('idOrigem','|', t4.idOrigem, '|',
            'idOrigem_Interface', '|', t4.idOrigemInterface
        ) as parametro,

        t2.idStatus as id_status,
        t2.nome as status,
        dateadd(hour, datediff(hour, GetDate(), GetUTCDate()), t1.inicio) as inicio,
        dateadd(hour, datediff(hour, GetDate(), GetUTCDate()), t1.termino) as termino,
        t3.objeto,
        t4.tabela_cliente as consulta_origem,
        t1.registros_recebidos,
        t1.registros_lidos,
        t1.registros_carregados

    FROM [Superdigital_ETL].[log].carga (nolock) t1

    JOIN [Superdigital_ETL].[log].carga_status (nolock) t2
    ON t1.idStatus = t2.idStatus

    JOIN _meta.origem t3 (nolock)
    ON t1.idOrigem = t3.idOrigem

    JOIN [Superdigital_ETL]._meta.origem_interface (nolock) t4
    ON t1.idOrigem = t4.idOrigem

    WHERE CAST(inicio AS DATE) = %s
    ORDER BY inicio
    """ % ref_date

    db_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__CONNECTION_STRING, }
    df = do_db_query_2_pandas(_sql, **db_kwargs)
    return df


def get_df_logs_ingestao(id_origem,
                         parametro,
                         ref_date=None):
    if ref_date is None:
        ref_date = 'CAST(GETDATE() AS DATE)'
    else:
        ref_date = "'%s'" % ref_date

    _sql = """
    SELECT
        t_logs.dt_log,
        t_logs.idWatcher,
        t_orig.tipo as pipe_type,
        t_orig.nome as pipe_name,
        t_orig.processo as pipe_source,
        t_logs.idWatcher_Evento,
        t_logs.mensagem

    FROM
    (
        SELECT
            t1.idWatcher,
            t1.idWatcher_Evento,
            t2.data as dt_log,
            t2.mensagem

        FROM [Superdigital].[_meta].[watcher_evento] (nolock) t1

        JOIN (
            SELECT
                CASE WHEN CHARINDEX(':', t_inner.chave) = 0 THEN
                    CAST(t_inner.chave AS BIGINT)
                ELSE -1
                END as idWatcher_Evento,
                t_inner.*
            FROM [Superdigital].[log].[evento] (nolock) t_inner
            WHERE CAST(t_inner.data AS DATE) = %s
            AND CHARINDEX(':', t_inner.chave) = 0
        ) t2
        ON t1.idWatcher_Evento = t2.idWatcher_Evento

        WHERE CAST(t1.inicio AS DATE) = %s
        AND t1.idWatcher = 10
        AND t1.parametro = '%s'

        UNION ALL

        (
            SELECT
                10 AS idWatcher,
                -2 AS idWatcher_Evento,
                t3.data as dt_log,
                t3.mensagem
            FROM [Superdigital_ETL].[log].[evento] (nolock) t3

            JOIN [Superdigital_ETL].[log].carga (nolock) t4
            ON CAST(t3.chave AS BIGINT) = t4.idLogCarga
            AND CAST(t3.data AS DATE) = CAST(t4.inicio AS DATE)

            WHERE CAST(t3.data AS DATE) = %s
            AND CHARINDEX(':', t3.chave) = 0
            AND t4.idOrigem = %s
        )
    ) t_logs

    LEFT JOIN [Superdigital].[_meta].[watcher] (nolock) t_orig
    ON t_logs.idWatcher = t_orig.idWatcher

    ORDER BY dt_log
    """ % (ref_date,
           ref_date,
           parametro,
           ref_date,
           id_origem)

    db_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__CONNECTION_STRING, }
    df = do_db_query_2_pandas(_sql, **db_kwargs)
    return df


def get_df_status_cleansing(ref_date=None,
                            origens_sd=None):
    if ref_date is None:
        ref_date = 'CAST(GETDATE() AS DATE)'
    else:
        ref_date = "'%s'" % ref_date

    parametro_stmt = ''

    if origens_sd is None:
        parametro_stmt = "'1001', '1002', '1003', '1004', '1005', '1006', '1007'"
    else:
        for origem in origens_sd:
            parametro_stmt += "'%s', " % origem

        parametro_stmt = parametro_stmt[:-2]

    _sql = """
    SELECT
        t_logs.log_type,
        t_logs.idWatcher,

        CASE WHEN t_logs.idWatcher IN (200, 201, 202, 203) THEN t_logs.parametro
            WHEN t_logs.idWatcher = 204 THEN SUBSTRING(t_logs.parametro, 10, 4)
            ELSE -2
        END as origem_superdigital,

        CASE WHEN t_logs.idWatcher = 204 THEN SUBSTRING(t_logs.parametro, 27, 1)
            ELSE -2
        END as id_tipo_dimensao,

        t_orig.tipo as pipe_type,
        t_orig.nome as pipe_name,
        t_orig.processo as pipe_source,
        t_logs.parametro,
        t_logs.idWatcher_Evento,

        dateadd(hour, datediff(hour, GetDate(), GetUTCDate()), t_logs.execucao) as execucao,
        dateadd(hour, datediff(hour, GetDate(), GetUTCDate()), t_logs.inicio) as inicio,
        dateadd(hour, datediff(hour, GetDate(), GetUTCDate()), t_logs.termino) as termino

    FROM
    (
        SELECT
            'root' as log_type,
            t1.idWatcher_Evento,
            t1.idWatcher,
            t1.parametro,
            t1.execucao,
            t1.inicio,
            t1.termino

        FROM [Superdigital].[_meta].[watcher_evento] (nolock) t1

        WHERE CAST(t1.inicio AS DATE) = %s

    ) as t_logs

    LEFT JOIN [Superdigital].[_meta].[watcher] (nolock) t_orig
    ON t_logs.idWatcher = t_orig.idWatcher

    WHERE ( (t_logs.idWatcher IN (200, 201, 202, 203) AND t_logs.parametro IN (%s) )
            OR
            (t_logs.idWatcher = 204 AND SUBSTRING(t_logs.parametro, 10, 4) IN (%s) )
    )

    ORDER BY inicio
    """ % (ref_date,
           parametro_stmt,
           parametro_stmt)

    db_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__CONNECTION_STRING, }
    df = do_db_query_2_pandas(_sql, **db_kwargs)
    return df


def get_df_logs_cleansing(ref_date=None):
    if ref_date is None:
        ref_date = 'CAST(GETDATE() AS DATE)'
    else:
        ref_date = "'%s'" % ref_date

    _sql = """
    SELECT
        t_logs.log_type,
        dateadd(hour, datediff(hour, GetDate(), GetUTCDate()), t_logs.dt_log) as dt_log,
        t_logs.status,
        t_logs.idWatcher,

        CASE WHEN t_logs.idWatcher IN (200, 201, 202, 203) THEN t_logs.parametro
            WHEN t_logs.idWatcher = 204 THEN SUBSTRING(t_logs.parametro, 10, 4)
            ELSE -2
        END as origem_superdigital,

        CASE WHEN t_logs.idWatcher = 204 THEN SUBSTRING(t_logs.parametro, 27, 1)
            ELSE -2
        END as id_tipo_dimensao,

        t_orig.tipo as pipe_type,
        t_orig.nome as pipe_name,
        t_orig.processo as pipe_source,
        t_logs.parametro,
        t_logs.idWatcher_Evento,
        t_logs.mensagem

    FROM
    (
        SELECT
            'root' as log_type,
            t1.idWatcher_Evento,
            t1.idWatcher,
            t1.parametro,
            t2.idStatusEvento as status,
            t2.data as dt_log,
            t2.mensagem

        FROM [Superdigital].[_meta].[watcher_evento] (nolock) t1

        JOIN (
            SELECT
                CASE WHEN CHARINDEX(':', t_inner.chave) = 0 THEN
                    CAST(t_inner.chave AS INT)
                ELSE -1
                END as idWatcher_Evento,
                t_inner.*
            FROM [Superdigital].[log].[evento] (nolock) t_inner
            WHERE CAST(t_inner.data as DATE) = %s
            AND CHARINDEX(':', t_inner.chave) = 0
        ) t2
        ON t1.idWatcher_Evento = t2.idWatcher_Evento

        WHERE CAST(t1.inicio AS DATE) = %s
        AND t1.idWatcher in (200, 201, 202, 203, 204)

        UNION ALL (
            SELECT
                'child' as log_type,
                t3.idWatcher_Evento,
                t3.idWatcher,
                t3.parametro,
                t4.idStatusEvento as status,
                t4.data as dt_log,
                t4.mensagem

            FROM [Superdigital].[_meta].[watcher_evento] (nolock) t3

            JOIN (
                SELECT
                    CAST(ISNULL(t_inner.chave, 0) AS INT) as idWatcher_Evento,
                    t_inner.*
                FROM [Superdigital].[log].[evento] (nolock) t_inner
                WHERE CAST(t_inner.data AS DATE) = %s
                AND (t_inner.chave IS NULL OR
                        (CHARINDEX(':', t_inner.chave) = 0 AND
                         CHARINDEX('|', t_inner.chave) = 0
                        )
                )
            ) t4
            ON t3.idWatcher = t4.idTipoEvento
            AND CAST(t3.parametro AS INT) = t4.idWatcher_Evento

            WHERE CAST(t3.inicio AS DATE) = %s
            AND t3.idWatcher in (200, 201, 202, 203)
            AND (t3.parametro IS NULL OR
                    (CHARINDEX(':', t3.parametro) = 0 AND
                     CHARINDEX('|', t3.parametro) = 0
                    )
            )
        )

        UNION ALL (
            SELECT
                'clean_child' as log_type,
                t5.idWatcher_Evento,
                t5.idWatcher,
                t5.parametro,
                t6.idStatusEvento as status,
                t6.data as dt_log,
                t6.mensagem

            FROM [Superdigital].[_meta].[watcher_evento] (nolock) t5

            JOIN (
                SELECT
                    CAST(ISNULL(t_inner.chave, 0) AS INT) as idWatcher_Evento,
                    t_inner.*
                FROM [Superdigital].[log].[evento] (nolock) t_inner
                WHERE CAST(t_inner.data AS DATE) = %s
                AND (t_inner.chave IS NULL OR
                        (CHARINDEX(':', t_inner.chave) = 0 AND
                         CHARINDEX('|', t_inner.chave) = 0
                        )
                )
            ) t6
            ON (CAST(SUBSTRING(t5.parametro, 27, 1) AS INT) + 204) = t6.idTipoEvento
            AND CAST(SUBSTRING(t5.parametro, 10, 4) AS INT) = t6.idWatcher_Evento

            WHERE CAST(t5.inicio AS DATE) = %s
            AND t5.idWatcher = 204
            AND t5.parametro IS NOT NULL
            AND CHARINDEX(':', t5.parametro) = 0
            AND CHARINDEX('|', t5.parametro) > 0
        )


    ) as t_logs

    LEFT JOIN [Superdigital].[_meta].[watcher] (nolock) t_orig
    ON t_logs.idWatcher = t_orig.idWatcher

    ORDER BY dt_log
    """ % (ref_date,
           ref_date,
           ref_date,
           ref_date,
           ref_date,
           ref_date)

    db_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__CONNECTION_STRING, }
    df = do_db_query_2_pandas(_sql, **db_kwargs)
    return df


def get_df_status_processamento(ref_date=None):

    if ref_date is None:
        ref_date = 'CAST(GETDATE() AS DATE)'
    else:
        ref_date = "'%s'" % ref_date

    _sql = """
    SELECT
        t1.idWatcher,

        t_orig.tipo as pipe_type,
        t_orig.nome as pipe_name,
        t_orig.processo as pipe_source,

        t1.idWatcher_Evento,

        dateadd(hour, datediff(hour, GetDate(), GetUTCDate()), t1.execucao) as execucao,
        dateadd(hour, datediff(hour, GetDate(), GetUTCDate()), t1.inicio) as inicio,
        dateadd(hour, datediff(hour, GetDate(), GetUTCDate()), t1.termino) as termino

    FROM [Superdigital].[_meta].[watcher_evento] (nolock) t1

    LEFT JOIN [Superdigital].[_meta].[watcher] (nolock) t_orig
    ON t1.idWatcher = t_orig.idWatcher

    WHERE CAST(t1.inicio AS DATE) = %s
    AND t1.idWatcher in (209, 214, 215, 216, 217, 218, 219,
                         220, 221, 222, 223, 224, 231, 229,
                         225, 226, 232, 233, 235, 234, 236)

    ORDER BY inicio
    """ % ref_date

    db_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__CONNECTION_STRING, }
    df = do_db_query_2_pandas(_sql, **db_kwargs)
    return df


def get_df_logs_processamento(ref_date=None):

    if ref_date is None:
        ref_date = 'CAST(GETDATE() AS DATE)'
    else:
        ref_date = "'%s'" % ref_date

    _sql = """
    SELECT
        t2.idWatcher_Evento,
        t2.idWatcher,
        t1.idStatusEvento as id_status,
        t1.data as dt_log,
        t1.mensagem

    FROM [Superdigital].[log].[evento] t1

    JOIN [Superdigital]._meta.watcher_evento t2
    ON t1.data >= t2.inicio
    AND t1.data <= DATEADD(SECOND,30,t2.termino)
    AND (t1.chave = t2.idWatcher_Evento OR t1.idTipoEvento = t2.idWatcher)

    WHERE (CHARINDEX(':', t1.chave) = 0 OR t1.chave IS NULL)
    AND t2.idWatcher in (209, 214, 215, 216, 217, 218, 219,
                         220, 221, 222, 223, 224, 231, 229,
                         225, 226, 232, 233, 235, 234, 236)
    AND CAST(t2.execucao AS DATE) = %s
    AND CAST(t1.data AS DATE) = %s

    ORDER BY t1.data
    """ % (ref_date,
           ref_date)

    db_kwargs = {Constantes.KEY__CONNECTION_STRING: Constantes.DEF__CONNECTION_STRING, }
    df = do_db_query_2_pandas(_sql, **db_kwargs)
    return df


def get_status_ingestao(dt_ref):
    df_ing_status = get_df_status_ingestao(dt_ref)

    init_ing_date = None
    end_ing_date = None
    subdag_ing_status = None

    if len(df_ing_status.index) > 0:
        first_ing_row = df_ing_status[0:1].iloc[0]
        last_ing_row = df_ing_status[-1:].iloc[0]

        if not pd.isnull(first_ing_row['inicio']):
            # init_ing_date = str(first_ing_row['inicio'])[:-3]
            init_ing_date = str(first_ing_row['inicio'])

            if len(init_ing_date) == 19:
                init_ing_date += ".001"
            elif len(init_ing_date) == 26:
                init_ing_date = init_ing_date[:-3]

        end_ing_date = None

        min_ing_status = df_ing_status.min()[['id_status']]['id_status']

        if last_ing_row['idOrigem'] == 136 and not pd.isnull(last_ing_row['termino']):
            # end_ing_date = str(last_ing_row['termino'])[:-3]
            end_ing_date = str(last_ing_row['termino'])

            if len(end_ing_date) == 19:
                end_ing_date += ".001"
            elif len(end_ing_date) == 26:
                end_ing_date = end_ing_date[:-3]

            if min_ing_status > 0:
                subdag_ing_status = 'success'
            else:
                subdag_ing_status = 'failed'
        else:
            subdag_ing_status = 'running'

        if init_ing_date is not None:
            dt_init_date = datetime.strptime(init_ing_date, "%Y-%m-%d %H:%M:%S.%f")

            run_delta = datetime.now() - dt_init_date
            run_delta.days

            if run_delta.days > 0:
                subdag_ing_status = 'failed'
                end_ing_date = dt_init_date.date().isoformat() + ' 23:59:59.001'

    return {'dt_inicio': init_ing_date,
            'dt_termino': end_ing_date,
            'status': subdag_ing_status,
            'df_status': df_ing_status}


def get_status_cleansing(dt_ref):
    df_clean_status = get_df_status_cleansing(dt_ref)

    init_clean_date = None
    end_clean_date = None
    subdag_clean_status = None

    df_clean_logs = None

    if len(df_clean_status.index) > 0:
        first_clean_row = df_clean_status[0:1].iloc[0]
        last_clean_row = df_clean_status[-1:].iloc[0]

        if not pd.isnull(first_clean_row['inicio']):
            init_clean_date = str(first_clean_row['inicio'])

            if len(init_clean_date) == 19:
                init_clean_date += ".001"

        df_204_rows = df_clean_status[df_clean_status['id_tipo_dimensao'] == 4]

        df_clean_logs = get_df_logs_cleansing(dt_ref)
        df_logs_com_erro = df_clean_logs[df_clean_logs['status'] == 0]

        df_origens_sd_com_erro = df_logs_com_erro.groupby(['origem_superdigital']).min()[['status']]
        expected_orig_sd = 7 - len(df_origens_sd_com_erro.index)

        df_no_rows = df_clean_logs[df_clean_logs['idWatcher'] == 200].groupby(['origem_superdigital']).count()
        df_no_rows = df_no_rows[['mensagem']]
        expected_orig_sd = expected_orig_sd - len(df_no_rows[df_no_rows['mensagem'] == 5].index)

        if len(df_origens_sd_com_erro.index) == 7:
            # Todos pipes de todas origens sd falharam
            end_clean_date = str(last_clean_row['termino'])
            subdag_clean_status = 'failed'

        elif not pd.isnull(first_clean_row['inicio']) and \
                (df_204_rows is None or len(df_204_rows.index) <= 0):
            subdag_clean_status = 'running'

        else:
            last_204_row = df_204_rows[-1:].iloc[0]

            if len(df_204_rows.index) == expected_orig_sd and not pd.isnull(last_204_row['termino']):
                end_clean_date = str(last_clean_row['termino'])

                min_clean_status = df_clean_logs.min()[['status']]['status']

                if min_clean_status > 0:
                    subdag_clean_status = 'success'
                else:
                    subdag_clean_status = 'failed'
            elif not pd.isnull(first_clean_row['inicio']):
                subdag_clean_status = 'running'

    if end_clean_date is not None and len(end_clean_date) == 19:
        end_clean_date += '.001'

    if init_clean_date is not None:
        dt_init_date = datetime.strptime(init_clean_date, "%Y-%m-%d %H:%M:%S.%f")

        run_delta = datetime.now() - dt_init_date
        run_delta.days

        if run_delta.days > 0:
            subdag_clean_status = 'failed'
            end_clean_date = dt_init_date.date().isoformat() + ' 23:59:59.001'

    return {'dt_inicio': init_clean_date,
            'dt_termino': end_clean_date,
            'status': subdag_clean_status,
            'df_status': df_clean_status,
            'df_logs': df_clean_logs}


def get_status_processamento(dt_ref):
    df_proc_status = get_df_status_processamento(dt_ref)

    init_proc_date = None
    end_proc_date = None
    subdag_proc_status = None

    df_proc_logs = None

    if len(df_proc_status.index) > 0:
        first_proc_row = df_proc_status[0:1].iloc[0]
        last_proc_row = df_proc_status[-1:].iloc[0]

        if not pd.isnull(last_proc_row['inicio']):
            init_proc_date = str(first_proc_row['inicio'])

            if len(init_proc_date) == 19:
                init_proc_date += ".001"

        df_proc_logs = get_df_logs_processamento(dt_ref)
        min_proc_status = df_proc_logs.min()[['id_status']]['id_status']

        if last_proc_row['idWatcher'] == 236 and not pd.isnull(last_proc_row['termino']):
            end_proc_date = str(last_proc_row['termino'])

            if len(end_proc_date) == 19:
                end_proc_date += ".001"

            if min_proc_status > 0:
                subdag_proc_status = 'success'
            else:
                subdag_proc_status = 'failed'

        elif min_proc_status > 0:
            subdag_proc_status = 'running'

        elif min_proc_status == 0:
            if not pd.isnull(last_proc_row['termino']):
                end_proc_date = str(last_proc_row['termino'])
            subdag_proc_status = 'failed'

        if init_proc_date is not None:
            dt_init_date = datetime.strptime(init_proc_date, "%Y-%m-%d %H:%M:%S.%f")

            run_delta = datetime.now() - dt_init_date
            run_delta.days

            if run_delta.days > 0:
                subdag_proc_status = 'failed'
                end_proc_date = dt_init_date.date().isoformat() + ' 23:59:59.001'

    return {'dt_inicio': init_proc_date,
            'dt_termino': end_proc_date,
            'status': subdag_proc_status,
            'df_status': df_proc_status,
            'df_logs': df_proc_logs}


def persiste_dag_ingestao(status_ingestao,
                          execution_date):

    persist_dag_run('dw.dag_carga_diaria.sub_dag_ingestao',
                    execution_date,
                    status_ingestao['status'],
                    'scheduled_',
                    0,
                    status_ingestao['dt_inicio'],
                    status_ingestao['dt_termino'])

    persist_task_instance('sub_dag_ingestao',
                          'dw.dag_carga_diaria',
                          execution_date,
                          status_ingestao['status'],
                          status_ingestao['dt_inicio'],
                          status_ingestao['dt_termino'],
                          operator='SubDagOperator')


def persiste_dag_cleansing(status_cleansing,
                           execution_date):

    persist_dag_run('dw.dag_carga_diaria.sub_dag_cleansing',
                    execution_date,
                    status_cleansing['status'],
                    'scheduled_',
                    0,
                    status_cleansing['dt_inicio'],
                    status_cleansing['dt_termino'])

    persist_task_instance('sub_dag_cleansing',
                          'dw.dag_carga_diaria',
                          execution_date,
                          status_cleansing['status'],
                          status_cleansing['dt_inicio'],
                          status_cleansing['dt_termino'],
                          operator='SubDagOperator')


def persiste_dag_processamento(status_processamento,
                               execution_date):

    persist_dag_run('dw.dag_carga_diaria.sub_dag_processamento',
                    execution_date,
                    status_processamento['status'],
                    'scheduled_',
                    0,
                    status_processamento['dt_inicio'],
                    status_processamento['dt_termino'])

    persist_task_instance('sub_dag_processamento',
                          'dw.dag_carga_diaria',
                          execution_date,
                          status_processamento['status'],
                          status_processamento['dt_inicio'],
                          status_processamento['dt_termino'],
                          operator='SubDagOperator')


def persiste_pipe_ingestao_logs(dag_id,
                                pipe_task_id,
                                execution_date,
                                task_status_row,
                                df_pipe_logs):

    str_exec_date = execution_date.replace(' ', 'T')

    log_file_path = '/var/log/airflow/%s/%s/%s.001+00:00' % (dag_id,
                                                            pipe_task_id,
                                                            str_exec_date)
    log_file_name = '1.log'

    if not os.path.exists(log_file_path):
        os.makedirs(log_file_path)

    with open(log_file_path+'/'+log_file_name, 'w') as log_file:
        first_line = True

        for index, log_entry in df_pipe_logs.iterrows():

            if first_line:
                log_line = '[%s] INFO - Informacoes do Pipe: \n' % log_entry['dt_log']
                log_line += ' pipe_type:[%s]\n' % log_entry['pipe_type']
                log_line += ' pipe_name:[%s]\n' % log_entry['pipe_name']
                log_line += ' pipe_source:[%s]\n' % log_entry['pipe_source']
                log_line += ' idWatcher_Evento:[%s] \n\n' % log_entry['idWatcher_Evento']
                log_line += ' Parametro Watcher:[%s]\n' % task_status_row['parametro']
                log_line += ' Objeto:[%s]\n' % task_status_row['objeto']
                log_line += ' Registros Recebidos:[%s]\n' % task_status_row['registros_recebidos']
                log_line += ' Registros Lidos:[%s]\n' % task_status_row['registros_lidos']
                log_line += ' Registros Carregados:[%s]\n' % task_status_row['registros_carregados']
                log_line += ' Consulta na Origem:\n'
                log_line += ('-'*60)
                log_line += '\n%s\n' % task_status_row['consulta_origem']
                log_line += ('-'*60)
                log_line += '\n\n'

                log_file.write(log_line)
                first_line = False

            log_type = 'INFO'

            log_line = '[%s] %s - %s \n' % (log_entry['dt_log'],
                                            log_type,
                                            log_entry['mensagem'])

            log_file.write(log_line)


def persiste_pipes_ingestao(dt_ref,
                            df_pipes_ingestao,
                            execution_date):

    for index, task in df_pipes_ingestao.iterrows():
        task_id = 'ingestao.%s.%s' % (task.idOrigem,
                                      task.objeto)

        if task.id_status == 0:
            status = 'failed'
        elif task.id_status == 1:
            status = 'running'
        elif task.id_status >= 2:
            status = 'success'
        else:
            status = ''

        pipe_subdag_id = 'dw.dag_carga_diaria.sub_dag_ingestao'

        persist_task_instance(task_id,
                              pipe_subdag_id,
                              execution_date,
                              status,
                              str(task.inicio)[:-3],
                              str(task.termino)[:-3])
                              # str(task.inicio),
                              # str(task.termino))

        df_logs_ingestao = get_df_logs_ingestao(task['idOrigem'],
                                                task['parametro'],
                                                dt_ref)

        persiste_pipe_ingestao_logs(pipe_subdag_id,
                                    task_id,
                                    execution_date,
                                    task,
                                    df_logs_ingestao)


def persiste_pipe_cleansing_logs(dag_id,
                                 pipe_task_id,
                                 execution_date,
                                 df_pipe_logs):

    str_exec_date = execution_date.replace(' ', 'T')

    log_file_path = '/var/log/airflow/%s/%s/%s.001+00:00' % (dag_id,
                                                            pipe_task_id,
                                                            str_exec_date)
    log_file_name = '1.log'

    if not os.path.exists(log_file_path):
        os.makedirs(log_file_path)

    with open(log_file_path+'/'+log_file_name, 'w') as log_file:
        first_line = True

        for index, log_entry in df_pipe_logs.iterrows():

            if first_line:
                log_line = '[%s] INFO - Informacoes do Pipe: \n' % log_entry['dt_log']
                log_line += ' pipe_type:[%s]\n' % log_entry['pipe_type']
                log_line += ' pipe_name:[%s]\n' % log_entry['pipe_name']
                log_line += ' pipe_source:[%s]\n' % log_entry['pipe_source']
                log_line += ' parametro:[%s]\n' % log_entry['parametro']
                log_line += ' idWatcher_Evento:[%s] \n' % log_entry['idWatcher_Evento']

                log_file.write(log_line)
                first_line = False

            log_type = 'INFO'

            if log_entry['status'] == 0:
                log_type = 'ERROR'

            log_line = '[%s] %s - %s \n' % (log_entry['dt_log'],
                                            log_type,
                                            log_entry['mensagem'])

            log_file.write(log_line)


def persiste_status_pipes_cleansing(task_id,
                                    dag_id,
                                    execution_date,
                                    df_ctx,
                                    df_ctx_logs):

    for index, pipe in df_ctx.iterrows():

        pipe_logs = df_ctx_logs.loc[(df_ctx_logs['idWatcher'] == pipe['idWatcher']) &
                                    (df_ctx_logs['id_tipo_dimensao'] == pipe['id_tipo_dimensao'])]

        pipe_init_dt = str(pipe['inicio']) # + '.001'
        pipe_end_dt = None
        pipe_status = None

        id_origem_sd = str(pipe['origem_superdigital'])
        id_watcher = int(pipe['idWatcher'])
        id_param_cleansing = int(pipe['id_tipo_dimensao'])

        if id_watcher < 204:
            pipe_task_id = '%s.%s.%s.%s' % (task_id,
                                            id_origem_sd,
                                            ORIGENS_SUPER[id_origem_sd],
                                            id_watcher)
        elif id_watcher == 204:
            pipe_task_id = '%s.%s.%s.%s.%s.%s' % (task_id,
                                                  id_origem_sd,
                                                  ORIGENS_SUPER[id_origem_sd],
                                                  id_watcher,
                                                  id_param_cleansing,
                                                  PARAMS_CLEANSING[id_param_cleansing])

        if len(pipe_init_dt) == 19:
                pipe_init_dt += ".001"
        elif len(pipe_init_dt) == 26:
            pipe_init_dt = pipe_init_dt[:-3]

        if not pd.isnull(pipe['termino']):
            pipe_end_dt = str(pipe['termino']) # + '.001'

            if len(pipe_end_dt) == 19:
                pipe_end_dt += ".001"
            elif len(pipe_end_dt) == 26:
                pipe_end_dt = pipe_end_dt[:-3]

            min_pipe_status = pipe_logs.min()['status']

            if min_pipe_status <= 0:
                pipe_status = 'failed'
            else:
                pipe_status = 'success'

        persist_task_instance(pipe_task_id,
                              dag_id,
                              execution_date,
                              pipe_status,
                              pipe_init_dt,
                              pipe_end_dt)

        persiste_pipe_cleansing_logs(dag_id,
                                     pipe_task_id,
                                     execution_date,
                                     pipe_logs)


def persiste_pipes_cleansing(task_id,
                             execution_date,
                             origens_ctx,
                             expected_watc,
                             status_cleansing,
                             df_clean_status,
                             df_clean_logs):

    df_ctx = df_clean_status[df_clean_status['origem_superdigital'].isin(origens_ctx)]
    df_ctx_logs = None
    if df_clean_logs is not None:
        df_ctx_logs = df_clean_logs[df_clean_logs['origem_superdigital'].isin(origens_ctx)]

    orig_ctx_init = str(df_ctx.min()['inicio'])
    orig_ctx_end = str(df_ctx.max()['termino'])
    orig_ctx_status = None

    orig_ctx_watc_count = len(df_ctx.index)

    if df_ctx_logs is not None and len(df_ctx_logs.index) > 0:
        orig_ctx_min_status = int(df_ctx_logs.min()['status'])
    else:
        orig_ctx_min_status = 1

    is_origem_no_rows = False

    if df_ctx_logs is not None:
        df_no_rows = df_ctx_logs[df_ctx_logs['idWatcher'] == 200].groupby(['origem_superdigital']).count()
        df_no_rows = df_no_rows[['mensagem']]
        is_origem_no_rows = (len(df_no_rows[df_no_rows['mensagem'] == 5].index) > 0)

    dt_termino_wat_200 = None
    df_wat_200 = df_ctx[df_ctx['idWatcher'] == 200][0:1]

    if len(df_wat_200) > 0:
        dt_termino_wat_200 = df_ctx[df_ctx['idWatcher'] == 200][0:1].iloc[0]['termino']

    if orig_ctx_watc_count < expected_watc and status_cleansing['status'] == 'running':
        orig_ctx_status = 'running'
    elif orig_ctx_min_status == 0:
        orig_ctx_status = 'failed'
    elif orig_ctx_watc_count == expected_watc and orig_ctx_min_status > 0:
        orig_ctx_status = 'success'
    elif is_origem_no_rows and not pd.isnull(dt_termino_wat_200):
        orig_ctx_status = 'success'

    sub_dag_id = 'dw.dag_carga_diaria.sub_dag_cleansing.%s' % task_id

    if not pd.isnull(orig_ctx_init) and pd.notna(orig_ctx_init) and orig_ctx_init != 'nan':
        if len(orig_ctx_init) == 19:
                orig_ctx_init += ".001"
        elif len(orig_ctx_init) == 26:
            orig_ctx_init = orig_ctx_init[:-3]
    else:
        orig_ctx_init = None

    if not pd.isnull(orig_ctx_end) and pd.notna(orig_ctx_end) and orig_ctx_end != 'nan':
        if len(orig_ctx_end) == 19:
                orig_ctx_end += ".001"
        elif len(orig_ctx_end) == 26:
            orig_ctx_end = orig_ctx_end[:-3]

    else:
        orig_ctx_end = None

    persist_dag_run(sub_dag_id,
                    execution_date,
                    orig_ctx_status,
                    'scheduled_',
                    0,
                    orig_ctx_init,
                    orig_ctx_end)

    persist_task_instance(task_id,
                          'dw.dag_carga_diaria.sub_dag_cleansing',
                          execution_date,
                          orig_ctx_status,
                          orig_ctx_init,
                          orig_ctx_end,
                          operator='SubDagOperator')

    persiste_status_pipes_cleansing(task_id,
                                    sub_dag_id,
                                    execution_date,
                                    df_ctx,
                                    df_ctx_logs)

    return {'dt_inicio': orig_ctx_init,
            'dt_termino': orig_ctx_end,
            'status': orig_ctx_status}


def persiste_pipe_processamento_logs(dag_id,
                                     pipe_task_id,
                                     execution_date,
                                     task_status_row,
                                     df_pipe_logs):

    str_exec_date = execution_date.replace(' ', 'T')

    log_file_path = '/var/log/airflow/%s/%s/%s.001+00:00' % (dag_id,
                                                            pipe_task_id,
                                                            str_exec_date)
    log_file_name = '1.log'

    if not os.path.exists(log_file_path):
        os.makedirs(log_file_path)

    with open(log_file_path+'/'+log_file_name, 'w') as log_file:
        first_line = True

        for index, log_entry in df_pipe_logs.iterrows():

            if first_line:
                log_line = '[%s] INFO - Informacoes do Pipe: \n' % log_entry['dt_log']
                log_line += ' pipe_type:[%s]\n' % task_status_row['pipe_type']
                log_line += ' pipe_name:[%s]\n' % task_status_row['pipe_name']
                log_line += ' pipe_source:[%s]\n' % task_status_row['pipe_source']
                log_line += ' idWatcher_Evento:[%s] \n\n' % log_entry['idWatcher_Evento']

                log_file.write(log_line)
                first_line = False

            log_type = 'INFO'

            if log_entry['id_status'] == 0:
                log_type = 'ERROR'

            log_line = '[%s] %s - %s \n' % (log_entry['dt_log'],
                                            log_type,
                                            log_entry['mensagem'])

            log_file.write(log_line)


def persiste_pipes_processamento(df_proc_status,
                                 df_proc_logs,
                                 execution_date):

    for index, task in df_proc_status.iterrows():
        task_id = 'processamento.%s.%s' % (task.idWatcher,
                                           WATCHERS_PROCESSAMENTO[task.idWatcher])

        df_ctx_logs = df_proc_logs[df_proc_logs['idWatcher'] == int(task.idWatcher)]
        status = ''

        if not pd.isnull(task.termino) and len(df_ctx_logs.index) > 0:
            ctx_min_status = int(df_ctx_logs.min()['id_status'])

            if ctx_min_status == 0:
                status = 'failed'
            elif ctx_min_status > 0:
                status = 'success'

        elif not pd.isnull(task.inicio):
            status = 'running'
        elif len(df_ctx_logs.index) == 0:
            status = 'failed'

        pipe_subdag_id = 'dw.dag_carga_diaria.sub_dag_processamento'

        init_task_date = None
        end_task_date = None

        if not pd.isnull(task.inicio):
            init_task_date = str(task.inicio)

            if len(init_task_date) == 19:
                    init_task_date += ".001"
            elif len(init_task_date) == 26:
                init_task_date = init_task_date[:-3]

        if not pd.isnull(task.termino):
            end_task_date = str(task.termino)

            if len(end_task_date) == 19:
                    end_task_date += ".001"
            elif len(end_task_date) == 26:
                end_task_date = end_task_date[:-3]

        persist_task_instance(task_id,
                              pipe_subdag_id,
                              execution_date,
                              status,
                              init_task_date,
                              end_task_date)

        persiste_pipe_processamento_logs(pipe_subdag_id,
                                         task_id,
                                         execution_date,
                                         task,
                                         df_ctx_logs)


def atualizar_logs_status_carga_diaria(**kwargs):
    dt_ref = date.today().isoformat()
    kwargs.update({Constantes.KEY__DATA_REPROCESSAMENTO_CARGA : dt_ref})

    atualizar_logs_status_carga_diaria_especifica(**kwargs)


def atualizar_logs_status_carga_diaria_especifica(**kwargs):

    dt_ref = kwargs[Constantes.KEY__DATA_REPROCESSAMENTO_CARGA]

    if dt_ref is None or len(dt_ref) <= 0:
        logging.info('Nenhuma data de referência encontrada para processamento! Fim do processamento.')
        return

    logging.info('DATA de REFERENCIA para o processamento de metadados [%s].' % dt_ref)

    status_ingestao = get_status_ingestao(dt_ref)
    status_cleansing = get_status_cleansing(dt_ref)
    status_processamento = get_status_processamento(dt_ref)

    execution_date = status_ingestao['dt_inicio']
    carga_diaria_status = None

    if status_ingestao['status'] == 'success' and \
        status_cleansing['status'] == 'success' and \
            status_processamento['status'] == 'success':
        carga_diaria_status = 'success'
    elif status_ingestao['status'] == 'running' or \
        status_cleansing['status'] == 'running' or \
            status_processamento['status'] == 'running':
        carga_diaria_status = 'running'
    elif status_ingestao['status'] == 'failed' or \
        status_cleansing['status'] == 'failed' or \
            status_processamento['status'] == 'failed':
        carga_diaria_status = 'failed'

    logging.info('Status de SUBDAGs recuperados com sucesso!')
    logging.info('SubDag [Ingestao] Inicio.: %s' % status_ingestao['dt_inicio'])
    logging.info('SubDag [Ingestao] Fim....: %s' % status_ingestao['dt_termino'])
    logging.info('SubDag [Ingestao] Status.: %s' % status_ingestao['status'])

    logging.info('SubDag [Cleansing] Inicio.: %s' % status_cleansing['dt_inicio'])
    logging.info('SubDag [Cleansing] Fim....: %s' % status_cleansing['dt_termino'])
    logging.info('SubDag [Cleansing] Status.: %s' % status_cleansing['status'])

    logging.info('SubDag [Processamento] Inicio.: %s' % status_processamento['dt_inicio'])
    logging.info('SubDag [Processamento] Fim....: %s' % status_processamento['dt_termino'])
    logging.info('SubDag [Processamento] Status.: %s' % status_processamento['status'])

    if execution_date is None:
        return

    # Injecao dos Metadados da Dags e SubDags Principais
    persist_dag_run('dw.dag_carga_diaria',
                    execution_date,
                    carga_diaria_status,
                    'manual__',
                    0,
                    status_ingestao['dt_inicio'],
                    status_processamento['dt_termino'])

    persiste_dag_ingestao(status_ingestao, execution_date)
    persiste_dag_cleansing(status_cleansing, execution_date)
    persiste_dag_processamento(status_processamento, execution_date)

    logging.info('Status de SUBDAGs persistidos!')

    # Injecao dos Metadados da SubDag de Ingestao
    df_ingestao_status = status_ingestao['df_status']
    persiste_pipes_ingestao(dt_ref, df_ingestao_status, execution_date)

    logging.info('Status e Logs da SUBDAG de INGESTAO persistidos com sucesso!')

    # Injecao dos Metadados da SubDag de Cleansing
    df_clean_status = status_cleansing['df_status']
    df_clean_logs = status_cleansing['df_logs']

    orig_18_status = persiste_pipes_cleansing('cleansing.subdag.18.PessoasFisicas',
                                              execution_date,
                                              [1001],
                                              8,
                                              status_cleansing,
                                              df_clean_status,
                                              df_clean_logs)

    orig_22_status = persiste_pipes_cleansing('cleansing.subdag.22.PessoasJuridicas',
                                              execution_date,
                                              [1002],
                                              8,
                                              status_cleansing,
                                              df_clean_status,
                                              df_clean_logs)

    orig_35_status = persiste_pipes_cleansing('cleansing.subdag.35.PessoasJuridicasContasCorrentes',
                                              execution_date,
                                              [1005, 1006, 1007],
                                              17,
                                              status_cleansing,
                                              df_clean_status,
                                              df_clean_logs)

    orig_88_status = persiste_pipes_cleansing('cleansing.subdag.88.Usuarios',
                                              execution_date,
                                              [1003, 1004],
                                              10,
                                              status_cleansing,
                                              df_clean_status,
                                              df_clean_logs)

    logging.info('Status e Logs da SUBDAG de CLEANSING persistidos com sucesso!')

    # Injecao dos Metadados da SubDag de Processamento
    df_proc_status = status_processamento['df_status']
    df_proc_logs = status_processamento['df_logs']

    persiste_pipes_processamento(df_proc_status,
                                 df_proc_logs,
                                 execution_date)

    logging.info('Status e Logs da SUBDAG de PROCESSAMENTO persistidos com sucesso!')
