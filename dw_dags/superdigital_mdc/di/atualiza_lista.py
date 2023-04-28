import logging
import os

import pandas as pd

import superdigital_mdc.dw.database as database
import superdigital_mdc.dw.utils as utils
from allin.api import API, UploadArquivoBuilder


# *****************************
# FUNCOES AUXILIARES
# *****************************

def obter_id_definition_4_envio_lista(**kwargs):
    base_id_def = [95, 96, 97, 98, 99, 101]

    id_def_2_exclude = [55, 81, 57, 67, 68, 70, 104, 161, 162, 163, 164, 90, 92, 93, 94, 56, 109, 58, 59, 148, 82, 83, 84, 91, 144, 145]

    _sql = '''
        SELECT yex.idDefinition
        FROM [Superdigital_YC].[dbo].yc_execution yex
        WHERE CAST(yex.dateExtraction AS DATE) = cast(getdate() as date)
        ORDER BY yex.idExecution DESC
        '''
    list_id_definition = []
    for _ in database.do_db_query_fetchall(_sql, **kwargs):
        list_id_definition.append(_[0])
    else:
        lista_exec = list(sorted(set(list_id_definition + base_id_def)))
        for __ in id_def_2_exclude:
            if __ in lista_exec:
                lista_exec.remove(__)

    return lista_exec


def buscar_execucao_info(id_definition, dt_ref=None):
    if dt_ref:
        dt_ref = '\'%s\'' % dt_ref
    else:
        dt_ref = 'CAST(GETDATE() AS DATE)'

    _sql = ''' SELECT TOP 1 al.cd_lista as cd_lista, al.nm_lista, t1.idExecution
                FROM [Superdigital_YC].[dbo].[allin_lista] al
                INNER JOIN
                    (
                        SELECT TOP 1 CONCAT(yde.name, '_Grupo_' ,yex.codGrouping, '_', yde.idDefinition) as nm_lista, yde.idDefinition, yex.idExecution
                        FROM [Superdigital_YC].[dbo].yc_execution yex
                        INNER JOIN [Superdigital_YC].[dbo].yc_definition yde
                            ON yde.idDefinition = yex.idDefinition
                        WHERE yde.idDefinition = %s
                            AND CAST(yex.dateExtraction AS DATE) = %s
                        ORDER BY yex.idExecution DESC
                    ) t1 ON al.nm_lista = t1.nm_lista

                ORDER BY t1.idExecution DESC
            ''' % (id_definition, dt_ref)

    return _sql


def atualizar_status_lista(conn, is_ok, cd_lista, id_execution, msg_error=None):
    _msg = None
    if msg_error:
        logging.error('Falha ao enviar arquivo. Causa[%s] \n\tStackTrace [%s]' % (msg_error,
                                                                                  utils.stack_trace(msg_error)))
        _msg = str(msg_error)

    _in_status = 'ENVIADO_PARA_ALLIN' if is_ok else 'ERRO_INSERIR_EMAIL'
    logging.info("\t\tAtualizando LISTA_EMAIL [%s-%s] msg_error[%s]..." % (is_ok, _in_status, _msg))

    _params = [_in_status, _msg, cd_lista, cd_lista, id_execution]
    _sql = ''' UPDATE [Superdigital_YC].[dbo].allin_lista_email
                        SET in_status = ?,
                            tx_response = ?
                        WHERE cd_lista = ?
                            AND tx_valor in (
                                SELECT yepe.txt_col
                                FROM [Superdigital_YC].[dbo].yc_email_publico_envio yepe
                                WHERE yepe.idcampanha = ?
                                    and yepe.idExecution = ?
                            )
                    '''

    return database.do_db_execute(_sql, parameters=_params, conn=conn)


def buscar_dados_envio_as_df(cd_lista, id_execution, conn):
    _sql = '''
        SELECT yepe.contaCorrenteId, 
                ale.tx_campos,
                ale.tx_valor
        FROM [Superdigital_YC].[dbo].yc_email_publico_envio yepe
        INNER JOIN [Superdigital_YC].[dbo].[allin_lista_email] ale 
            ON ale.cd_lista = yepe.idcampanha AND ale.tx_valor = yepe.txt_col
        WHERE yepe.idcampanha = %s
        and yepe.idExecution = %s
    ''' % (cd_lista, id_execution)

    _rs = pd.read_sql(_sql, conn)
    df = pd.DataFrame(_rs)
    return df


def criar_arquivo_lista(df, file_full_path, header):
    _header = header.split(',')

    logging.info('\t\tCriando arquivo [%s]...' % (file_full_path))

    df_split = pd.DataFrame(df.tx_valor.str.split(';', 0).tolist(), columns=_header)

    df_split.to_csv(file_full_path,
                    index=False,
                    doublequote=False,
                    columns=_header,
                    header=_header,
                    encoding='latin1')

    logging.info('\t\tArquivo [%s] criado!' % file_full_path)


def executar_upload_lista(api, cd_lista, nm_lista, header, file_name, file_path):
    logging.info('\t\tEnviando arquivo... (%s, %s, %s, %s)' % (cd_lista, nm_lista, header, file_name))
    assert isinstance(api, API)

    b_upload = UploadArquivoBuilder() \
        .set_nm_lista(nm_lista) \
        .set_campos_arquivo(''.join(header)) \
        .set_separador(UploadArquivoBuilder.SEPARADOR_COMMA) \
        .set_excluidos(UploadArquivoBuilder.EXCLUIDOS_SIM) \
        .set_acao_arquivo(UploadArquivoBuilder.ACAO_ARQUIVO_DELETE_REESCREVER) \
        .set_qualificador(UploadArquivoBuilder.QUALIFICADOR_NENHUM) \
        .set_file_path(file_path) \
        .set_file_name(file_name)

    try:
        logging.debug('\n%s\n' % b_upload.build())

        _json = api.upload_file(b_upload)
        _id_log = _json["output"]["id_log"]

        logging.info('\t\tArquivo enviado com sucesso! id_log[%s]' % _id_log)
        return _id_log

    except Exception as e:
        is_token_invalidado = str(e).count(API.INVALID_TOKEN_RESPONSE) > 0
        if is_token_invalidado:
            logging.info('\t\t\tSessao AlliN encerrada, iniciando nova sessao...')
            api.start_session()
            logging.info('\t\t\tNova sessao criada com sucesso!')
        raise e


def persistir_contato(contato, id_definition, **kwargs):
    _params = [(contato['tx_valor'].split(';')[0]),
               (contato['contaCorrenteId']),
               id_definition]

    _upsert = """
                MERGE [Superdigital_YC].[dbo].MDC_email_contato AS [Target] 
                USING (SELECT 
                            ? AS [tx_email], 
                            ? AS [contaCorrenteId],
                            getdate() AS [dt_ultimo_contato],
                            ? AS [id_definition_contato]
                    ) AS [Source]  ON [Target].tx_email = [Source].tx_email

                WHEN MATCHED THEN 
                    UPDATE SET 
                        [Target].[tx_email] = [Source].[tx_email],
                        [Target].[contaCorrenteId] = [Source].[contaCorrenteId],
                        [Target].[dt_ultimo_contato] = [Source].[dt_ultimo_contato],
                        [Target].[id_definition_contato] = [Source].[id_definition_contato]

                WHEN NOT MATCHED THEN 
                    INSERT ([tx_email], [dt_ultimo_contato], [contaCorrenteId], [id_definition_contato]) 
                    VALUES ([Source].[tx_email], [Source].[dt_ultimo_contato], [Source].[contaCorrenteId], [Source].[id_definition_contato]);
            """  # ponto-e-virgula necessario!

    try:
        database.do_db_execute(_upsert, parameters=_params, conn=kwargs.get('conn'))

    except Exception as e:
        logging.error('FALHA persistir_contato [%s]' % e)

    return True


def apagar_arquivo(file_name):
    try:
        os.remove(file_name)
        logging.info("Arquivo [%s] deletado com sucesso!" % file_name)

    except Exception as e:
        logging.info("Nao foi possivel deletar arquivo [%s] Causa[%s]" % (file_name, e))
