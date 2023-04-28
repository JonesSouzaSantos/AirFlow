from abc import ABC
from enum import Enum

from tribool import Tribool

from superdigital_mdc.dw import database, utils
from superdigital_mdc.dw.utils import SingletonException


class Canal(Enum):
    '''
    Canais possiveis de Segmentacao
    '''

    EMAIL = 'EMAIL'
    SMS = 'SMS'
    PUSH = 'PUSH'


class DispositivoPlataforma(Enum):
    """
    Possiveis Plataformas de Dispositivos Moveis
    """
    ANDROID = 'Android'
    IOS = 'iOS'

    @staticmethod
    def by_value(by_value):
        all = [DispositivoPlataforma.ANDROID, DispositivoPlataforma.IOS]
        return [_ for _ in all if _.value == by_value][0]


class TipoDelta(Enum):
    '''
    Canais possiveis de Segmentacao
    '''

    DATETIME = 'datetime'
    DATE = 'date'
    INT = 'int'


class Sucesso(Enum):
    INDEFINIDO = -1
    NOK = 0
    OK = 1


class NivelLog(Enum):
    TRACE = 0
    INFO = 1
    ERROR = 9


class OrigemDestino(Enum):
    """
    Indicador de Origem ou Destino
    """
    ORIGEM = 'O'
    DESTINO = 'D'


class AcaoExtracao(Enum):
    """
    Indica qual acao foi realizada para o registro durante o processo de extracao
    """

    INDETERMINADO = '0'
    INSERT = 'I'
    UPDATE = 'U'


class SSIS_Status(Enum):
    '''
    Valores possiveis para Status de um job SSIS (integration service)
    '''

    CREATED = 1
    RUNNING = 2
    CANCELED = 3
    FAILED = 4
    PENDING = 5
    ENDED_UNEXPECTEDLY = 6
    SUCCEEDED = 7
    STOPPING = 8
    COMPLETED = 9


# **********************************************************************
# UTILS                                                                *
# **********************************************************************
def _all_columns(columns, prefix=None, exclude=None):
    prefix = prefix or ''
    exclude = exclude or []

    columns = [_ for _ in columns if _ not in exclude]

    if prefix:
        return ','.join(['%s.%s' % (prefix, col) for col in columns])
    else:
        return ','.join(columns)


def _all_entity_values(entity, columns, exclude=None):
    exclude = exclude or []

    return [entity[col] for col in columns if col not in exclude]


def _all_columns_as_params(param_count):
    return str('?,' * param_count)[:-1]


def _update(tabela, dict_update, dict_where, **kwargs):
    assert isinstance(dict_update, dict)
    assert isinstance(dict_where, dict)

    if not dict_update:
        raise ValueError('Nao ha colunas a atualizar! [%s]' % dict_update)

    if not dict_where:
        raise ValueError('Nao se realiza update sem where! [%s]' % dict_where)

    _p = list(dict_update.values())
    _p.extend(list(dict_where.values()))

    cols_update = ','.join('%s = ?' % _ for _ in dict_update.keys())
    cols_where = ' AND '.join('%s = ?' % _ for _ in dict_where.keys())

    _sql = '''
        UPDATE %s
        SET %s
        WHERE %s
    ''' % (tabela,
           cols_update,
           cols_where)

    # print('Update generico\n', _sql, _p)

    return database.do_db_execute(_sql, parameters=_p, **kwargs)


# **********************************************************************
# TABELAS                                                              *
# **********************************************************************

class ExecutionLog(object):
    TABLE_NAME = '[Superdigital_Work].[dbo].[CM_ExecutionLog]'

    ID_DEFINITION = 'IdDefinition'
    ID_EXECUTION = 'IdExecution'

    @staticmethod
    def findAll_minimum_by_Canal_DataExecucaoIsToday_SucessoIsTrue_IdExecutionNotNegativo(canal, **kwargs):
        if canal not in Canal:
            raise ValueError('Canal invalido! Informe [%s]' % (','.join(str(_) for _ in Canal)))

        _ind_canal = 'Ind%s' % canal.value

        _stm = '''
            SELECT A.IdDefinition, A.IdExecution
            FROM %s A
            INNER JOIN %s B
                ON B.IdDefinition = A.IdDefinition
            WHERE B.%s = 1
                AND FORMAT(A.DataExecucao, 'yyyy-MM-dd') = CAST(GETDATE() AS DATE)
                AND A.Sucesso = 1
                AND A.IdExecution > 0
            ORDER BY 1
        ''' % (ExecutionLog.TABLE_NAME, Campanha.TABLE_NAME, _ind_canal)

        _ret = []
        for _ in database.do_db_query_fetchall(_stm, **kwargs):
            _ret.append({ExecutionLog.ID_DEFINITION: _[0],
                         ExecutionLog.ID_EXECUTION: _[1]})

        return _ret

    @staticmethod
    def findAll_minimum_By_Canal_DataExecucaoGT_SucessoIsTrue(canal, d, **kwargs):
        if canal not in Canal:
            raise ValueError('Canal invalido! Informe [%s]' % (','.join(str(_) for _ in Canal)))

        _ind_canal = 'Ind%s' % canal.value

        _stm = '''
            SELECT A.IdDefinition, A.IdExecution
            FROM %s A
            INNER JOIN %s B
                ON B.IdDefinition = A.IdDefinition
            WHERE B.%s = 1
                AND CAST(A.DataExecucao AS DATE) >= CAST(GETDATE()%s AS DATE)
                AND A.Sucesso = 1
        ''' % (ExecutionLog.TABLE_NAME, Campanha.TABLE_NAME, _ind_canal, d)

        _ret = []
        for _ in database.do_db_query_fetchall(_stm, **kwargs):
            _ret.append({ExecutionLog.ID_DEFINITION: _[0],
                         ExecutionLog.ID_EXECUTION: _[1]})

        return _ret

    @staticmethod
    def find_MAX_IdExecution_by_IdDefinition_DataExecucao_SucessoIsTrue(id_definition, data_execucao, **kwargs):
        _p = [id_definition,
              data_execucao]

        _stm = '''
                SELECT MAX(IdExecution)
                FROM %s
                WHERE IdDefinition = ?
                        and CAST(DataExecucao as date) = CAST(? as date)
        ''' % ExecutionLog.TABLE_NAME

        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            return _[0]

        else:
            return None


class OutputSegmentadorEmail(object):
    TABLE_NAME = '[SUPERDIGITAL_WORK].[dbo].[CM_OutputSegmentador_Email]'

    EMAIL = 'Email'
    CONTA_CORRENTE_ID = 'ContaCorrenteId'
    HEADER_CAMPANHA = 'HeaderCampanha'
    VALUES_CAMPANHA = 'valuesCampanha'
    ID_DEFINITION = 'IdDefinition'
    ID_EXECUTION = 'IdExecution'
    GC = 'GC'
    IND_IP_ATIVO = 'IndIPAtivo'
    DATA_TRANSMISSAO = 'DataTransmissao'
    GCR = 'GCR'

    @staticmethod
    def find_minimum_by_IdDefinition_IdExecution_DataTransmissaoIsNull_GCIsFalse_TriboolSegmentaIp(id_definition,
                                                                                                   id_execution,
                                                                                                   tribool_segmenta_ip,
                                                                                                   **kwargs):
        assert isinstance(tribool_segmenta_ip, Tribool)

        _p = [id_definition,
              id_execution]

        _stm = '''
            SELECT Email,
                    ContaCorrenteId,
                    HeaderCampanha,
                    valuesCampanha
            FROM %s
            WHERE IdDefinition = ?
            AND IdExecution = ?
            AND DataTransmissao is NULL
            AND GC = 0
            AND GCR <> 1
        ''' % OutputSegmentadorEmail.TABLE_NAME

        _is_true_or_false = Tribool(None).count(tribool_segmenta_ip.value) < 1
        if _is_true_or_false:
            _p.append(utils.from_bool_2_int(tribool_segmenta_ip.value))
            _stm = '%s AND IndIPAtivo = ?' % _stm

        _ret = []

        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            _entity = {OutputSegmentadorEmail.EMAIL: _[0],
                       OutputSegmentadorEmail.CONTA_CORRENTE_ID: _[1],
                       OutputSegmentadorEmail.HEADER_CAMPANHA: _[2],
                       OutputSegmentadorEmail.VALUES_CAMPANHA: _[3],
                       }
            _ret.append(_entity)

        return _ret

    @staticmethod
    def count_by_IdDefinition_IdExecution_DataTransmissaoIsToday_GCIsFalse_TriboolSegmentaIp(id_definition,
                                                                                             id_execution,
                                                                                             tribool_segmenta_ip,
                                                                                             **kwargs):
        _p = [id_definition,
              id_execution]

        _stm = '''
            SELECT COUNT(*)
            FROM %s
            WHERE IdDefinition = ?
            AND IdExecution = ?
            AND CAST(DataTransmissao AS DATE) = CAST(GETDATE() AS DATE)
            AND GC = 0
        ''' % OutputSegmentadorEmail.TABLE_NAME

        _is_true_or_false = Tribool(None).count(tribool_segmenta_ip.value) < 1
        if _is_true_or_false:
            _p.append(utils.from_bool_2_int(tribool_segmenta_ip.value))
            _stm = '%s AND IndIPAtivo = ?' % _stm

        count = database.do_db_query_fetchone(_stm, parameters=_p, **kwargs)
        return count[0]

    @staticmethod
    def updateAll_DataTransmissao_by_IdDefinition_IdExecution_TriboolSegmentaIp(id_definition, id_execution,
                                                                                tribool_segmenta_ip, **kwargs):
        if not (id_definition and id_execution):
            raise ValueError('Informe idDefinition/idExecution corretamente! [%s,%s,%s]' % (
                id_definition, id_execution, tribool_segmenta_ip))

        _p = [id_definition,
              id_execution,
              ]

        _update = ''' 
                UPDATE %s
                SET DataTransmissao = GETDATE()
                WHERE IdDefinition = ?
                    AND IdExecution = ?
                    AND DataTransmissao IS NULL
                ''' % OutputSegmentadorEmail.TABLE_NAME

        _is_true_or_false = Tribool(None).count(tribool_segmenta_ip.value) < 1
        if _is_true_or_false:
            _p.append(utils.from_bool_2_int(tribool_segmenta_ip.value))
            _update = '%s AND IndIPAtivo = ?' % _update

        database.do_db_execute(_update, parameters=_p, **kwargs)


class OutputSegmentadorSMS(object):
    TABLE_NAME = '[Superdigital_Work].[dbo].[CM_OutputSegmentador_SMS]'

    CONTA_CORRENTE_ID = 'ContaCorrenteID'
    ID_DEFINITION = 'IdDefinition'
    ID_EXECUTION = 'IdExecution'
    TELEFONE = 'Telefone'
    MENSAGEM = 'Mensagem'

    @staticmethod
    def findAll_by_IdDefinition_IdExecution_DataTransmissaoIsNull_notGC(id_definition, id_execution, **kwargs):
        if not (id_definition and id_execution):
            raise ValueError('Informe idDefinition/idExecution corretamente! [%s,%s]' % (id_definition, id_execution))

        _p = [id_definition, id_execution]

        _stm = '''
            SELECT ContaCorrenteID,
                IdDefinition,
                IdExecution,
                Telefone,
                Mensagem
            FROM %s
            WHERE IdDefinition = ?
                AND IdExecution = ?
                AND DataTransmissao is NULL
                AND GC = 0
                AND GCR <> 1
        ''' % OutputSegmentadorSMS.TABLE_NAME

        _ret = []
        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            _entity = {OutputSegmentadorSMS.CONTA_CORRENTE_ID: _[0],
                       OutputSegmentadorSMS.ID_DEFINITION: _[1],
                       OutputSegmentadorSMS.ID_EXECUTION: _[2],
                       OutputSegmentadorSMS.TELEFONE: _[3],
                       OutputSegmentadorSMS.MENSAGEM: _[4],
                       }
            _ret.append(_entity)

        return _ret

    @staticmethod
    def updateAll_DataTransmissao_by_IdDefinition_IdExecution(id_definition, id_execution, data_transmissao, **kwargs):
        if not (id_definition and id_execution and data_transmissao):
            raise ValueError('Informe id_definition/id_execution/data_transmissao corretamente! [%s, %s, %s]' % (
                id_definition, id_execution, data_transmissao))

        _p = [data_transmissao, id_definition, id_execution]

        _stm = '''
            UPDATE %s
            set DataTransmissao = ?
            WHERE IdDefinition = ?
                AND IdExecution = ?
                AND DataTransmissao IS NULL
        ''' % OutputSegmentadorSMS.TABLE_NAME

        database.do_db_execute(_stm, parameters=_p, **kwargs)


class OutputSegmentadorPush(object):
    TABLE_NAME = '[Superdigital_Work].[dbo].[CM_OutputSegmentador_PUSH]'

    CONTA_CORRENTE_ID = 'ContaCorrenteId'
    ID_DEFINITION = 'IdDefinition'
    ID_EXECUTION = 'IdExecution'
    DEVICE_ID = 'DeviceId'
    DEVICE_PLATAFORMA = 'DevicePlataforma'
    ASSUNTO = 'Assunto'
    MENSAGEM = 'Mensagem'
    GC = 'GC'
    DATA_TRANSMISSAO = 'DataTransmissao'
    GCR = 'GCR'

    ALL_COLUMNS = [CONTA_CORRENTE_ID,
                   ID_DEFINITION,
                   ID_EXECUTION,
                   DEVICE_ID,
                   DEVICE_PLATAFORMA,
                   ASSUNTO,
                   MENSAGEM,
                   GC,
                   DATA_TRANSMISSAO,
                   GCR]

    @staticmethod
    def findAll_by_IdDefinition_IdExecution_DataTransmissaoIsNull_notGC_DevicePlataforma(id_definition,
                                                                                         id_execution,
                                                                                         device_plataforma,
                                                                                         **kwargs):
        if not (id_definition and id_execution and device_plataforma):
            raise ValueError(
                    'Informe Parametros corretamente! [%s,%s,%s]' % (id_definition, id_execution, device_plataforma))

        _p = [id_definition, id_execution, device_plataforma.value]

        _stm = '''
                SELECT %s
                FROM %s
                WHERE IdDefinition = ?
                    AND IdExecution = ?
                    AND DataTransmissao is NULL
                    AND GC = 0
                    AND GCR <> 1
                    AND DevicePlataforma = ?
            ''' % (_all_columns(OutputSegmentadorPush.ALL_COLUMNS), OutputSegmentadorPush.TABLE_NAME)

        _ret = []
        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            _entity = OutputSegmentadorPush._load_entity(_)
            _ret.append(_entity)

        return _ret

    @staticmethod
    def updateAll_DataTransmissao_by_IdDefinition_IdExecution_DevicePlataforma(id_definition,
                                                                               id_execution,
                                                                               device_plataforma,
                                                                               data_transmissao,
                                                                               **kwargs):
        if not (id_definition and id_execution and device_plataforma and data_transmissao):
            raise ValueError('Informe Parametros corretamente! [%s, %s, %s, %s]' % (
                id_definition, id_execution, device_plataforma, data_transmissao))

        _p = [data_transmissao, id_definition, id_execution, device_plataforma.value]

        _stm = '''
                    UPDATE %s
                    set DataTransmissao = ?
                    WHERE IdDefinition = ?
                        AND IdExecution = ?
                        AND DataTransmissao IS NULL
                        AND DevicePlataforma = ?
                ''' % OutputSegmentadorPush.TABLE_NAME

        database.do_db_execute(_stm, parameters=_p, **kwargs)

    @staticmethod
    def _load_entity(entidade):
        _entity = {OutputSegmentadorPush.CONTA_CORRENTE_ID: entidade[0],
                   OutputSegmentadorPush.ID_DEFINITION: entidade[1],
                   OutputSegmentadorPush.ID_EXECUTION: entidade[2],
                   OutputSegmentadorPush.DEVICE_ID: entidade[3],
                   OutputSegmentadorPush.DEVICE_PLATAFORMA: entidade[4],
                   OutputSegmentadorPush.ASSUNTO: entidade[5],
                   OutputSegmentadorPush.MENSAGEM: entidade[6],
                   OutputSegmentadorPush.GC: entidade[7],
                   OutputSegmentadorPush.DATA_TRANSMISSAO: entidade[8],
                   OutputSegmentadorPush.GCR: entidade[9]
                   }

        all_columns = OutputSegmentadorPush.ALL_COLUMNS
        if len(all_columns) != len(_entity):
            raise Exception('[OutputSegmentadorPush] _load_entity DEVE conter todos colunas de ALL_COLUMNS! '
                            '\nentity[%s] \nall_columns[%s]' % (
                                _entity, all_columns))

        return _entity


class Campanha(object):
    TABLE_NAME = '[SUPERDIGITAL_WORK].[dbo].[CM_Campanhas]'

    ID_DEFINITION = 'IdDefinition'
    NAME = 'Name'
    NOME_FANTASIA = 'NomeFantasia'
    PUBLICO = 'Publico'
    CTA = 'CTA'
    CLASS = 'Class'
    PRODUCT_ACTION = 'ProductAction'
    BUSINESS = 'Business'
    TIPO_COMUNICAO = 'TipoComunicao'
    SOURCE = 'Source'
    DIAS_DURACAO = 'DiasDuracao'
    FREQUENCIA = 'Frequencia'
    IND_SMS = 'IndSMS'
    IND_EMAIL = 'IndEmail'
    IND_PUSH = 'IndPush'
    ATIVO = 'Ativo'
    TX_CAMPOS = 'tx_campos'
    EFETIVIDADE_MAX = 'EfetividadeMax'
    ID_GC_PARAMETROS = 'IdGCParametros'
    MENSAGEM_SMS = 'MensagemSMS'
    ASSUNTO_PUSH = 'AssuntoPush'
    MENSAGEM_PUSH = 'MensagemPush'
    IND_SEGMENTA_IP = 'IndSegmentaIP'
    VALOR_CASHBACK = 'ValorCashback'
    HORA_LIMITE_PROMOCAO = 'HoraLimitePromocao'
    CASHBACK_EXTENSO = 'CashbackExtenso'
    ID_SUBCATEGORIA = 'IdSubcategoria'
    ID_SUBCATEGORIA_ORDEM = 'IdSubcategoriaOrdem'
    ID_SUBCATEGORIA_DD = 'IdSubcategoriaDD'

    ALL_COLUMNS = [ID_DEFINITION,
                   NAME,
                   NOME_FANTASIA,
                   PUBLICO,
                   CTA,
                   CLASS,
                   PRODUCT_ACTION,
                   BUSINESS,
                   TIPO_COMUNICAO,
                   SOURCE,
                   DIAS_DURACAO,
                   FREQUENCIA,
                   IND_SMS,
                   IND_EMAIL,
                   IND_PUSH,
                   ATIVO,
                   TX_CAMPOS,
                   EFETIVIDADE_MAX,
                   ID_GC_PARAMETROS,
                   MENSAGEM_SMS,
                   ASSUNTO_PUSH,
                   MENSAGEM_PUSH,
                   IND_SEGMENTA_IP,
                   VALOR_CASHBACK,
                   HORA_LIMITE_PROMOCAO,
                   CASHBACK_EXTENSO,
                   ID_SUBCATEGORIA,
                   ID_SUBCATEGORIA_ORDEM,
                   ID_SUBCATEGORIA_DD]

    @staticmethod
    def _load_entity(campanha):
        _entity = {Campanha.ID_DEFINITION: campanha[0],
                   Campanha.NAME: campanha[1],
                   Campanha.NOME_FANTASIA: campanha[2],
                   Campanha.PUBLICO: campanha[3],
                   Campanha.CTA: campanha[4],
                   Campanha.CLASS: campanha[5],
                   Campanha.PRODUCT_ACTION: campanha[6],
                   Campanha.BUSINESS: campanha[7],
                   Campanha.TIPO_COMUNICAO: campanha[8],
                   Campanha.SOURCE: campanha[9],
                   Campanha.DIAS_DURACAO: campanha[10],
                   Campanha.FREQUENCIA: campanha[11],
                   Campanha.IND_SMS: campanha[12],
                   Campanha.IND_EMAIL: campanha[13],
                   Campanha.IND_PUSH: campanha[14],
                   Campanha.ATIVO: campanha[15],
                   Campanha.TX_CAMPOS: campanha[16],
                   Campanha.EFETIVIDADE_MAX: campanha[17],
                   Campanha.ID_GC_PARAMETROS: campanha[18],
                   Campanha.MENSAGEM_SMS: campanha[19],
                   Campanha.ASSUNTO_PUSH: campanha[20],
                   Campanha.MENSAGEM_PUSH: campanha[21],
                   Campanha.IND_SEGMENTA_IP: campanha[22],
                   Campanha.VALOR_CASHBACK: campanha[23],
                   Campanha.HORA_LIMITE_PROMOCAO: campanha[24],
                   Campanha.CASHBACK_EXTENSO: campanha[25],
                   Campanha.ID_SUBCATEGORIA: campanha[26],
                   Campanha.ID_SUBCATEGORIA_ORDEM: campanha[27],
                   Campanha.ID_SUBCATEGORIA_DD: campanha[28],
                   }

        all_columns = Campanha.ALL_COLUMNS
        if len(all_columns) != len(_entity):
            raise Exception('[Campanha] _load_entity DEVE conter todos colunas de ALL_COLUMNS! '
                            '\nentity[%s] \nall_columns[%s]' % (
                                _entity, all_columns))

        return _entity

    @staticmethod
    def find_minimum_by_id(id_definition, **kwargs):
        if not id_definition:
            raise ValueError('Informe idDefinition corretamente! %s' % id_definition)

        _p = [id_definition]

        _stm = '''
                SELECT IdDefinition, Name
                FROM %s
                WHERE IdDefinition = ?
            ''' % Campanha.TABLE_NAME

        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            _entity = {Campanha.ID_DEFINITION: _[0],
                       Campanha.NAME: _[1],
                       }
            return _entity

        else:
            return None

    @staticmethod
    def find_by_id(id_definition, **kwargs):
        if not id_definition:
            raise ValueError('Informe idDefinition corretamente! %s' % id_definition)

        _p = [id_definition]

        _stm = '''
                SELECT %s
                FROM %s
                WHERE IdDefinition = ?
            ''' % (_all_columns(Campanha.ALL_COLUMNS), Campanha.TABLE_NAME)

        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            _entity = Campanha._load_entity(_)
            return _entity

        else:
            return None

    @staticmethod
    def find_by_Ativo(ativo, **kwargs):
        if ativo is None:
            raise ValueError('Informe Ativo corretamente! %s' % ativo)

        _p = [ativo]

        _stm = '''
                SELECT %s
                FROM %s
                WHERE Ativo = ?
            ''' % (_all_columns(Campanha.ALL_COLUMNS), Campanha.TABLE_NAME)

        _ret = []
        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            _entity = Campanha._load_entity(_)
            _ret.append(_entity)

        else:
            return _ret


class CampanhaContatoEmail(object):
    TABLE_NAME = '[Superdigital_Work].[dbo].[CM_CampanhaContato_Email]'

    ID_DEFINITION = 'IdDefinition'
    ID_EXECUTION = 'IdExecution'
    EMAIL = 'Email'
    CONTA_CORRENTE_ID = 'ContaCorrenteId'
    DATA_ENVIO = 'DataEnvio'
    ID_EXTERNO = 'IdExterno'
    STATUS_ENVIO = 'StatusEnvio'
    MOTIVO_BOUNCE = 'MotivoBounce'
    DATA_ABERTURA = 'DataAbertura'
    DATA_CLIQUE = 'DataClique'
    LINK_CLIQUE = 'LinkClique'

    @staticmethod
    def insert(email, conta_corrente_id, id_definition, id_execution, dt_envio, **kwargs):
        if not (email and conta_corrente_id and id_definition and id_execution and dt_envio):
            raise ValueError('Informe parametros corretamente! '
                             'email[%s] conta_corrente_id[%s] id_definition[%s] id_execution[%s] dt_envio[%s]' % (
                                 email, conta_corrente_id, id_definition, id_execution, dt_envio))

        _p = [email,
              dt_envio,
              conta_corrente_id,
              id_definition,
              id_execution]

        _upsert = '''
            INSERT INTO %s
            ([Email], [DataEnvio], [ContaCorrenteId], [IdDefinition], [IdExecution])
            VALUES (?, ?, ?, ?, ?);
        ''' % CampanhaContatoEmail.TABLE_NAME

        database.do_db_execute(_upsert, parameters=_p, **kwargs)

    @staticmethod
    def upsert_by_Email(email, conta_corrente_id, id_definition, id_execution, **kwargs):
        if not (email and conta_corrente_id and id_definition and id_execution):
            raise ValueError('Informe parametros corretamente! '
                             'email[%s] conta_corrente_id[%s] id_definition[%s] id_execution[%s] ' % (
                                 email, conta_corrente_id, id_definition, id_execution))

        _p = [email,
              conta_corrente_id,
              id_definition,
              id_execution]

        _upsert = '''
                MERGE %s AS [Target]
                    USING (SELECT
                                ? AS [Email],
                                ? AS [ContaCorrenteId],
                                getdate() AS [DataEnvio],
                                ? AS [IdDefinition],
                                ? AS [IdExecution]
                        ) AS [Source] ON [Target].Email = [Source].Email
    
                    WHEN MATCHED THEN
                        UPDATE SET
                            [Target].[Email] = [Source].[Email],
                            [Target].[ContaCorrenteId] = [Source].[ContaCorrenteId],
                            [Target].[DataEnvio] = [Source].[DataEnvio],
                            [Target].[IdDefinition] = [Source].[IdDefinition],
                            [Target].[IdExecution] = [Source].[IdExecution]
        
                    WHEN NOT MATCHED THEN
                        INSERT ([Email], 
                                [DataEnvio], 
                                [ContaCorrenteId], 
                                [IdDefinition], 
                                [IdExecution]
                        )
                        VALUES ([Source].[Email], 
                                [Source].[DataEnvio], 
                                [Source].[ContaCorrenteId], 
                                [Source].[IdDefinition], 
                                [Source].[IdExecution]
                        );
                ''' % CampanhaContatoEmail.TABLE_NAME

        database.do_db_execute(_upsert, parameters=_p, **kwargs)

    @staticmethod
    def update_StatusEnvio_by_Email_IdDefinition_IdExecution(status_envio, id_externo, email, id_definition,
                                                             id_execution,
                                                             **kwargs):
        if not (status_envio and id_externo and email and id_definition and id_execution):
            raise ValueError('Informe parametros corretamente! '
                             'status_envio[%s] id_externo[%s] '
                             'email[%s] id_definition[%s] id_execution[%s] ' % (
                                 status_envio, id_externo,
                                 email, id_definition, id_execution))

        _p = [status_envio,
              id_externo,
              email,
              id_definition,
              id_execution]

        _upsert = '''
                UPDATE %s
                SET
                    StatusEnvio = ?,
                    IdExterno = COALESCE(IdExterno, ?)
                WHERE Email = ? 
                    AND IdDefinition = ? 
                    AND IdExecution = ?
                ''' % CampanhaContatoEmail.TABLE_NAME

        database.do_db_execute(_upsert, parameters=_p, **kwargs)

    @staticmethod
    def update_DataClique_LinkClique_by_Email_IdDefinition_IdExecution(data_clique, link_clique, id_externo,
                                                                       email, id_definition, id_execution,
                                                                       **kwargs):
        if not (data_clique and link_clique and id_externo and email and id_definition and id_execution):
            raise ValueError('Informe parametros corretamente! '
                             'data_clique[%s] link_clique[%s] id_externo[%s] email[%s] id_definition[%s] id_execution[%s] ' % (
                                 data_clique, link_clique, id_externo, email, id_definition, id_execution))

        _p = [data_clique,
              link_clique,
              id_externo,
              email,
              id_definition,
              id_execution]

        _upsert = '''
                    UPDATE %s
                    SET
                        DataClique = ?,
                        LinkClique = ?,
                        IdExterno = COALESCE(IdExterno, ?)
                    WHERE Email = ? 
                        AND IdDefinition = ? 
                        AND IdExecution = ?
                    ''' % CampanhaContatoEmail.TABLE_NAME

        database.do_db_execute(_upsert, parameters=_p, **kwargs)

    @staticmethod
    def update_DataAbertura_by_Email_IdDefinition_IdExecution(data_abertura, id_externo,
                                                              email, id_definition, id_execution,
                                                              **kwargs):
        if not (data_abertura and id_externo and email and id_definition and id_execution):
            raise ValueError('Informe parametros corretamente! '
                             'data_abertura[%s] id_externo[%s] email[%s] id_definition[%s] id_execution[%s] ' % (
                                 data_abertura, id_externo, email, id_definition, id_execution))

        _p = [data_abertura,
              id_externo,
              email,
              id_definition,
              id_execution]

        _upsert = '''
                    UPDATE %s
                    SET
                        DataAbertura = ?,
                        IdExterno = COALESCE(IdExterno, ?)
                    WHERE Email = ? 
                        AND IdDefinition = ? 
                        AND IdExecution = ?
                    ''' % CampanhaContatoEmail.TABLE_NAME

        database.do_db_execute(_upsert, parameters=_p, **kwargs)

    @staticmethod
    def update_MotivoBounce_by_Email_IdDefinition_IdExecution(motivo_bounce, email, id_definition, id_execution,
                                                              **kwargs):
        if not (motivo_bounce and email and id_definition and id_execution):
            raise ValueError('Informe parametros corretamente! '
                             'motivo_bounce[%s] email[%s] id_definition[%s] id_execution[%s] ' % (
                                 motivo_bounce, email, id_definition, id_execution))

        _p = [motivo_bounce,
              email,
              id_definition,
              id_execution]

        _upsert = '''
                    UPDATE %s
                    SET
                        MotivoBounce = ?
                    WHERE Email = ? 
                        AND IdDefinition = ? 
                        AND IdExecution = ?
                    ''' % CampanhaContatoEmail.TABLE_NAME

        database.do_db_execute(_upsert, parameters=_p, **kwargs)


class CampanhaContatoSMS(object):
    TABLE_NAME = '[Superdigital_Work].[dbo].[CM_CampanhaContato_SMS]'

    TELEFONE = 'Telefone'
    CONTA_CORRENTE_ID = 'ContaCorrenteId'
    ID_DEFINITION = 'IdDefinition'
    ID_EXECUTION = 'IdExecution'
    DATA_ENVIO = 'DataEnvio'

    @staticmethod
    def upsert_by_Telefone(telefone, conta_corrente_id, id_definition, id_execution, **kwargs):
        if not (telefone and conta_corrente_id and id_definition and id_execution):
            raise ValueError('Informe parametros corretamente! '
                             'telefone[%s] conta_corrente_id[%s] id_definition[%s] id_execution[%s] ' % (
                                 telefone, conta_corrente_id, id_definition, id_execution))

        _p = [telefone,
              conta_corrente_id,
              id_definition,
              id_execution]

        _upsert = '''
            MERGE %s AS [Target]
                USING (SELECT
                            ? AS [Telefone],
                            ? AS [ContaCorrenteId],
                            getdate() AS [DataEnvio],
                            ? AS [IdDefinition],
                            ? AS [IdExecution]
                    ) AS [Source] ON [Target].Telefone = [Source].Telefone

                WHEN MATCHED THEN
                    UPDATE SET
                        [Target].[Telefone] = [Source].[Telefone],
                        [Target].[ContaCorrenteId] = [Source].[ContaCorrenteId],
                        [Target].[DataEnvio] = [Source].[DataEnvio],
                        [Target].[IdDefinition] = [Source].[IdDefinition],
                        [Target].[IdExecution] = [Source].[IdExecution]

                WHEN NOT MATCHED THEN
                    INSERT ([Telefone], 
                            [DataEnvio], 
                            [ContaCorrenteId], 
                            [IdDefinition], 
                            [IdExecution])
                    VALUES ([Source].[Telefone], 
                            [Source].[DataEnvio], 
                            [Source].[ContaCorrenteId], 
                            [Source].[IdDefinition], 
                            [Source].[IdExecution]);
            ''' % CampanhaContatoSMS.TABLE_NAME

        database.do_db_execute(_upsert, parameters=_p, **kwargs)


class CampanhaContatoPush(object):
    TABLE_NAME = '[Superdigital_Work].[dbo].[CM_CampanhaContato_Push]'

    ID_DEFINITION = 'IdDefinition'
    ID_EXECUTION = 'IdExecution'
    DEVICE_ID = 'DeviceId'
    DEVICE_PLATAFORMA = 'DevicePlataforma'
    CONTA_CORRENTE_ID = 'ContaCorrenteId'
    DATA_ENVIO = 'DataEnvio'

    @staticmethod
    def insert(campanha_contato_push, **kwargs):
        if not (campanha_contato_push.get(CampanhaContatoPush.ID_DEFINITION) and
                campanha_contato_push.get(CampanhaContatoPush.ID_EXECUTION) and
                campanha_contato_push.get(CampanhaContatoPush.DEVICE_ID) and
                campanha_contato_push.get(CampanhaContatoPush.CONTA_CORRENTE_ID)):
            raise ValueError('Informe parametros corretamente! CampanhaContatoPush[%s]' % campanha_contato_push)

        _p = [campanha_contato_push[CampanhaContatoPush.ID_DEFINITION],
              campanha_contato_push[CampanhaContatoPush.ID_EXECUTION],
              campanha_contato_push[CampanhaContatoPush.DEVICE_ID],
              campanha_contato_push[CampanhaContatoPush.DEVICE_PLATAFORMA],
              campanha_contato_push[CampanhaContatoPush.CONTA_CORRENTE_ID],
              campanha_contato_push[CampanhaContatoPush.DATA_ENVIO]]

        _stm = '''
                INSERT INTO %s (
                    [IdDefinition], 
                    [IdExecution], 
                    [DeviceId], 
                    [DevicePlataforma], 
                    [ContaCorrenteId], 
                    [DataEnvio]
                )
                VALUES (?, ?, ?, ?, ?, ?);
                ''' % CampanhaContatoPush.TABLE_NAME

        database.do_db_execute(_stm, parameters=_p, **kwargs)


class CampanhaRetornoEmail(object):
    TABLE_NAME = '[Superdigital_Work].[dbo].[CM_CampanhaRetorno_Email]'

    ID_DEFINITION = 'IdDefinition'
    ID_EXECUTION = 'IdExecution'
    ID_CAMPANHA_EXTERNO = 'IdCampanhaExterno'
    NOME_CAMPANHA_EXTERNO = 'NomeCampanhaExterno'
    TOTAL_ENVIO = 'TotalEnvio'
    TOTAL_ENTREGUE = 'TotalEntregue'
    TOTAL_ABERTURA = 'TotalAbertura'
    TOTAL_CLIQUE = 'TotalClique'
    DATA_INSERCAO = 'DataInsercao'
    DATA_ATUALIZACAO = 'DataAtualizacao'

    @staticmethod
    def find_by_id(id_definition, id_execution, id_campanha_externo, **kwargs):
        if not (id_definition, id_execution, id_campanha_externo):
            raise ValueError('Informe PK corretamente! '
                             'id_definition[%s] id_execution[%s] id_campanha_externo[%s]' % (
                                 id_definition, id_execution, id_campanha_externo))

        _p = [id_definition,
              id_execution,
              id_campanha_externo]

        _stm = '''
                SELECT IdDefinition
                        ,IdExecution
                        ,IdCampanhaExterno
                        ,NomeCampanhaExterno
                        ,TotalEnvio
                        ,TotalEntregue
                        ,TotalAbertura
                        ,TotalClique
                        ,DataInsercao
                        ,DataAtualizacao
                FROM %s
                WHERE IdDefinition = ?
                    AND IdExecution = ?
                    AND IdCampanhaExterno = ?
            ''' % CampanhaRetornoEmail.TABLE_NAME

        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            _entity = {CampanhaRetornoEmail.ID_DEFINITION: _[0],
                       CampanhaRetornoEmail.ID_EXECUTION: _[1],
                       CampanhaRetornoEmail.ID_CAMPANHA_EXTERNO: _[2],
                       CampanhaRetornoEmail.NOME_CAMPANHA_EXTERNO: _[3],
                       CampanhaRetornoEmail.TOTAL_ENVIO: _[4],
                       CampanhaRetornoEmail.TOTAL_ENTREGUE: _[5],
                       CampanhaRetornoEmail.TOTAL_ABERTURA: _[6],
                       CampanhaRetornoEmail.TOTAL_CLIQUE: _[7],
                       CampanhaRetornoEmail.DATA_INSERCAO: _[8],
                       CampanhaRetornoEmail.DATA_ATUALIZACAO: _[9]
                       }
            return _entity

        else:
            return None

    @staticmethod
    def upsert(entidade, **kwargs):
        if not (isinstance(entidade, dict) and entidade[CampanhaRetornoEmail.ID_DEFINITION] is not None):
            raise ValueError('Informe parametros corretamente! CampanhaRetornoEmail[%s]' % entidade)

        _p = [entidade[CampanhaRetornoEmail.ID_DEFINITION],
              entidade[CampanhaRetornoEmail.ID_EXECUTION],
              entidade[CampanhaRetornoEmail.ID_CAMPANHA_EXTERNO],
              entidade[CampanhaRetornoEmail.NOME_CAMPANHA_EXTERNO],
              entidade[CampanhaRetornoEmail.TOTAL_ENVIO],
              entidade[CampanhaRetornoEmail.TOTAL_ENTREGUE],
              entidade[CampanhaRetornoEmail.TOTAL_ABERTURA],
              entidade[CampanhaRetornoEmail.TOTAL_CLIQUE]]

        _upsert = '''
                    MERGE %s AS [Target]
                        USING (SELECT
                                    ? as [IdDefinition],
                                    ? as [IdExecution],
                                    ? as [IdCampanhaExterno],
                                    ? as [NomeCampanhaExterno],
                                    ? as [TotalEnvio],
                                    ? as [TotalEntregue],
                                    ? as [TotalAbertura],
                                    ? as [TotalClique],
                                    GETDATE() as [DataInsercao],
                                    GETDATE() as [DataAtualizacao]
                            ) AS [Source] 
                                ON [Target].IdDefinition = [Source].IdDefinition
                                AND[Target].IdExecution = [Source].IdExecution
                                AND[Target].IdCampanhaExterno = [Source].IdCampanhaExterno

                        WHEN MATCHED THEN
                            UPDATE SET
                                [Target].[TotalEnvio] = [Source].[TotalEnvio],
                                [Target].[TotalEntregue] = [Source].[TotalEntregue],
                                [Target].[TotalAbertura] = [Source].[TotalAbertura],
                                [Target].[TotalClique] = [Source].[TotalClique],
                                [Target].[DataAtualizacao] = [Source].[DataAtualizacao]

                        WHEN NOT MATCHED THEN
                            INSERT ([IdDefinition],
                                    [IdExecution],
                                    [IdCampanhaExterno],
                                    [NomeCampanhaExterno],
                                    [TotalEnvio],
                                    [TotalEntregue],
                                    [TotalAbertura],
                                    [TotalClique],
                                    [DataInsercao]
                            )
                            VALUES (Source.[IdDefinition],
                                    Source.[IdExecution],
                                    Source.[IdCampanhaExterno],
                                    Source.[NomeCampanhaExterno],
                                    Source.[TotalEnvio],
                                    Source.[TotalEntregue],
                                    Source.[TotalAbertura],
                                    Source.[TotalClique],
                                    Source.[DataInsercao]
                            );
                ''' % CampanhaRetornoEmail.TABLE_NAME

        database.do_db_execute(_upsert, parameters=_p, **kwargs)


class ParametroOperacional(object):
    TABLE_NAME = '[Superdigital_Work].[dbo].[CM_ParametroOperacional]'

    NOME_PARAMETRO = 'NomeParametro'
    VALOR_PARAMETRO = 'ValorParametro'

    P__DAG_ENVIOEMAIL_CAMPANHA_WHITELIST = 'DAG_ENVIOEMAIL_CAMPANHA_WHITELIST'
    P__DAG_ENVIOSMS_CAMPANHA_WHITELIST = 'DAG_ENVIOSMS_CAMPANHA_WHITELIST'
    P__DAG_ENVIOPUSH_CAMPANHA_WHITELIST = 'DAG_ENVIOPUSH_CAMPANHA_WHITELIST'
    P__DAG_SEGMENTA_CAMPANHA_WHITELIST = 'DAG_SEGMENTA_CAMPANHA_WHITELIST'

    P__DAG_ENVIOEMAIL_CAMPANHA_REPROCESSAR = 'DAG_ENVIOEMAIL_CAMPANHA_REPROCESSAR'

    P__CAMPANHA_RETORNO_DIAS_A_ANALISAR = 'CAMPANHA_RETORNO_DIAS_A_ANALISAR'
    P__HOLOS_DASHBOARDVENDAS_NUMDBM_PJ = 'HOLOS_DASHBOARDVENDAS_NUMDBM_PJ'

    @staticmethod
    def find_by_id(nome_parametro, **kwargs):
        if not nome_parametro:
            raise ValueError('Informe NomeParametro corretamente! %s' % nome_parametro)

        _p = [nome_parametro]

        _stm = '''
                    SELECT NomeParametro, ValorParametro
                    FROM %s
                    WHERE NomeParametro = ?
                ''' % ParametroOperacional.TABLE_NAME

        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            _entity = {ParametroOperacional.NOME_PARAMETRO: _[0],
                       ParametroOperacional.VALOR_PARAMETRO: _[1],
                       }
            return _entity

        else:
            return None

    @staticmethod
    def find_ValorParametro_by_id(nome_parametro, **kwargs):
        if not nome_parametro:
            raise ValueError('Informe NomeParametro corretamente! %s' % nome_parametro)

        _p = [nome_parametro]

        _stm = '''
                    SELECT ValorParametro
                    FROM %s
                    WHERE NomeParametro = ?
                ''' % ParametroOperacional.TABLE_NAME

        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            return _[0]

        else:
            return None

    @staticmethod
    def find_Valor_by_id_as_list(nome_parametro, **kwargs):
        if not nome_parametro:
            raise ValueError('Informe NomeParametro corretamente! %s' % nome_parametro)

        _p = [nome_parametro]

        _stm = '''
            SELECT ValorParametro
            FROM %s
            WHERE NomeParametro = ?
        ''' % ParametroOperacional.TABLE_NAME

        _blank = ''

        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            valor = _[0]
            _ret = [str(_).strip() for _ in valor.split(',') if _ != _blank]
            return _ret

        else:
            return []


class FonteDados(object):
    TABLE_NAME = 'Superdigital_ETL._meta.ED_FonteDados'

    ID_FONTE_DADOS = 'idFonteDados'
    ENDERECO = 'endereco'
    NOME = 'nome'
    ORIGEM_DESTINO = 'origemDestino'
    COLUNAS = 'colunas'
    DELTA_CRIACAO = 'deltaCriacao'
    DELTA_ALTERACAO = 'deltaAlteracao'
    DELTA_TIPO = 'deltaTipo'
    CLAUSULA_WHERE = 'clausulaWhere'
    CRIADO_EM = 'criadoEm'
    ALTERADO_EM = 'alteradoEm'
    COLUNA_DEDUP = 'colunaDedup'

    ALL_COLUMNS = [ID_FONTE_DADOS,
                   ENDERECO,
                   NOME,
                   ORIGEM_DESTINO,
                   COLUNAS,
                   DELTA_CRIACAO,
                   DELTA_ALTERACAO,
                   DELTA_TIPO,
                   CLAUSULA_WHERE,
                   CRIADO_EM,
                   ALTERADO_EM,
                   COLUNA_DEDUP,
                   ]

    @staticmethod
    def _load_entity(entidade, start_index=0):

        _entity = {FonteDados.ID_FONTE_DADOS: entidade[0 + start_index],
                   FonteDados.ENDERECO: entidade[1 + start_index],
                   FonteDados.NOME: entidade[2 + start_index],
                   FonteDados.ORIGEM_DESTINO: entidade[3 + start_index],
                   FonteDados.COLUNAS: entidade[4 + start_index],
                   FonteDados.DELTA_CRIACAO: entidade[5 + start_index],
                   FonteDados.DELTA_ALTERACAO: entidade[6 + start_index],
                   FonteDados.DELTA_TIPO: entidade[7 + start_index],
                   FonteDados.CLAUSULA_WHERE: entidade[8 + start_index],
                   FonteDados.CRIADO_EM: entidade[9 + start_index],
                   FonteDados.ALTERADO_EM: entidade[10 + start_index],
                   FonteDados.COLUNA_DEDUP: entidade[11 + start_index],
                   }

        all_columns = FonteDados.ALL_COLUMNS
        if len(all_columns) != len(_entity):
            raise Exception('[FonteDados] _load_entity DEVE conter todos colunas de ALL_COLUMNS! '
                            '\nentity[%s] \nall_columns[%s]' % (
                                _entity, all_columns))

        return _entity

    def find_by_id(self, id_fonte_dados, **kwargs):
        if not id_fonte_dados:
            raise ValueError('Informe id_fonte_dados corretamente! %s' % id_fonte_dados)

        _p = [id_fonte_dados]

        _stm = '''
                SELECT %s
                FROM %s
                WHERE idFonteDados = ?
            ''' % (_all_columns(FonteDados.ALL_COLUMNS), FonteDados.TABLE_NAME)

        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            _entity = FonteDados._load_entity(_)
            return _entity

        else:
            return None


class Extracao(object):
    TABLE_NAME = 'Superdigital_ETL._meta.ED_Extracao'

    ID_EXTRACAO = 'idExtracao'
    ID_FONTE_DADOS_ORIGEM = 'idFonteDadosOrigem'
    ID_FONTE_DADOS_DESTINO = 'idFonteDadosDestino'
    CRIADO_EM = 'criadoEm'
    ALTERADO_EM = 'alteradoEm'
    ATIVO = 'ativo'
    PERIODICIDADE = 'periodicidade'
    PRIORIDADE = 'prioridade'
    PERMITE_N_EXECUCOES_DIA = 'permiteNExecucoesDia'

    ALL_COLUMNS = [ID_EXTRACAO,
                   ID_FONTE_DADOS_ORIGEM,
                   ID_FONTE_DADOS_DESTINO,
                   CRIADO_EM,
                   ALTERADO_EM,
                   ATIVO,
                   PERIODICIDADE,
                   PRIORIDADE,
                   PERMITE_N_EXECUCOES_DIA, ]

    @staticmethod
    def _load_entity(entidade):
        _entity = {Extracao.ID_EXTRACAO: entidade[0],
                   Extracao.ID_FONTE_DADOS_ORIGEM: entidade[1],
                   Extracao.ID_FONTE_DADOS_DESTINO: entidade[2],
                   Extracao.CRIADO_EM: entidade[3],
                   Extracao.ALTERADO_EM: entidade[4],
                   Extracao.ATIVO: entidade[5],
                   Extracao.PERIODICIDADE: entidade[6],
                   Extracao.PRIORIDADE: entidade[7],
                   Extracao.PERMITE_N_EXECUCOES_DIA: entidade[8],
                   }

        all_columns = Extracao.ALL_COLUMNS
        if len(all_columns) != len(_entity):
            raise Exception('[Extracao] _load_entity DEVE conter todos colunas de ALL_COLUMNS! '
                            '\nentity[%s] \nall_columns[%s]' % (
                                _entity, all_columns))

        return _entity

    @staticmethod
    def find_by_id_with_FKs(id_extracao, **kwargs):
        if not id_extracao:
            raise ValueError('Informe id_extracao corretamente! %s' % id_extracao)

        _p = [id_extracao]

        columns = _all_columns([_all_columns(Extracao.ALL_COLUMNS, 'a'), _all_columns(FonteDados.ALL_COLUMNS, 'b'),
                                _all_columns(FonteDados.ALL_COLUMNS, 'c')])
        _stm = '''
            SELECT %s
            FROM %s a
            INNER JOIN %s b
                ON a.%s = b.%s
            INNER JOIN %s c
                ON a.%s = c.%s
            WHERE a.%s = ?
        ''' % (columns,
               Extracao.TABLE_NAME,
               FonteDados.TABLE_NAME, Extracao.ID_FONTE_DADOS_ORIGEM, FonteDados.ID_FONTE_DADOS,
               FonteDados.TABLE_NAME, Extracao.ID_FONTE_DADOS_DESTINO, FonteDados.ID_FONTE_DADOS,
               Extracao.ID_EXTRACAO)

        _idx_origem = len(Extracao.ALL_COLUMNS)
        _idx_destino = _idx_origem + len(FonteDados.ALL_COLUMNS)

        _cur = database.do_db_execute(_stm, parameters=_p, **kwargs)
        return Extracao._load_entity(_cur), \
               FonteDados._load_entity(_cur, _idx_origem), \
               FonteDados._load_entity(_cur, _idx_destino)

    @staticmethod
    def find_by_PeriodicidadeIsD_AtivoIsTrue_OrderByPrioridade_with_FKs(**kwargs):

        columns = _all_columns([_all_columns(Extracao.ALL_COLUMNS, 'a'), _all_columns(FonteDados.ALL_COLUMNS, 'b'),
                                _all_columns(FonteDados.ALL_COLUMNS, 'c')])
        _stm = '''
            SELECT %s
            FROM %s a
            INNER JOIN %s b
                ON a.%s = b.%s
            INNER JOIN %s c
                ON a.%s = c.%s
            WHERE a.periodicidade = 'D'
                AND a.ativo = 1
            ORDER BY a.%s
        ''' % (columns,
               Extracao.TABLE_NAME,
               FonteDados.TABLE_NAME, Extracao.ID_FONTE_DADOS_ORIGEM, FonteDados.ID_FONTE_DADOS,
               FonteDados.TABLE_NAME, Extracao.ID_FONTE_DADOS_DESTINO, FonteDados.ID_FONTE_DADOS,
               Extracao.PRIORIDADE)

        _ret = []  # array de tupla(Extracao, FonteDados, FonteDados)
        for _ in database.do_db_query_fetchall(_stm, **kwargs):
            _idx_origem = len(Extracao.ALL_COLUMNS)
            _idx_destino = _idx_origem + len(FonteDados.ALL_COLUMNS)

            _ret.append((Extracao._load_entity(_),
                         FonteDados._load_entity(_, _idx_origem),
                         FonteDados._load_entity(_, _idx_destino)
                         ))

        return _ret


class ExtracaoExecucao(object):
    TABLE_NAME = 'Superdigital_ETL._meta.ED_Extracao_Execucao'

    ID_EXTRACAO_EXECUCAO = 'idExtracaoExecucao'
    ID_EXTRACAO = 'idExtracao'
    VALOR_DELTA = 'valorDelta'
    VALOR_PROXIMO_DELTA = 'valorProximoDelta'
    QUERY_FONTE_DADOS = 'queryFonteDados'
    SQL_DELTA_DESTINO = 'sqlDeltaDestino'
    INICIO = 'inicio'
    TERMINO = 'termino'
    SUCESSO = 'sucesso'
    REGISTROS_CRIADOS = 'registrosCriados'
    REGISTROS_ALTERADOS: str = 'registrosAlterados'

    __ID_LOG_CARGA__ = 100000

    ALL_COLUMNS = [ID_EXTRACAO_EXECUCAO,
                   ID_EXTRACAO,
                   VALOR_DELTA,
                   VALOR_PROXIMO_DELTA,
                   QUERY_FONTE_DADOS,
                   SQL_DELTA_DESTINO,
                   INICIO,
                   TERMINO,
                   SUCESSO,
                   REGISTROS_CRIADOS,
                   REGISTROS_ALTERADOS,
                   ]

    @staticmethod
    def update(dict_update, dict_where, **kwargs):
        return _update(ExtracaoExecucao.TABLE_NAME, dict_update, dict_where, **kwargs)

    @staticmethod
    def _load_entity(entidade, start_index=0):

        _entity = {ExtracaoExecucao.ID_EXTRACAO_EXECUCAO: entidade[0 + start_index],
                   ExtracaoExecucao.ID_EXTRACAO: entidade[1 + start_index],
                   ExtracaoExecucao.VALOR_DELTA: entidade[2 + start_index],
                   ExtracaoExecucao.VALOR_PROXIMO_DELTA: entidade[3 + start_index],
                   ExtracaoExecucao.QUERY_FONTE_DADOS: entidade[4 + start_index],
                   ExtracaoExecucao.SQL_DELTA_DESTINO: entidade[5 + start_index],
                   ExtracaoExecucao.INICIO: entidade[6 + start_index],
                   ExtracaoExecucao.TERMINO: entidade[7 + start_index],
                   ExtracaoExecucao.SUCESSO: entidade[8 + start_index],
                   ExtracaoExecucao.REGISTROS_CRIADOS: entidade[9 + start_index],
                   ExtracaoExecucao.REGISTROS_ALTERADOS: entidade[10 + start_index],
                   }

        all_columns = ExtracaoExecucao.ALL_COLUMNS
        if len(all_columns) != len(_entity):
            raise Exception('[ExtracaoExecucao] _load_entity DEVE conter todos colunas de ALL_COLUMNS! '
                            '\nentity[%s] \nall_columns[%s]' % (
                                _entity, all_columns))

        return _entity

    @staticmethod
    def as_entity():
        return dict([(col, None) for col in ExtracaoExecucao.ALL_COLUMNS])

    @staticmethod
    def create(entidade, **kwargs):
        if not entidade:
            raise ValueError('Entidade necessaria! %s' % entidade)

        _exclude = [ExtracaoExecucao.ID_EXTRACAO_EXECUCAO, ExtracaoExecucao.INICIO]

        _p = _all_entity_values(entidade, ExtracaoExecucao.ALL_COLUMNS, exclude=_exclude)

        _stm = '''
            INSERT INTO %s
            (%s)
            VALUES(%s)
        ''' % (ExtracaoExecucao.TABLE_NAME,
               _all_columns(ExtracaoExecucao.ALL_COLUMNS,
                            exclude=_exclude),
               _all_columns_as_params(len(ExtracaoExecucao.ALL_COLUMNS) - len(_exclude)))

        database.do_db_execute(_stm, parameters=_p, **kwargs)

        return ExtracaoExecucao.find_last_by_IdExtracao(entidade[ExtracaoExecucao.ID_EXTRACAO], **kwargs)

    @staticmethod
    def find_by_id(id_extracao_execucao, **kwargs):
        if not id_extracao_execucao:
            raise ValueError('Informe ID corretamente! %s' % id_extracao_execucao)
        if isinstance(id_extracao_execucao, tuple):
            id_extracao_execucao = id_extracao_execucao[0]

        _p = [id_extracao_execucao]

        _stm = '''
            SELECT %s
            FROM %s
            WHERE %s = ?
        ''' % (
            _all_columns(ExtracaoExecucao.ALL_COLUMNS), ExtracaoExecucao.TABLE_NAME,
            ExtracaoExecucao.ID_EXTRACAO_EXECUCAO)

        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            _entity = ExtracaoExecucao._load_entity(_)
            return _entity

        else:
            return None

    @staticmethod
    def find_last_by_IdExtracao(id_extracao, **kwargs):
        if not id_extracao:
            raise ValueError('Informe id_extracao corretamente! %s' % id_extracao)

        _p = [id_extracao]

        _stm = '''
            SELECT %s
            FROM %s
            WHERE idExtracaoExecucao = (SELECT MAX(idExtracaoExecucao) 
                                        FROM %s 
                                        WHERE idExtracao = ?)
        ''' % (_all_columns(ExtracaoExecucao.ALL_COLUMNS),
               ExtracaoExecucao.TABLE_NAME,
               ExtracaoExecucao.TABLE_NAME)

        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            _entity = ExtracaoExecucao._load_entity(_)
            return _entity

        else:
            return None

    @staticmethod
    def find_last_by_IdExtracao_SucessoIsTrue(id_extracao, **kwargs):
        if not id_extracao:
            raise ValueError('Informe id_extracao corretamente! %s' % id_extracao)

        _p = [id_extracao]

        _stm = '''
            SELECT %s
            FROM %s
            WHERE idExtracaoExecucao = (SELECT MAX(idExtracaoExecucao) 
                                        FROM %s 
                                        WHERE idExtracao = ? 
                                            AND sucesso = %s)
        ''' % (_all_columns(ExtracaoExecucao.ALL_COLUMNS),
               ExtracaoExecucao.TABLE_NAME,
               ExtracaoExecucao.TABLE_NAME,
               Sucesso.OK.value)

        for _ in database.do_db_query_fetchall(_stm, parameters=_p, **kwargs):
            _entity = ExtracaoExecucao._load_entity(_)
            return _entity

        else:
            return None

    @staticmethod
    def has_SucessoIsINDEFINIDO_TerminoIsNull(**kwargs):

        _p = [Sucesso.INDEFINIDO.value]

        _stm = '''
            SELECT count(1)
            FROM %s
            WHERE %s = ?
                and %s is null
        ''' % (ExtracaoExecucao.TABLE_NAME,
               ExtracaoExecucao.SUCESSO,
               ExtracaoExecucao.TERMINO)

        _cur = database.do_db_query_fetchone(_stm, parameters=_p, **kwargs)
        return _cur[0]


# **********************************************************************
# PROCEDURES                                                           *
# **********************************************************************
class Procedure(ABC):

    def __init__(self, db, nome, use_use=False):
        self.db = db
        self.nome = nome
        self.use_use = use_use

    def exec(self, parameters, **kwargs):
        if parameters:
            assert isinstance(parameters, list)

        place_holders = _all_columns_as_params(len(parameters))

        _exec = 'EXEC %s %s' % (self.nome, place_holders)
        if self.use_use:
            _exec = '''
                USE %s
                SET NOCOUNT ON;
                %s
            ''' % (self.db, _exec)

        print('Executando procedure[%s] com os parametros[%s]\n\nSQL:\n%s' % (self.nome, parameters, _exec))

        _ret = database.do_db_execute(_exec, parameters=parameters, **kwargs)

        return _ret


class SegmentacaoCampanhas(Procedure):
    __instance = None
    DB = 'Superdigital_Work'
    NOME = '%s.dbo.SegmentacaoCampanhas' % DB

    @staticmethod
    def get_instance():
        if not SegmentacaoCampanhas.__instance:
            SegmentacaoCampanhas()
        return SegmentacaoCampanhas.__instance

    def __init__(self):
        if SegmentacaoCampanhas.__instance:
            raise SingletonException('A classe e um Singleton! Utilize `get_instance()` ao inves do construtor.')
        else:
            super().__init__(SegmentacaoCampanhas.DB, SegmentacaoCampanhas.NOME)
            SegmentacaoCampanhas.__instance = self

    def exec(self, id_definition, id_execution, **kwargs):
        return super().exec(parameters=[id_definition, id_execution], **kwargs)


class ExecExtracao(Procedure):
    __instance = None
    DB = 'Superdigital_ETL'
    NOME = '%s._meta.sp_ED_exec_extracao' % DB

    class Retorno():
        def __init__(self, total_registros, log_file, error_log):
            self.total_registros, self.log_file, self.error_log = total_registros, log_file, error_log

        def __str__(self):
            return "%s [%s, '%s', '%s']" % (
                self.__class__.__name__, self.total_registros, self.log_file, self.error_log)

        def as_dict(self):
            return dict(
                    (key, value if isinstance(value, (list, tuple)) else str(value) if value else value)
                    for (key, value) in self.__dict__.items()
            )

        def is_sucesso(self):
            return not self.error_log or self.total_registros < 0

    @staticmethod
    def get_instance():
        if not ExecExtracao.__instance:
            ExecExtracao()
        return ExecExtracao.__instance

    def __init__(self):
        if ExecExtracao.__instance:
            raise SingletonException('A classe e um Singleton! Utilize `get_instance()` ao inves do construtor.')
        else:
            super().__init__(ExecExtracao.DB, ExecExtracao.NOME, False)
            ExecExtracao.__instance = self

    def exec(self, entidade, origem_conn, origem_cmd, destino_conn, destino_tbl, destino_sanitizacao, **kwargs):
        _log = super().exec(parameters=[entidade,
                                        origem_conn, origem_cmd,
                                        destino_conn, destino_tbl, destino_sanitizacao],
                            **kwargs)
        if isinstance(_log, int):
            return ExecExtracao.Retorno(_log, None, 'Erro inesperado')
        return ExecExtracao.Retorno(_log[0], _log[1], _log[2])


class ProceduresCleansing(Procedure):
    DB = 'Superdigital'
    NOME = '%s.dbo.proc' % DB

    def __init__(self, procedure):
        ProceduresCleansing.NOME = '%s.%s' % (ProceduresCleansing.DB, procedure)
        super().__init__(ProceduresCleansing.DB, ProceduresCleansing.NOME)


class ProceduresProcessamento(Procedure):
    DB = 'Superdigital'
    NOME = '%s.dbo.proc' % DB

    def __init__(self, procedure):
        if len(procedure.split('.')) == 3:
            ProceduresProcessamento.NOME = procedure
        else:
            ProceduresProcessamento.NOME = '%s.%s' % (ProceduresProcessamento.DB, procedure)

        super().__init__(ProceduresProcessamento.DB, ProceduresProcessamento.NOME)


# class ExecSSIS_QA(Procedure):
#     __instance = None
#     DB = 'Superdigital_ETL'
#     NOME = '%s._meta.sp_ED_Exec_SSIS_QA' % DB
#
#     def __init__(self, db=DB, nome=NOME):
#         super().__init__(db, nome)
#
#     def exec(self, processo=None, parametros=None, **kwargs):
#         if not processo:
#             raise ValueError('Processo informado [%s] invalido!' % processo)
#
#         _param = [processo, parametros]
#
#         _exec = '''
#                     DECLARE @retorno int = 0
#                     EXEC @retorno = %s ?, ?
#                     SELECT @retorno
#                 ''' % self.nome
#         print('Executando procedure[%s] com os parametros[%s]\n\nSQL:\n%s\n' % (self.nome, _param, _exec))
#
#         _ret = database.do_db_callproc(_exec, parameters=_param, **kwargs)
#         return _ret[0]
#
#     def create_parameters_for_proc_call(self, **kwargs):
#         return '|'.join(['%s|%s' % (k, v) for k, v in kwargs.items() if v])
#
#
# class ExtracaoFonteDados(ExecSSIS_QA):
#     __instance = None
#
#     SSIS_EXTRACAO = 'ETL - Extracao'
#
#     @staticmethod
#     def get_instance():
#         if not ExtracaoFonteDados.__instance:
#             ExtracaoFonteDados()
#         return ExtracaoFonteDados.__instance
#
#     def __init__(self):
#         if ExtracaoFonteDados.__instance:
#             raise SingletonException('A classe e um Singleton! Utilize `get_instance()` ao inves do construtor.')
#         else:
#             super().__init__(ExtracaoFonteDados.DB, ExtracaoFonteDados.NOME)
#             ExtracaoFonteDados.__instance = self
#
#     def exec(self, id_extracao_execucao, **kwargs):
#         params_for_ssis = self.create_parameters_for_proc_call(id_extracao_execucao=id_extracao_execucao)
#
#         retorno = super().exec(processo=self.SSIS_EXTRACAO, parametros=params_for_ssis, **kwargs)
#
#         return SSIS_Status.SUCCEEDED == SSIS_Status(retorno)


class JobSQL(Procedure):
    __instance = None

    DB = 'msdb'
    NOME = 'dbo.sp_start_job'

    def __init__(self, job_name):
        super().__init__(self.DB, self.NOME)
        self.job_name = job_name

    def exec(self, **kwargs):
        if not self.job_name:
            raise ValueError('Job informado [%s] invalido!' % self.job_name)

        _param = [self.job_name]

        _exec = 'EXEC %s.%s @job_name=?' % (self.DB, self.NOME)
        print('Executando job[%s] \n\nSQL:\n%s\n' % (self.job_name, _exec))

        database.do_db_execute(_exec, parameters=_param, **kwargs)
        return True


class ExtracaoJob(JobSQL):
    __instance = None

    JOB_NAME = '[ED] Extracao'

    @staticmethod
    def get_instance():
        if not ExtracaoJob.__instance:
            ExtracaoJob()
        return ExtracaoJob.__instance

    def __init__(self):
        if ExtracaoJob.__instance:
            raise SingletonException('A classe e um Singleton! Utilize `get_instance()` ao inves do construtor.')
        else:
            super().__init__(ExtracaoJob.JOB_NAME)
            ExtracaoJob.__instance = self


class Procedures(object):
    """
    Essa class encapsula as procedures utilizadas no projeto. Caso precise executar uma procedure, utilize a instancia
    que estarah nessa classe.
    Por questoes do proprio python, a definicao das classes de procedure devem estar declarados ANTES dentro desse
    modulo (repositorios).
    """
    SegmentacaoCampanhas = SegmentacaoCampanhas.get_instance()
    # ExtracaoFonteDados = ExtracaoFonteDados.get_instance()
    ExtracaoJob = ExtracaoJob.get_instance()
    ExecExtracao = ExecExtracao.get_instance()
