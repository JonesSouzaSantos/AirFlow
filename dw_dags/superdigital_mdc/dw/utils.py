import logging
import traceback
from datetime import date, datetime

from allin import core, api, api_v2


# *****************************
# Classes
# *****************************
# class Config(object):
#     __instance = None
#
#     @staticmethod
#     def get_instance():
#         if not Config.__instance:
#             Config()
#         return Config.__instance
#
#     def __init__(self):
#         if Config.__instance:
#             raise SingletonException('A classe e um Singleton! Utilize `get_instance()` ao inves do construtor.')
#
#         super().__init__()
#         Config.__instance = self
#         self._c = configparser.ConfigParser()
#         self._c.optionxform = str
#         self._c.read('cm_n5.conf')


class Constantes(object):
    KEY__ALLIN_API_RUNTIME_INSTANCE = 'allin__api_runtime_instance'

    KEY__DATA_REPROCESSAMENTO_CARGA = 'data.reprocessamento.carga'

    KEY__ALLIN_API_TOKEN = core.Restful.KWARGS__API_TOKEN
    KEY__CONNECTION_STRING = 'database__connection_string'
    DEF__CONNECTION_STRING = 'DSN=MDC-AIRFLOW-MSSQL;UID=airflow.mdc;PWD=Super@123'

    DEF__HML_CONNECTION_STRING = 'DSN=HML-AIRFLOW-MSSQL;UID=airflow.mdc;PWD=Super@123'

    DEF__ORIGEM_EXTRACAO_CONNECTION_STRING = 'Server= SW06SQLAO.dvo.supermeios.com;Database=Super; User Id=BILink; Password=B11cb!gEMc3Z;multiSubnetFailover=True;ApplicationIntent=ReadOnly;Connect Timeout=60;'
    DEF__DESTINO_EXTRACAO_CONNECTION_STRING = 'Server = 10.1.50.85; Database = superdigital; Integrated Security = True;'

    KEY__MAX_TENTATIVAS_ENVIO_LISTA = 'allin__max_tentativas_envio_lista'
    DEF__MAX_TENTATIVAS_ENVIO_LISTA = 3

    DEF__ALLIN_API_USER = 'superdigital_allinapi'
    DEF__ALLIN_API_PASS = '5URuGuJA'
    DEF__ALLIN_API_PROXY = 'http://fernandoa.avanade:Super8080@@10.1.11.138:8080' ##'http://francisco.n5:@Dado1258@10.1.11.138:8080' ##'http://juliana.n5:jto@n52019s9@10.1.11.138:8080'

    DEF__ALLIN_API_V2_CLIENTID = '341'
    DEF__ALLIN_API_V2_CLIENTSECRET = 'b4d82535a2d50c2ad0d367bad1e97908'
    DEF__ALLIN_API_V2_USER = 'superdigital_tallinapi'
    DEF__ALLIN_API_V2_PASS = 'uv@SYvU6'

    DEF__FILESYSTEM_FOLDER = '/home/airflow/'

    DEF__HOLOS_FTP_HOST = '10.1.10.163'
    DEF__HOLOS_FTP_PORT = 22
    DEF__HOLOS_FTP_USER = 'svc.dashboardvendas'
    DEF__HOLOS_FTP_PASS = '123456Q!'
    DEF__HOLOS_FTP_HOMEDIR = 'Dashboard Vendas'

    DEF__RABBIT_PRD_HOST = '10.0.6.2'
    DEF__RABBIT_PRD_USER = 'mquser'
    DEF__RABBIT_PRD_PASS = 'mqpassSD'
    DEF__RABBIT_PRD_PORT = 5672
    DEF__RABBIT_PRD_VIRTUAL_HOST = 'superdigital'
    DEF__RABBIT_PRD_PUSH_QUEUE = 'Queue.Firebase.Campanhas'
    DEF__RABBIT_PRD_PUSH_EXCHANGE = ''  # blank
    DEF__RABBIT_PRD_PUSH_MESSAGE_TYPE = 'urn:message:Super.Eventos.PushCampanha:IPushCampanha'

    # configurations = Config.get_instance()

    @staticmethod
    def get(key):
        # TODO: implementar busca do DEF__ pela str informada
        raise NotImplementedError


class MDCFactory(object):

    @staticmethod
    def allin_api_by_version(versao, **kwargs):
        if versao == 1:
            return MDCFactory.allin_api(**kwargs)

        elif versao == 2:
            return MDCFactory.allin_api_v2(**kwargs)

        else:
            raise ValueError('Versao informada [%s] incorreta!' % versao)

    @staticmethod
    def allin_api(**kwargs):
        """
        Encapsula para o projeto a criacao do objeto de API da Allin, centralizando os parametros base e possibilitando
        a extensao dos parametros de inicializacao da mesma via `kwargs`
        :param kwargs: parametros nomeados que serao passados para inicializacao da instancia da API
        :return: _api
        """
        _api = api.API(username=Constantes.DEF__ALLIN_API_USER,
                       password=Constantes.DEF__ALLIN_API_PASS,
                       proxy_def=Constantes.DEF__ALLIN_API_PROXY,
                       **kwargs)

        return _api

    @staticmethod
    def allin_api_v2(**kwargs):
        """
        Encapsula para o projeto a criacao do objeto de API V2 da Allin, centralizando os parametros base e
        possibilitando a extensao dos parametros de inicializacao da mesma via `kwargs`
        :param kwargs: parametros nomeados que serao passados para inicializacao da instancia da API
        :return: _api
        """
        _api = api_v2.API_v2(client_id=Constantes.DEF__ALLIN_API_V2_CLIENTID,
                             client_secret=Constantes.DEF__ALLIN_API_V2_CLIENTSECRET,
                             username=Constantes.DEF__ALLIN_API_V2_USER,
                             password=Constantes.DEF__ALLIN_API_V2_PASS,
                             proxy_def=Constantes.DEF__ALLIN_API_PROXY,
                             **kwargs)

        return _api


class AbstractBusinessObject(object):
    def __init__(self, conn=None):
        self.conn = conn
        logging.basicConfig(level=logging.INFO)  # ERROR #INFO #DEBUG

    def execute_db(self, sql_statement):
        logging.info('[execute_db] Executando sql_statement {\n%s...%s\n}' % (sql_statement[0:50], sql_statement[-20:]))

        import superdigital_mdc.dw.database as database
        _ret = database.do_db_execute(sql_statement, conn=self.conn)

        logging.info('\t[execute_db] SUCESSO! Retorno [%s]' % _ret)

        return _ret

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()


# *****************************
# Exception
# *****************************
class SingletonException(Exception):
    """
    Excecao que deve ser disparada em caso de tentativa de construcao de uma classe singleton que nao
    via `get_instance()`.
    """


# *****************************
# Log utils
# *****************************

def stack_trace(e):
    return ''.join(traceback.format_tb(e.__traceback__))


def log_kwargs(**kwargs):
    logging.debug('KWARGS:\n%s' % ('\n'.join(['%s[%s]' % (k, v) for k, v in kwargs.items()])))


def time_this(funk):
    def time_it(*args, **kwargs):
        from datetime import datetime

        _ini = datetime.today()
        try:
            return funk(*args, **kwargs)

        finally:
            _fim = datetime.today()
            logging.info('\tFuncao [%s] EXECUTADA! Finalizacao levou [%s]. Inicio[%s] Fim[%s] ' % (
                funk, (_fim - _ini), _ini, _fim))

    return time_it


# *****************************
# utils
# *****************************
def from_int_2_bool(int_value):
    try:
        return int(int_value) == 1
    except:
        return False


def from_bool_2_int(bool_value):
    return True is bool_value


def to_int(value, default_value=0):
    try:
        return int(value)
    except:
        return default_value


def from_date_to_str(date_obj):
    if not date_obj:
        return None

    if isinstance(date_obj, str):
        return date_obj

    return date.strftime(date_obj, '%Y-%m-%d')


def from_datetime_to_str(date_obj):
    if not date_obj:
        return None

    if isinstance(date_obj, str):
        return date_obj

    return date.strftime(date_obj, '%Y-%m-%d %H:%M:%S')


def from_str_to_date(date_str):
    if not date_str:
        return None

    return datetime.strptime(date_str[:10], '%Y-%m-%d').date()


def from_str_to_datetime(date_str):
    if not date_str:
        return None

    return datetime.strptime(date_str[:19], '%Y-%m-%d %H:%M:%S')


def get_utc(date_ref):
    target_dt = date_ref.utcnow()
    posix_seconds = (target_dt - datetime(1970, 1, 1)).total_seconds()
    return target_dt, int(posix_seconds * 1000)


def get_now_utc():
    return get_utc(datetime.now())


def get_now_utc_datetime():
    return get_now_utc()[0]


def get_now_utc_posix():
    return get_now_utc()[1]
