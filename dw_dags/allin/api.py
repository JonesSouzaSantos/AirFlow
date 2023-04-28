import itertools
import logging
import time
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from functools import partial

from allin.core import Restful, AllinInvalidToken, JsonBuilder, InterruptableRequestRetryError


# ******************
# PAGEABLES
# ******************


class Pageable(ABC):
    def __init__(self, next_page_func=None, count=-1, **kwargs):
        self.cur_page = 0
        self.cur_reg = itertools.count(0)
        self._pages = [[]]

        self._offset = -1
        self._count = count

        self.next_page = next_page_func if next_page_func else self._default_next_page

    def count(self):
        return self._count

    def _default_next_page(self):
        if self.cur_page == 1:
            return False

        new_page = [{'id': 656}, {'id': 548}]

        self._append_page(new_page)

        return True

    def _append_page(self, new_page):
        self._pages.append(new_page)
        self.cur_page += 1
        self.cur_reg = itertools.count(0)

    def __iter__(self):
        return self

    def __next__(self):
        idx_r = next(self.cur_reg)
        if self.__is_eop(idx_r):
            # TODO FIXME https://trello.com/c/gOpN4xmc/120-bug-api-allin-cliques
            if self.bug_pagination:
                is_1st_page = self.cur_page > 0
                if is_1st_page:
                    logging.info('**BUG PAGINATION** '
                                 'Count[%s] CurPageNumItems[%s]' % (self.count(), len(self._pages[self.cur_page])))
                    raise StopIteration

            if self.next_page():
                idx_r = next(self.cur_reg)
            else:
                raise StopIteration

        return self._pages[self.cur_page][idx_r]

    def __is_eop(self, index):
        is_last_obj = index >= len(self._pages[self.cur_page])
        return is_last_obj

    def get_pages(self):
        return self._pages


class AbstractAlliNPageagle(Pageable):
    def __init__(self, api, builder, **kwargs):
        assert isinstance(api, API)

        self.api = api
        self.builder = builder
        self.bug_pagination = kwargs.get('bug_pagination') or False
        super().__init__(next_page_func=self._next_page, **kwargs)

    @abstractmethod
    def do_next_page(self):
        raise NotImplementedError

    def _next_page(self):
        offset = self.builder.pagination_offset
        range = self.builder.range

        logging.debug('next page called actual(cur_page[%s] count[%s] range[%s] offset[%s])' % (
            self.cur_page, self.count(), self.builder.get_range(), offset))

        _next_index = range[1] + 1
        if _next_index > self.count():
            logging.debug('no more data [count limit]')
            return False

        self.builder.range = [_next_index, range[1] + offset]

        page = self.do_next_page()
        if not page:
            logging.debug('no more data [page is null]')
            return False

        self._append_page(page)

        _new_page_len = len(page)
        logging.debug('new page added length[%s]' % _new_page_len)
        if _new_page_len > offset:
            logging.debug('new page range exceed offset, change offset to [%s]' % _new_page_len)
            offset = _new_page_len
            self.builder.pagination_offset = offset
            self.builder.range = [_next_index, range[1] + offset]

        return True


class AnaliticoEntregaPageable(AbstractAlliNPageagle):

    def __init__(self, api, builder, **kwargs):
        super().__init__(api, builder, **kwargs)

    def do_next_page(self):

        _page = self.api._pageable_delivery_analytic(self.builder, page=self.cur_page + 1)

        if _page:
            # count atualizado apos consulta
            self._count = int(_page['totalItems'])
            return _page[AnaliticoEntregaBuilder.RETORNO_ROOT]

        else:
            return None


class AnaliticoAberturaPageable(AbstractAlliNPageagle):

    def __init__(self, api, builder, **kwargs):
        super().__init__(api, builder, **kwargs)

    def do_next_page(self):
        return self.api._pageable_opening_analytic(self.builder)


class AnaliticoClickPageable(AbstractAlliNPageagle):

    def __init__(self, api, builder, **kwargs):
        # TODO FIXME https://trello.com/c/gOpN4xmc/120-bug-api-allin-cliques
        super().__init__(api, builder, bug_pagination=True, **kwargs)

    def do_next_page(self):
        return self.api._pageable_click_analytic(self.builder)


class ErrosPermanentesPageable(AbstractAlliNPageagle):

    def __init__(self, api, builder, **kwargs):
        super().__init__(api, builder, **kwargs)

    def do_next_page(self):
        return self.api._pageable_permanent_errors(self.builder)


# ******************
# BUILDERS
# ******************
class BuscaListasBuilder(object):
    RETORNO_ROOT = 'itensConteudo'
    RETORNO_ID_LISTA = 'itensConteudo_id_lista'
    RETORNO_NM_LISTA = 'itensConteudo_nm_lista'

    def __init__(self):
        self.nm_lista = None
        self.id_lista = None

    def set_nm_lista(self, nm_lista):
        self.nm_lista = nm_lista
        return self

    def set_id_lista(self, id_lista):
        self.id_lista = id_lista
        return self


class ListaEnvioEncerradoBuilder(object):
    EXCEPTION_FILTER = 'Periodo limitado a um dia'
    EXCEPTION_NO_DATA = 'Neste intervalo nao existem envios encerrados!'

    RETORNO_ROOT = 'itensConteudo'

    def __init__(self, dt_ref=None):
        self.dt_ref = dt_ref

    def hoje(self):
        self.dt_ref = date.today()
        return self

    def ontem(self):
        self.base_hoje(-1)
        return self

    def base_hoje(self, dias=0):
        self.dt_ref = date.today() + timedelta(days=dias)
        return self

    def set_dt_ref(self, data_ref):
        self.dt_ref = data_ref
        return self


class ErrosPermanentesBuilder(object):
    EXCEPTION_FILTER = 'Este envio nao existe!'
    EXCEPTION_NO_DATA = 'Esse envio nao possui emails com erros permanente.'
    EXCEPTION_REQUIRED = 'Informe o campanha_id'

    PAGINATION_OFFSET = 100

    RETORNO_ROOT = 'itensConteudo'

    def __init__(self, campanha_id, pagination_offset=PAGINATION_OFFSET):
        self.campanha_id = campanha_id
        self.pagination_offset = pagination_offset
        self.range = [-1, 0]

    def get_range(self):
        return ','.join(str(_) for _ in self.range)


class AnaliticoClickBuilder(object):
    EXCEPTION_FILTER = 'Este envio nao existe!'
    EXCEPTION_NO_DATA = 'Esse envio nao possui cliques!'
    EXCEPTION_REQUIRED = 'Informe o campanha_id'

    PAGINATION_OFFSET = 100

    RETORNO_ROOT_TOTAL = 'total'
    RETORNO_ROOT = 'itensConteudo'

    def __init__(self, campanha_id, pagination_offset=PAGINATION_OFFSET):
        self.campanha_id = campanha_id
        self.pagination_offset = pagination_offset
        self.range = [-1, 0]

    def get_range(self):
        return ','.join(str(_) for _ in self.range)


class AnaliticoAberturaBuilder(object):
    EXCEPTION_FILTER = 'Este envio nao existe!'
    EXCEPTION_NO_DATA = 'Esse envio nao possui aberturas!'
    EXCEPTION_REQUIRED = 'Informe o campanha_id'

    PAGINATION_OFFSET = 100

    RETORNO_ROOT_TOTAL = 'total'
    RETORNO_ROOT = 'itensConteudo'

    def __init__(self, campanha_id, pagination_offset=PAGINATION_OFFSET):
        self.campanha_id = campanha_id
        self.pagination_offset = pagination_offset
        self.range = [-1, 0]

    def get_range(self):
        return ','.join(str(_) for _ in self.range)


class AnaliticoEntregaBuilder(object):
    EXCEPTION_NO_DATA = 'Este envio nao existe!'
    EXCEPTION_REQUIRED = 'Informe o campanha_id'

    RETORNO_ROOT = 'items'

    def __init__(self, campanha_id=None):
        self.campanha_id = campanha_id
        self.pagination_offset = 10000
        self.range = [-1, 0]

    def get_range(self):
        return ','.join(str(_) for _ in self.range)


class RelatorioEntregaBuilder(object):
    EXCEPTION_NO_DATA = 'Este envio nao existe!'

    def __init__(self, campanha_id=None):
        self.campanha_id = campanha_id


class DadosEntregaPorDataBuilder(object):
    EXCEPTION_NO_DATA = 'Neste intervalo nao existem envios encerrados!'
    EXCEPTION_PERIODO = 'Periodo limitado a um dia'
    EXCEPTION_REQUIRED = 'Informe o campanha_id'

    RETORNO_ROOT = 'itensConteudo'

    def __init__(self, dt_ref=None):
        self.dt_ref = dt_ref

    def hoje(self):
        self.dt_ref = date.today()
        return self

    def ontem(self):
        self.dt_ref = date.today() - timedelta(days=1)
        return self

    def base_hoje(self, dias=0):
        self.dt_ref = date.today() + timedelta(days=dias)
        return self


class StatusUploadListaBuilder(object):
    STATUS_AGUARDANDO = "aguardando inicio"
    STATUS_INSERINDO = 'Inserindo dados na lista. 100%'
    STATUS_CONCLUIDO = "Upload Concluido"
    STATUS_NAO_ENCONTRADO = "Upload nao encontrado."

    RETORNO_ROOT = 'output'

    def __init__(self, id_log, check_timeout_seconds=300, check_interval=60):
        self.id_log = id_log
        self.check_timeout_seconds = check_timeout_seconds
        self.check_interval = check_interval


class CriaListaBuilder(JsonBuilder):
    TIPO_TEXTO = "texto"
    TIPO_NUMERO = "numero"
    TIPO_DATA = "data"

    STATUS_DUPLICADO = "O nome da lista nao esta disponivel. Dica: Utilize underline no lugar de espaco."

    def __init__(self, nm_lista, campos=[]):
        super().__init__(exclude_fields=None)
        self.nm_lista = nm_lista
        self.campos = campos

    def add_campo(self, nome, tipo, **kwargs):
        new_campo = {"nome": nome, "tipo": tipo}
        for _attr, _vlr in kwargs.items():
            new_campo[_attr] = _vlr

        self.campos.append(new_campo)

        return self


class UploadArquivoBuilder(JsonBuilder):
    ACAO_ARQUIVO_DELETE_REESCREVER = 1
    ACAO_ARQUIVO_ADICIONADOS = 2
    ACAO_ARQUIVO_ATUALIZAR = 3

    SEPARADOR_SEMICOLON = 1
    SEPARADOR_COMMA = 2
    SEPARADOR_TAB = 3

    QUALIFICADOR_NENHUM = 1
    QUALIFICADOR_ASPAS_DUPLAS = 2
    QUALIFICADOR_ASPAS_SIMPLES = 3

    EXCLUIDOS_NAO = 0
    EXCLUIDOS_SIM = 1

    def __init__(self):
        super(UploadArquivoBuilder, self).__init__(exclude_fields=['_file_path', '_file_name', '_file_content'])
        self.nm_lista = None
        self.campos_arquivo = None
        self._file_path = None
        self._file_name = None
        self._file_content = None

        self.acao_arquivo = UploadArquivoBuilder.ACAO_ARQUIVO_ATUALIZAR
        self.separador = UploadArquivoBuilder.SEPARADOR_SEMICOLON
        self.qualificador = UploadArquivoBuilder.QUALIFICADOR_NENHUM
        self.excluidos = UploadArquivoBuilder.EXCLUIDOS_NAO

    def set_nm_lista(self, value):
        self.nm_lista = value
        return self

    def set_campos_arquivo(self, value):
        self.campos_arquivo = value
        return self

    def set_file_path(self, value):
        self._file_path = value
        return self

    def set_file_name(self, value):
        self._file_name = value
        return self

    def set_file_content(self, value):
        self._file_content = value
        return self

    def set_acao_arquivo(self, value):
        self.acao_arquivo = value
        return self

    def set_separador(self, value):
        self.separador = value
        return self

    def set_qualificador(self, value):
        self.qualificador = value
        return self

    def set_excluidos(self, value):
        self.excluidos = value
        return self


# ******************
# API
# ******************

class API(Restful):
    INVALID_TOKEN_RESPONSE = "Ticket nao confere!"

    def __init__(self, username, password, **kwargs):
        super().__init__(**kwargs)
        logging.basicConfig(level=logging.INFO)

        self.username = username
        self.password = password
        self.token = None

        self._base_url = "https://painel02.allinmail.com.br/allinapi/" #"https://transacional.allin.com.br/api/" #"
        self.url_token = "%s?method=get_token&output=json" % self._base_url
        self.url_upload = "%s?method=uploadLista&output=json" % self._base_url
        self.url_create_list = "%s?method=criarLista" % self._base_url
        self.url_status_upload = "%s?method=getStatusUploadLista&output=json" % self._base_url
        self.url_reactivate_schedule = "%s?method=despausar&output=json" % self._base_url
        self.url_deliveries_by_date = "%s?method=get_encerradas_info&output=json" % self._base_url
        self.url_delivery_report = "%s?method=relatorio_envio&output=json" % self._base_url
        self.url_delivery_analytic = "%s?method=get_enviados&output=json" % self._base_url
        self.url_opening_analytic = "%s?method=get_abertura_envio&output=json" % self._base_url
        self.url_click_analytic = "%s?method=get_clique_envio&output=json" % self._base_url
        self.url_list_schedule = "%s?method=get_agendadas_info&output=json" % self._base_url
        self.url_list_ended = "%s?method=get_encerradas_info&output=json" % self._base_url
        self.url_permanent_errors = "%s?method=get_erros_permanentes&output=json" % self._base_url
        self.url_lists = "%s?method=getlistas&output=json" % self._base_url

    def check_for_invalid_token(self, r):
        if API.INVALID_TOKEN_RESPONSE in r.text:
            self.token = None
            raise AllinInvalidToken

    def start_session(self, **kwargs):
        self.token = kwargs.get(Restful.KWARGS__API_TOKEN)

        if self.token is None or self.token == '':
            if Restful.KWARGS__API_TOKEN in kwargs:
                kwargs.pop(Restful.KWARGS__API_TOKEN)  # token deve ser invalidado

            func = lambda _kwargs: self._get_access_token(**_kwargs)

            kwargs = self.set_skip_assert_session_started(True, **kwargs)

            r = self._do_request_with_retry(func, [200, 407], **kwargs)
            _json = self._get_as_json(r)

            self.token = _json['token']
            logging.info("Nova Sessao iniciada com token [%s]" % self.token)

        else:
            logging.info("Sessao iniciada REAPROVEITANDO token [%s]" % self.token)

        if self.token not in self._request_log.keys():
            self._request_log.update({self.token: {'calls': 0, 'post': 0, 'get': 0}})

        return self.token

    def end_session(self):
        super(API, self).end_session()

    def _get_access_token(self, **kwargs):
        _url = self.url_token
        logging.info("get_access_token [%s] user[%s]" % (_url, self.username))

        kwargs.update({"params": {
            'username': self.username,
            'password': self.password}
        })

        r = self._do_get(url=_url, verify=False, **kwargs)

        return r

    def create_list(self, builder, **kwargs):

        def do_this(*args):
            func = lambda _kwargs: self._create_list(builder, *args, **_kwargs)

            r = self._do_request_with_retry(func, 200, **kwargs)
            if r:
                if CriaListaBuilder.STATUS_DUPLICADO in r.text:
                    raise Exception('Nome para lista indisponivel!')

                else:
                    _json = self._get_as_json(r)
                    if "fail" in _json["output"]:
                        raise Exception("Falha ao criar lista. Retorno[%s]" % _json)

                    else:
                        return _json

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _create_list(self, create_builder, *args, **kwargs):
        assert isinstance(create_builder, CriaListaBuilder)

        __url = self.url_create_list
        logging.info("create_list [%s] list[%s]" % (__url, create_builder.nm_lista))

        kwargs.update({"params": {
            'token': self.token,
        }})

        kwargs.update({"data": {
            'dados': create_builder.build(),
        }})

        kwargs.update({"headers": {
            "content-type": "application/x-www-form-urlencoded"
        }})

        r = self._do_post(url=__url, verify=False, **kwargs)
        return r

    def upload_file(self, builder, **kwargs):

        def do_this(*args):
            func = lambda _kwargs: self._upload_file(builder, *args, **_kwargs)

            r = self._do_request_with_retry(func, 200, **kwargs)
            _json = self._get_as_json(r)
            if "fail" in _json["output"]:
                raise Exception("Falha ao realizar upload de arquivo. Retorno[%s]" % _json["output"]["fail"])

            return _json

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _upload_file(self, upload_builder, *args, **kwargs):
        assert isinstance(upload_builder, UploadArquivoBuilder)

        __url = self.url_upload

        logging.info("upload_file [%s] file[%s] path[%s]" % (__url,
                                                             upload_builder._file_name,
                                                             upload_builder._file_path))

        _file_name = None
        _file = None
        _use_content = upload_builder._file_name is None
        if _use_content:
            _file_name = upload_builder.nm_lista
            _file = upload_builder._file_content
        else:
            _file_path = upload_builder._file_path or ''
            _file_name = '%s%s' % (_file_path, upload_builder._file_name)
            _file = open(_file_name, 'rb')
            logging.info('Arquivo para upload [%s] %s' % (_file_name, _file))

        kwargs.update({"params": {
            'token': self.token,
            'arquivo': upload_builder._file_name  # caminho nao eh necessario
        }})

        kwargs.update({"files": [
            ('arquivo', _file),
            ('dados', (None, upload_builder.build(), 'text/plain')),  # None por questoes do `requests`
        ]})

        try:
            return self._do_post(url=__url, verify=False, **kwargs)
        except Exception as e:
            logging.error(e)
            raise e

    def batch_check_status_upload_file(self, builders, interval=1, **kwargs):
        logging.info("batch_check_status_upload_file builders[%s]" % (len(builders)))

        from concurrent.futures.thread import ThreadPoolExecutor
        with ThreadPoolExecutor(thread_name_prefix='check_upload_status_') as executor:
            return [executor.submit(self.check_status_upload_file, builder, **kwargs) for builder in builders]


    def check_status_upload_file(self, builder, **kwargs):
        assert isinstance(builder, StatusUploadListaBuilder) or isinstance(builder[1], StatusUploadListaBuilder)
        if isinstance(builder, tuple):
            builder = builder[1]

        logging.info("check_status_upload_file id_log[%s]" % (builder.id_log))
        _ini = datetime.now()

        _check = None
        for _check in iter(partial(self.status_upload_file, builder, **kwargs), ''):

            _status = None
            try:
                _status = _check[0]["fail"]
            except KeyError:
                _status = _check["status"]

            logging.info("\n\tid_log[%s] status[%s]\n" % (builder.id_log, _status))

            is_status_concluido = _status in [StatusUploadListaBuilder.STATUS_CONCLUIDO,
                                              StatusUploadListaBuilder.STATUS_NAO_ENCONTRADO]
            if is_status_concluido:
                return _check

            _now = datetime.now()
            _time = _now - _ini

            is_timeout = builder.check_timeout_seconds < _time.seconds
            if is_timeout:
                _msg = 'Timeout ao verificar status do upload de arquivo! id_log[%s] timeout[%s]' % (
                    builder.id_log, builder.check_timeout_seconds)

                logging.info(_msg)
                raise TimeoutError(_msg)

            # pare um pouquinho, pense um pouquinho...
            time.sleep(builder.check_interval)

    def status_upload_file(self, builder, **kwargs):

        def do_this(*args):
            func = lambda _kwargs: self._status_upload_file(builder, *args, **_kwargs)

            r = self._do_request_with_retry(func, 200, **kwargs)
            _json = self._get_as_json(r)

            try:
                if "fail" in _json[StatusUploadListaBuilder.RETORNO_ROOT][0]:
                    raise InterruptableRequestRetryError("Falha ao verificar status de upload. "
                                                         "IdLog[%s] Retorno[%s]" % (builder.id_log, _json))

            except KeyError:
                # Estrutura 'output[0].fail' existe apenas em caso de erro
                pass

            return _json[StatusUploadListaBuilder.RETORNO_ROOT]

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _status_upload_file(self, status_upload_builder, *args, **kwargs):
        assert isinstance(status_upload_builder, StatusUploadListaBuilder)

        __url = self.url_status_upload
        logging.info("status_upload_file [%s] id_log[%s]" % (__url, status_upload_builder.id_log))

        kwargs.update({"params": {
            'token': self.token,
            'id_log': status_upload_builder.id_log
        }})

        r = self._do_get(url=__url, verify=False, **kwargs)
        return r

    def get_delivery_info_by_date(self, builder, **kwargs):

        def do_this(*args):
            func = lambda _kwargs: self._get_delivery_info_by_date(builder, *args, **_kwargs)

            r = self._do_request_with_retry(func, 200, **kwargs)
            _json = self._get_as_json(r)
            if "exception" in _json:
                if DadosEntregaPorDataBuilder.EXCEPTION_NO_DATA.count(_json['exception']) > 0:
                    return None

                else:
                    if (DadosEntregaPorDataBuilder.EXCEPTION_PERIODO.count(_json['exception']) > 0) or \
                            (DadosEntregaPorDataBuilder.EXCEPTION_REQUIRED.count(_json['exception']) > 0):
                        raise Exception("Falha ao buscar envios encerrados. Retorno[%s]" % _json)

                    else:
                        raise Exception("Erro inesperado ao buscar envios encerrados. Retorno[%s]" % _json)

            return _json[DadosEntregaPorDataBuilder.RETORNO_ROOT]

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _get_delivery_info_by_date(self, delivery_info_builder, *args, **kwargs):

        assert isinstance(delivery_info_builder, DadosEntregaPorDataBuilder)

        __url = self.url_deliveries_by_date
        logging.info("get_delivery_info_by_date [%s] dt_fim[%s]" % (__url, delivery_info_builder.dt_ref))

        kwargs.update({"params": {
            'token': self.token,
            'dt_inicio': delivery_info_builder.dt_ref,
            'dt_fim': delivery_info_builder.dt_ref,
        }})

        r = self._do_get(url=__url, verify=False, **kwargs)
        return r

    def get_delivery_report(self, builder, **kwargs):

        def do_this(*args):
            func = lambda _kwargs: self._get_delivery_report(builder, *args, **_kwargs)

            r = self._do_request_with_retry(func, 200, **kwargs)
            _json = self._get_as_json(r)
            if "exception" in _json:
                if RelatorioEntregaBuilder.EXCEPTION_NO_DATA.count(_json['exception']) > 0:
                    return None
                else:
                    if DadosEntregaPorDataBuilder.EXCEPTION_REQUIRED.count(_json['exception']) > 0:
                        raise Exception("Falha ao buscar envios encerrados. Retorno[%s]" % _json)
                    else:
                        raise Exception("Erro inesperado ao buscar envios encerrados. Retorno[%s]" % _json)

            return _json

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _get_delivery_report(self, delivery_report_builder, *args, **kwargs):
        assert isinstance(delivery_report_builder, RelatorioEntregaBuilder)

        __url = self.url_delivery_report
        logging.info("get_delivery_report [%s] campanha_id[%s]" % (__url, delivery_report_builder.campanha_id))

        kwargs.update({"params": {
            'token': self.token,
            'campanha_id': delivery_report_builder.campanha_id,
        }})

        r = self._do_get(url=__url, verify=False, **kwargs)
        return r

    def get_delivery_analytic(self, builder, **kwargs):

        def do_this(*args):
            assert isinstance(builder, AnaliticoEntregaBuilder)

            import sys
            pageble = AnaliticoEntregaPageable(self, builder=builder, count=sys.maxsize, **kwargs)

            return pageble

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _pageable_delivery_analytic(self, builder, **kwargs):

        def do_this(*args):
            func = lambda _kwargs: self._inner_pageable_delivery_analytic(builder, *args, **_kwargs)

            r = self._do_request_with_retry(func, 200, **kwargs)
            _json = self._get_as_json(r)
            if "exception" in _json:
                _is_no_data = AnaliticoEntregaBuilder.EXCEPTION_NO_DATA.count(_json['exception']) > 0
                if _is_no_data:
                    return None
                else:
                    if AnaliticoEntregaBuilder.EXCEPTION_REQUIRED.count(_json['exception']) > 0:
                        raise Exception("Falha ao buscar analitico de entrega. Retorno[%s]" % _json)
                    else:
                        raise Exception("Erro inesperado ao buscar analitico de entrega. Retorno[%s]" % _json)
            else:
                _is_eop = _json['totalResult'] == 0
                if _is_eop:
                    return None

            return _json

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _inner_pageable_delivery_analytic(self, delivery_delivery_builder, *args, **kwargs):
        assert isinstance(delivery_delivery_builder, AnaliticoEntregaBuilder)

        __url = self.url_delivery_analytic
        logging.info(
                "_inner_pageable_delivery_analytic [%s] campanha_id[%s]" % (
                    __url, delivery_delivery_builder.campanha_id))

        kwargs.update({"params": {
            'token': self.token,
            'campanha_id': delivery_delivery_builder.campanha_id,
            'page': kwargs.pop('page')
        }})

        r = self._do_get(url=__url, verify=False, **kwargs)
        return r

    def _get_delivery_analytic(self, delivery_analytic_builder, *args, **kwargs):
        assert isinstance(delivery_analytic_builder, AnaliticoEntregaBuilder)

        __url = self.url_delivery_analytic
        logging.info("get_delivery_analytic [%s] campanha_id[%s]" % (__url, delivery_analytic_builder.campanha_id))

        kwargs.update({"params": {
            'token': self.token,
            'campanha_id': delivery_analytic_builder.campanha_id,
        }})

        r = self._do_get(url=__url, verify=False, **kwargs)
        return r

    def get_opening_analytic(self, builder, **kwargs):

        def do_this(*args):
            assert isinstance(builder, AnaliticoAberturaBuilder)

            count = self._get_opening_analytic_total(builder, *args, **kwargs)
            logging.info('[get_opening_analytic] Total [%s]' % count)

            pageble = AnaliticoAberturaPageable(self, builder=builder, count=count, **kwargs)

            return pageble

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _pageable_opening_analytic(self, builder, **kwargs):

        def do_this(*args):
            func = lambda _kwargs: self._inner_pageable_opening_analytic(builder, *args, **_kwargs)

            r = self._do_request_with_retry(func, 200, **kwargs)
            _json = self._get_as_json(r)
            if "exception" in _json:
                _is_no_data = AnaliticoAberturaBuilder.EXCEPTION_NO_DATA.count(_json['exception']) > 0
                _is_filtered = AnaliticoAberturaBuilder.EXCEPTION_FILTER.count(_json['exception']) > 0
                if _is_no_data or _is_filtered:
                    return None
                else:
                    if AnaliticoAberturaBuilder.EXCEPTION_REQUIRED.count(_json['exception']) > 0:
                        raise Exception("Falha ao buscar analitico de aberturas. Retorno[%s]" % _json)
                    else:
                        raise Exception("Erro inesperado ao buscar analitico de aberturas. Retorno[%s]" % _json)

            return _json[AnaliticoAberturaBuilder.RETORNO_ROOT]

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _inner_pageable_opening_analytic(self, opening_report_builder, *args, **kwargs):
        assert isinstance(opening_report_builder, AnaliticoAberturaBuilder)

        __url = self.url_opening_analytic
        logging.info("pageable_opening_analytic [%s] campanha_id[%s] range[%s]" % (
            __url, opening_report_builder.campanha_id, opening_report_builder.range))

        kwargs.update({"params": {
            'token': self.token,
            'campanha_id': opening_report_builder.campanha_id,
            'range': opening_report_builder.get_range()
        }})

        r = self._do_get(url=__url, verify=False, **kwargs)
        return r

    def _get_opening_analytic_total(self, builder, *args, **kwargs):
        func = lambda _kwargs: self._inner_get_opening_analytic_total(builder, **_kwargs)

        r = self._do_request_with_retry(func, 200, **kwargs)
        _json = self._get_as_json(r)
        if "exception" in _json:
            _is_no_data = AnaliticoAberturaBuilder.EXCEPTION_NO_DATA.count(_json['exception']) > 0
            _is_filtered = AnaliticoAberturaBuilder.EXCEPTION_FILTER.count(_json['exception']) > 0
            if _is_no_data or _is_filtered:
                return 0
            else:
                if AnaliticoAberturaBuilder.EXCEPTION_REQUIRED.count(_json['exception']) > 0:
                    raise Exception("Falha ao buscar total de registros do relatorio de aberturas. Retorno[%s]" % _json)
                else:
                    raise Exception(
                            "Erro inesperado ao buscar total de registros do relatorio de aberturas. Retorno[%s]" % _json)

        _total = _json[AnaliticoAberturaBuilder.RETORNO_ROOT_TOTAL]
        return _total if _total else 0

    def _inner_get_opening_analytic_total(self, opening_analytic_builder, **kwargs):
        assert isinstance(opening_analytic_builder, AnaliticoAberturaBuilder)

        __url = self.url_opening_analytic
        logging.info("get_opening_report_total [%s] campanha_id[%s]" % (__url, opening_analytic_builder.campanha_id))

        kwargs.update({"params": {
            'token': self.token,
            'campanha_id': opening_analytic_builder.campanha_id,
            'total': 1
        }})

        r = self._do_get(url=__url, verify=False, **kwargs)
        return r

    def get_click_analytic(self, builder, **kwargs):

        def do_this(*args):
            assert isinstance(builder, AnaliticoClickBuilder)

            count = self._get_click_analytic_total(builder, *args, **kwargs)
            logging.info('[get_click_analytic] Total [%s]' % count)

            pageble = AnaliticoClickPageable(self, builder=builder, count=count, **kwargs)

            return pageble

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _get_click_analytic_total(self, builder, *args, **kwargs):
        func = lambda _kwargs: self._inner_get_click_analytic_total(builder, **_kwargs)

        r = self._do_request_with_retry(func, 200, **kwargs)
        _json = self._get_as_json(r)
        if "exception" in _json:
            _is_no_data = AnaliticoClickBuilder.EXCEPTION_NO_DATA.count(_json['exception']) > 0
            _is_filtered = AnaliticoClickBuilder.EXCEPTION_FILTER.count(_json['exception']) > 0
            if _is_no_data or _is_filtered:
                return 0
            else:
                if AnaliticoClickBuilder.EXCEPTION_REQUIRED.count(_json['exception']) > 0:
                    raise Exception("Falha ao buscar total de registros do relatorio de clicks. Retorno[%s]" % _json)
                else:
                    raise Exception(
                            "Erro inesperado ao buscar total de registros do relatorio de clicks. Retorno[%s]" % _json)

        _total = _json[AnaliticoClickBuilder.RETORNO_ROOT_TOTAL]
        return _total if _total else 0

    def _inner_get_click_analytic_total(self, click_report_builder, **kwargs):
        assert isinstance(click_report_builder, AnaliticoClickBuilder)

        __url = self.url_click_analytic
        logging.info("get_click_report_total [%s] campanha_id[%s]" % (__url, click_report_builder.campanha_id))

        kwargs.update({"params": {
            'token': self.token,
            'campanha_id': click_report_builder.campanha_id,
            'total': '1'
        }})

        r = self._do_get(url=__url, verify=False, **kwargs)
        return r

    def _pageable_click_analytic(self, builder, **kwargs):

        def do_this(*args):
            func = lambda _kwargs: self._inner_pageable_click_analytic(builder, *args, **_kwargs)

            r = self._do_request_with_retry(func, 200, **kwargs)
            _json = self._get_as_json(r)
            if "exception" in _json:
                _is_no_data = AnaliticoClickBuilder.EXCEPTION_NO_DATA.count(_json['exception']) > 0
                _is_filtered = AnaliticoClickBuilder.EXCEPTION_FILTER.count(_json['exception']) > 0
                if _is_no_data or _is_filtered:
                    return None
                else:
                    if AnaliticoClickBuilder.EXCEPTION_REQUIRED.count(_json['exception']) > 0:
                        raise Exception("Falha ao buscar relatorio de aberturas. Retorno[%s]" % _json)
                    else:
                        raise Exception("Erro inesperado ao buscar relatorio de aberturas. Retorno[%s]" % _json)

            return _json[AnaliticoClickBuilder.RETORNO_ROOT]

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _inner_pageable_click_analytic(self, click_report_builder, *args, **kwargs):
        assert isinstance(click_report_builder, AnaliticoClickBuilder)

        __url = self.url_click_analytic
        logging.info("pageable_click_report [%s] campanha_id[%s] range[%s]" % (
            __url, click_report_builder.campanha_id, click_report_builder.range))

        kwargs.update({"params": {
            'token': self.token,
            'campanha_id': click_report_builder.campanha_id,
            'range': click_report_builder.get_range()
        }})

        r = self._do_get(url=__url, verify=False, **kwargs)
        return r

    def get_permanent_errors(self, builder, **kwargs):

        def do_this(*args):
            assert isinstance(builder, ErrosPermanentesBuilder)

            count = self._get_permanent_errors_total(builder, **kwargs)
            logging.info('[get_permanent_errors] Total [%s]' % count)

            pageble = ErrosPermanentesPageable(self, builder=builder, count=count, **kwargs)

            return pageble

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _get_permanent_errors_total(self, builder, **kwargs):
        func = lambda _kwargs: self._inner_get_permanent_errors_total(builder, **_kwargs)

        r = self._do_request_with_retry(func, 200, **kwargs)

        # retorno vem cru porem com aspas duplas, ex: "16"
        if r and r.text.strip('"').isnumeric():
            return int(r.text.strip('"'))

        _json = self._get_as_json(r)
        if "exception" in _json:
            _is_no_data = ErrosPermanentesBuilder.EXCEPTION_NO_DATA.count(_json['exception']) > 0
            _is_filtered = ErrosPermanentesBuilder.EXCEPTION_FILTER.count(_json['exception']) > 0
            if _is_no_data or _is_filtered:
                return 0
            else:
                if ErrosPermanentesBuilder.EXCEPTION_REQUIRED.count(_json['exception']) > 0:
                    raise Exception("Falha ao buscar total de registros do relatorio de clicks. Retorno[%s]" % _json)
                else:
                    raise Exception(
                            "Erro inesperado ao buscar total de registros do relatorio de clicks. Retorno[%s]" % _json)

    def _inner_get_permanent_errors_total(self, perm_errors_builder, **kwargs):
        assert isinstance(perm_errors_builder, ErrosPermanentesBuilder)

        __url = self.url_permanent_errors
        logging.info("get_permanent_errors_total [%s] campanha_id[%s]" % (__url, perm_errors_builder.campanha_id))

        kwargs.update({"params": {
            'token': self.token,
            'campanha_id': perm_errors_builder.campanha_id,
        }})

        r = self._do_get(url=__url, verify=False, **kwargs)
        return r

    def _pageable_permanent_errors(self, builder, **kwargs):

        def do_this(*args):
            func = lambda _kwargs: self._inner_pageable_permanent_errors(builder, *args, **_kwargs)

            r = self._do_request_with_retry(func, 200, **kwargs)
            _json = self._get_as_json(r)
            if "exception" in _json:
                _is_no_data = ErrosPermanentesBuilder.EXCEPTION_NO_DATA.count(_json['exception']) > 0
                _is_filtered = ErrosPermanentesBuilder.EXCEPTION_FILTER.count(_json['exception']) > 0
                if _is_no_data or _is_filtered:
                    return None
                else:
                    if ErrosPermanentesBuilder.EXCEPTION_REQUIRED.count(_json['exception']) > 0:
                        raise Exception("Falha ao buscar relatorio de aberturas. Retorno[%s]" % _json)
                    else:
                        raise Exception("Erro inesperado ao buscar relatorio de aberturas. Retorno[%s]" % _json)

            return _json[ErrosPermanentesBuilder.RETORNO_ROOT]

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _inner_pageable_permanent_errors(self, perm_errors_builder, *args, **kwargs):
        assert isinstance(perm_errors_builder, ErrosPermanentesBuilder)

        __url = self.url_permanent_errors
        logging.info("pageable_permanent_errors [%s] campanha_id[%s] range[%s]" % (
            __url, perm_errors_builder.campanha_id, perm_errors_builder.range))

        kwargs.update({"params": {
            'token': self.token,
            'campanha_id': perm_errors_builder.campanha_id,
            'range': perm_errors_builder.get_range()
        }})

        r = self._do_get(url=__url, verify=False, **kwargs)
        return r

    def list_ended(self, builder, **kwargs):

        def do_this(*args):
            func = lambda _kwargs: self._list_ended(builder, *args, **_kwargs)

            r = self._do_request_with_retry(func, 200, **kwargs)
            _json = self._get_as_json(r)
            if "exception" in _json:
                _is_no_data = ListaEnvioEncerradoBuilder.EXCEPTION_NO_DATA.count(_json['exception']) > 0
                _is_filtered = ListaEnvioEncerradoBuilder.EXCEPTION_FILTER.count(_json['exception']) > 0
                if _is_no_data or _is_filtered:
                    return []
                else:
                    raise Exception("Erro inesperado ao buscar envios encerrados. Retorno[%s]" % _json)

            return _json[ListaEnvioEncerradoBuilder.RETORNO_ROOT]

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _list_ended(self, builder, *args, **kwargs):
        assert isinstance(builder, ListaEnvioEncerradoBuilder)

        _url = self.url_list_ended
        logging.info("list_ended [%s] [%s]" % (_url, builder.dt_ref))

        kwargs.update({"params": {
            'token': self.token,
            'dt_inicio': builder.dt_ref,
            'dt_fim': builder.dt_ref,
        }})

        r = self._do_get(url=_url, verify=False, **kwargs)
        return r

    def get_lists(self, builder, **kwargs):
        assert isinstance(builder, BuscaListasBuilder)

        def do_this(*args):
            func = lambda _kwargs: self._get_lists(builder, *args, **_kwargs)

            r = self._do_request_with_retry(func, 200, **kwargs)
            _json = self._get_as_json(r)
            if "exception" in _json:
                raise Exception("Erro inesperado ao buscar listas. Retorno[%s]" % _json)

            # print(_json)
            if builder.nm_lista or builder.id_lista:
                for _ in _json[BuscaListasBuilder.RETORNO_ROOT]:
                    if _[BuscaListasBuilder.RETORNO_ID_LISTA] == builder.id_lista:
                        return [_]
                    elif _[BuscaListasBuilder.RETORNO_NM_LISTA] == builder.nm_lista:
                        return [_]
                else:
                    return []
            else:
                return _json[BuscaListasBuilder.RETORNO_ROOT]

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _get_lists(self, builder, *args, **kwargs):
        assert isinstance(builder, BuscaListasBuilder)

        __url = self.url_lists
        logging.info("get_lists [%s]" % __url)

        kwargs.update({"params": {
            'token': self.token,
        }})

        r = self._do_get(url=__url, verify=False, **kwargs)
        return r
