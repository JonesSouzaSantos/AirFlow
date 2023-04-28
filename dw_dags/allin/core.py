import logging
import time
from abc import ABC, abstractmethod

import requests


class JsonBuilder(object):
    """
    Builder padrao para gerar json atraves dos atributos da classe, excluindo os atributos listados em `exclude_fields`
    """

    def __init__(self, exclude_fields):
        self.exclude_fields = exclude_fields or []
        self.exclude_fields.append('exclude_fields')

    def as_dict(self):
        return dict(
                (key, value if isinstance(value, (list, tuple)) else str(value) if value else value)
                for (key, value) in self.__dict__.items()
                if key not in self.exclude_fields
        )

    def build(self):
        return str(self.as_dict()).replace('\'', "\"")


class AllinInvalidToken(Exception):
    """
    Excecao caso o token utilizado para as requisicoes nao for mais valido.
    """


class FallbackStrategyError(Exception):
    """A estrategia nao foi muito bem, logo..."""

    def __init__(self, cause, *args):
        super().__init__(*args)
        self.cause = cause


class InterruptableRequestRetryError(Exception):
    """
    Esse erro determina que nao eh necessario realizar o retry da request automaticamente se essa for a
    excecao capturada
    """


class RequestRetryVO(object):
    def __init__(self, max_retry=None):
        self.__DEFAULT_NIFI_RETRY_TIMES = 0
        self.__max_retry = self.__DEFAULT_NIFI_RETRY_TIMES if max_retry is None else max_retry

    def get_max_retry(self):
        return self.__max_retry

    def set_max_retry(self, number_of_retries):
        number_of_retries = int(number_of_retries)
        if number_of_retries > -1:
            self.__max_retry = number_of_retries
        else:
            self.__max_retry = self.__DEFAULT_NIFI_RETRY_TIMES


class FallbackStrategy(object):
    """
    Conjunto de estrategias (que funcionam mais ou menos como um `method decorator`) para as chamadas a API.
    """

    class Strategy(ABC):
        """
        Contrato de uma estrategia
        """

        @abstractmethod
        def gimme_what_2_do(self, closure, api):
            raise NotImplementedError

    class AsIs(Strategy):
        """
        Apenas executa a funcao normalmente
        """

        def gimme_what_2_do(self, closure, *args):
            logging.debug('FallbackStrategy.AsIs')

            return closure(*args)

    class ListOf(Strategy):
        """
        Aplica todas as estrategias informadas no construtor
        """

        def __init__(self, strategies):
            self._strategies = strategies or []

        def gimme_what_2_do(self, closure, api):
            logging.debug('FallbackStrategy.ListOf')

            nested = self._strategies.copy()
            nested.reverse()

            outer = nested.pop(0).gimme_what_2_do

            return self.do_nested(closure, outer, nested, api)

        def do_nested(self, inner, outer, strategies, *args, **kwargs):
            def _new_inner(*a, **k):
                return outer(inner, *a, **k)

            if len(strategies) > 0:
                _new_outer = strategies.pop(0).gimme_what_2_do
                return self.do_nested(_new_inner, _new_outer, strategies, *args, **kwargs)
            else:
                return _new_inner(*args, **kwargs)

    class WaitAndRetry(Strategy):
        """
        Estrategia que, em caso de falha, aguarda o tempo informado e reexecuta a funcao decorada,
        respeitando o limite maximo de retries
        """

        def __init__(self, wait_sec=61, max_retry=3):
            self.wait_sec = wait_sec
            self.max_retry = max_retry

        def gimme_what_2_do(self, closure, *args):
            logging.debug('FallbackStrategy.WaitAndRetry')

            _count = 0
            while True:
                try:
                    return closure(*args)

                except InterruptableRequestRetryError as e:
                    raise e
                except FallbackStrategyError as e:
                    raise e

                except Exception as e:
                    _count += 1
                    if _count == self.max_retry:
                        logging.error('Final Attempt failed, cause[%s].' % e)
                        raise FallbackStrategyError(e)

                    else:
                        logging.info(
                                'Attempt failed, cause[%s]. Waiting [%s] secs and retrying for the [%s] time ...' % (
                                    e, self.wait_sec, _count))
                        time.sleep(self.wait_sec)
                        logging.info('Retrying by strategy...')

    class AutoReconnect(Strategy):
        """
        Estrategia que, em caso de erro de token invalido, revalida a sessao e executa a funcao decorada novamente
        """

        def gimme_what_2_do(self, closure, *args):
            logging.debug('FallbackStrategy.AutoReconnect')

            while True:
                _retry = False
                try:
                    return closure(*args)

                except FallbackStrategyError as e:
                    if not isinstance(e.cause, AllinInvalidToken):
                        raise e

                except AllinInvalidToken as e:
                    pass

                logging.info('Sessao foi invalidada, criando novo TOKEN...')

                api = args[0]
                assert isinstance(api, Restful)
                api.start_session()


class Restful(object):
    _SKIP_ASSERT_SESSION_STARTED = 'skip_assert_session_started'
    __KWARGS_EXCLUDE = [_SKIP_ASSERT_SESSION_STARTED]

    KWARGS__API_TOKEN = 'allin__api_token'

    INVALID_TOKEN_RESPONSE = "Ticket nao confere!"

    def __init__(self, **kwargs):
        logging.basicConfig(level=logging.INFO)

        self.max_retry = kwargs.get('max_retry') or 3
        self.proxy_def = kwargs.get('proxy_def') or None
        self.fallback_strategy = kwargs.get('fallback_strategy') or FallbackStrategy.AsIs()

        self.retry_vo = RequestRetryVO(max_retry=self.max_retry)

        self.token = None
        self._request_log = {}

    def _do_post(self, url, data=None, json=None, **kwargs):
        self._add_request('post')

        logging.debug("POST: %s" % url)

        _session = self._get_default_session()
        ret = _session.post(url, data=data, json=json, **kwargs)
        return ret

    def _do_get(self, url, **kwargs):
        self._add_request('get')

        ret = None
        logging.debug("GET: %s" % url)
        # print("GET: %s [%s]" % (url, kwargs))

        _session = self._get_default_session()
        ret = _session.get(url, **kwargs)
        return ret

    def _get_default_session(self):
        s = requests.Session()
        s.trust_env = False

        if self.proxy_def:
            s.proxies = {'http': self.proxy_def,
                         'https': self.proxy_def}

        http_adapter = requests.adapters.HTTPAdapter(max_retries=3)
        s.mount('http://', http_adapter)
        s.mount('https://', http_adapter)

        return s

    def _get_as_json(self, response):
        if response is None:
            raise Warning('Response is [%s]' % response)

        try:
            _json = response.json()
            if isinstance(_json, str):
                import json
                _json = json.dumps(_json)
                if isinstance(_json, str):
                    raise Warning(_json)
            return _json

        except:
            raise Warning('Json return expected! Response[%s]' % response.text)

    def _update_kwargs_4_request(self, **kwargs):
        ret = kwargs.copy()
        for _param in Restful.__KWARGS_EXCLUDE:
            _has_exclude = _param in kwargs
            if _has_exclude:
                ret.pop(_param)

        return ret

    def set_skip_assert_session_started(self, bool_value, **kwargs):
        '''
        Informa ao mecanismo se ele deve ou nao validar o token
        :param bool_value: True ou False
        :param kwargs: lista de parametros a atualizar
        :return: o mesmo kwargs atualizado
        '''
        kwargs.update({Restful._SKIP_ASSERT_SESSION_STARTED: bool_value})
        return kwargs

    def _do_request_with_retry(self, lambda_function, expected_status_code, none_if_status=None, **kwargs):

        _skip_assert_session_started = kwargs.get(Restful._SKIP_ASSERT_SESSION_STARTED) is not True
        if _skip_assert_session_started:
            self._assert_session_started()

        msg = ''

        _retry_count = self.retry_vo.get_max_retry()
        while _retry_count > 0:
            r = None
            try:
                r = lambda_function(self._update_kwargs_4_request(**kwargs))

            except Exception as e:
                msg = 'Last Exception [%s]' % e
                logging.info('\tRetry basico chamado! Erro [%s]' % e)
                pass  # ignore

            finally:
                if r is not None:
                    is_check_for_none = none_if_status is not None

                    if is_check_for_none:
                        is_integer = isinstance(none_if_status, int)
                        none_if_status = (none_if_status,) if is_integer else none_if_status  # transforma em tupla

                        is_ret_none = (r.status_code in none_if_status)
                        if is_ret_none:
                            return None

                    _is_iterable = isinstance(expected_status_code, (list, tuple))
                    expected_status_code = expected_status_code if _is_iterable else [expected_status_code]

                    is_status_code_incorrect = r.status_code not in expected_status_code
                    if is_status_code_incorrect:
                        msg = "Invalid Status code. Response: [%s] %s\n%s" % (r.status_code, r.text[0:100], msg)
                        logging.error(msg)
                        logging.debug('\t retry count [%s]' % _retry_count)

                    else:
                        self.check_for_invalid_token(r)
                        break

                _retry_count -= 1

        if _retry_count < 0:
            raise Exception(msg)

        return r

    def _add_request(self, http_method):
        _is_token_ready = bool(len(self._request_log) > 0 and self.token)
        if _is_token_ready:
            self._request_log[self.token][http_method] = self._request_log[self.token][http_method] + 1
            self._request_log[self.token]['calls'] = self._request_log[self.token]['calls'] + 1

    def _assert_session_started(self):
        if self.token is None:
            raise Warning("Session is not started yet, do 'start_session' first!")

    @abstractmethod
    def start_session(self, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def end_session(self):
        logging.info("Resumo execucao AlliN %s\n%s" % (self.__class__.__name__, self._request_log))

    @abstractmethod
    def check_for_invalid_token(self, r):
        raise NotImplementedError
