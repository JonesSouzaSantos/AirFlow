import logging
from datetime import datetime, timedelta

from allin.core import Restful, JsonBuilder


# ******************
# BUILDERS
# ******************


class SMSBuilder(JsonBuilder):
    def __init__(self):
        super(SMSBuilder, self).__init__(exclude_fields=None)
        self.nm_envio = None
        self.nm_celular = None
        self.nm_mensagem = None
        self.dt_envio = None
        self.hr_envio = None
        self.datetime_envio = None

    def set_nm_envio(self, new_value):
        self.nm_envio = new_value
        return self

    def set_nm_celular(self, new_value):
        self.nm_celular = new_value
        return self

    def set_nm_mensagem(self, new_value):
        self.nm_mensagem = new_value
        return self

    def set_dt_envio(self, new_value):
        self.dt_envio = new_value
        return self

    def set_hr_envio(self, new_value):
        self.hr_envio = new_value
        return self

    def set_datetime_envio(self, new_value):
        self.datetime_envio = new_value
        _dt, _hr = str(self.datetime_envio).split()
        self.dt_envio, self.hr_envio = _dt, _hr[:8]
        return self

    def build(self):
        return self.as_dict()


class BatchSMSBuilder(object):
    BIND_CELULAR = 'nm_celular'
    BIND_MENSAGEM = 'nm_mensagem'

    def __init__(self):
        self.sms = SMSBuilder()
        self.binds = []
        self.def_nm_mensagem = None
        self.def_nm_envio = None

    def set_def_nm_mensagem(self, new_value):
        self.def_nm_mensagem = new_value
        return self

    def set_def_nm_envio(self, new_value):
        self.def_nm_envio = new_value
        return self

    def add_binds(self, binds):
        if not binds:
            raise ValueError

        if isinstance(binds, list):
            if len(binds) > 0:
                for _ in binds:
                    if isinstance(_, dict):
                        self.binds.append(_)
                    else:
                        raise TypeError

        elif isinstance(binds, dict):
            self.binds.append(binds)

        else:
            raise TypeError

        return self

    def build(self):
        _idx = 0
        for _ in self.binds:
            _.update({'_index': _idx})
            _nm_mensagem = str(self.def_nm_mensagem)
            _nm_mensagem = _nm_mensagem.format_map(_)

            _nm_envio = str(self.def_nm_envio)
            _nm_envio = _nm_envio.format_map(_)

            self.sms \
                .set_nm_celular(_[self.BIND_CELULAR]) \
                .set_nm_mensagem(_nm_mensagem) \
                .set_nm_envio(_nm_envio)

            yield self.sms

            _idx += 1


class PushBuilder(JsonBuilder):
    def __init__(self):
        super(PushBuilder, self).__init__(exclude_fields=None)
        self.nm_envio = None
        self.valor_json = '{}'
        self.campos = ''
        self.valor = ''
        self.dt_envio = None
        self.hr_envio = None
        self.nm_titulo = None
        self.nm_mensagem = None
        self.nm_push = None
        self.url_scheme = None
        self.html = None
        self.html_id = None
        self.nm_plataforma = None
        self.id_execution = None
        self.id_definition = None

    def set_nm_envio(self, new_value):
        self.nm_envio = new_value
        return self

    def set_valor_json(self, new_value):
        self.valor_json = new_value
        return self

    def set_campos(self, new_value):
        self.campos = new_value
        return self

    def set_valor(self, new_value):
        self.valor = new_value
        return self

    def set_dt_envio(self, new_value):
        self.dt_envio = new_value
        return self

    def set_hr_envio(self, new_value):
        self.hr_envio = new_value
        return self

    def set_nm_titulo(self, new_value):
        self.nm_titulo = new_value
        return self

    def set_nm_mensagem(self, new_value):
        self.nm_mensagem = new_value
        return self

    def set_nm_push(self, new_value):
        self.nm_push = new_value
        return self

    def set_url_scheme(self, new_value):
        self.url_scheme = new_value
        return self

    def set_html(self, new_value):
        self.html = new_value
        return self

    def set_html_id(self, new_value):
        self.html_id = new_value
        return self

    def set_nm_plataforma(self, new_value):
        self.nm_plataforma = new_value
        return self

    def set_id_execution(self, new_value):
        self.id_execution = new_value
        return self

    def set_id_definition(self, new_value):
        self.id_definition = new_value
        return self

    def build(self):
        _ret = self.as_dict()

        key_html = 'html'
        key_html_id = 'html_id'

        if _ret[key_html_id]:
            _ret.pop(key_html)

        elif _ret[key_html]:
            _ret.pop(key_html_id)

        return _ret

    def build_4_rabbit(self):

        _ret = {
            "titulo": self.nm_titulo,
            "mensagem": self.nm_mensagem,
            "contaCorrenteId": self.nm_push,
            "params": {
                "idDefinition": self.id_definition,
                "idExecution": self.id_execution,
            }
        }

        return _ret


class BatchPushBuilder(object):
    """
    Essa classe encapsula destinatarios push para o envio batch de push. Ao utilizar o metodo *build*,
    tem-se em *yield* um dict com o item *BIND_PUSH_ID* populado conforme os dados que foram inserido via *add_binds*
    """
    BIND_PUSH_ID = 'nm_push'
    BIND_TITULO = 'nm_titulo'
    BIND_MENSAGEM = 'nm_mensagem'
    BIND_PLATAFORMA = 'nm_plataforma'
    BIND_HTML_ID = 'html_id'
    BIND_ID_DEFINITION = 'id_definition'
    BIND_ID_EXECUTION = 'id_execution'

    def __init__(self):
        self.push = PushBuilder()
        self.binds = []
        self.def_nm_mensagem = None
        self.def_nm_titulo = None
        self.def_nm_envio = None
        self.def_url_scheme = None
        self.def_html = None
        self.def_html_id = None

    def set_def_nm_mensagem(self, new_value):
        self.def_nm_mensagem = new_value
        return self

    def set_def_nm_titulo(self, new_value):
        self.def_nm_titulo = new_value
        return self

    def set_def_nm_envio(self, new_value):
        self.def_nm_envio = new_value
        return self

    def set_def_url_scheme(self, new_value):
        self.def_url_scheme = new_value
        return self

    def set_def_html(self, new_value):
        self.def_html = new_value
        return self

    def set_def_html_id(self, new_value):
        self.def_html_id = new_value
        return self

    def add_binds(self, binds):
        if not binds:
            raise ValueError

        if isinstance(binds, list):
            if len(binds) > 0:
                for _ in binds:
                    if isinstance(_, dict):
                        self.binds.append(_)
                    else:
                        raise TypeError

        elif isinstance(binds, dict):
            self.binds.append(binds)

        else:
            raise TypeError

        return self

    def build(self):
        for _idx, bind in enumerate(self.binds):
            bind.update({'_index': _idx})
            _nm_mensagem = self.from_default(self.def_nm_mensagem, bind)

            _nm_titulo = self.from_default(self.def_nm_titulo, bind)

            _url_scheme = self.from_default(self.def_url_scheme, bind)

            _html_id = self.def_html_id or None

            _html = self.def_html or None

            _nm_envio = self.def_nm_envio or None

            self.push \
                .set_nm_push(bind[self.BIND_PUSH_ID]) \
                .set_nm_plataforma(bind[self.BIND_PLATAFORMA]) \
                .set_nm_mensagem(_nm_mensagem) \
                .set_nm_titulo(_nm_titulo) \
                .set_nm_envio(_nm_envio) \
                .set_url_scheme(_url_scheme) \
                .set_html_id(_html_id) \
                .set_html(_html) \
                .set_id_definition(bind[self.BIND_ID_DEFINITION]) \
                .set_id_execution(bind[self.BIND_ID_EXECUTION])

            # logging.info('Push a enviar:\n%s' % self.push.build())

            yield self.push

    def from_default(self, def_value, bind):
        return str(def_value).format_map(bind) if def_value else None


# ******************
# API
# ******************

class API_v2(Restful):

    def __init__(self, client_id, client_secret, username, password, **kwargs):
        super().__init__(**kwargs)
        logging.basicConfig(level=logging.DEBUG)

        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.refresh_token = None

        # ENDPOINTS
        self._base_url = 'https://transacional-apiv2.allin.com.br/'
        self.url_token = '%soauth/token' % self._base_url
        self.url_sms = '%sapi/sms' % self._base_url
        self.url_push = '%sapi/pushm' % self._base_url

    def start_session(self, **kwargs):
        self.token = kwargs.get(Restful.KWARGS__API_TOKEN)

        if self.token is None or self.token == '':
            func = lambda _kwargs: self._get_access_token(**_kwargs)

            kwargs = self.set_skip_assert_session_started(True, **kwargs)

            r = self._do_request_with_retry(func, [200, ], **kwargs)
            _json = self._get_as_json(r)

            self.token = _json['access_token']
            self.refresh_token = _json['refresh_token']
            logging.info("Nova Sessao iniciada com token [%s]" % self.token)

        else:
            logging.info("Sessao iniciada REAPROVEITANDO token [%s]" % self.token)

        if self.token not in self._request_log.keys():
            self._request_log.update({self.token: {'calls': 0, 'post': 0, 'get': 0}})

        return self.token

    def _get_access_token(self, **kwargs):
        _url = self.url_token
        logging.info("get_access_token [%s] user[%s]" % (_url, self.username))

        kwargs.update({"data": {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'password',
            'username': self.username,
            'password': self.password,
        }})

        kwargs.update({'headers': {
            'Content-Type': 'application/x-www-form-urlencoded'
        }})

        r = self._do_post(url=_url, verify=False, **kwargs)

        return r

    def check_for_invalid_token(self, r):
        # nao precisa pois caso o token bearer nao for informado, dah erro 500
        pass

    def end_session(self):
        super(API_v2, self).end_session()

    def send_sms(self, builder, **kwargs):

        def do_this(*args):
            func = lambda _kwargs: self._send_sms(builder, *args, **_kwargs)

            r = self._do_request_with_retry(func, 201, **kwargs)
            if r:
                _json = self._get_as_json(r)
                if "stored" not in _json["message"]:
                    raise Exception("Falha ao enviar SMS. Retorno[%s]" % _json)

                else:
                    return _json

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _send_sms(self, sms_builder, *args, **kwargs):
        assert isinstance(sms_builder, SMSBuilder)

        _url = self.url_sms
        logging.debug("send_sms [%s] Envio[%s] Dest[%s]" % (_url, sms_builder.nm_envio, sms_builder.nm_celular))

        kwargs.update({"headers": {
            'content-type': 'application/json',
            'Authorization': 'Bearer %s' % self.token,
        }})

        r = self._do_post(url=_url,
                          verify=False,
                          json=sms_builder.build(),
                          **kwargs)
        return r

    def batch_send_sms(self, builder, **kwargs):
        assert isinstance(builder, BatchSMSBuilder)

        logging.info("batch_send_sms Enviando [%s] sms" % len(builder.binds))

        _log = {'total': 0, 'sucesso': 0, 'falha': 0, 'erros': []}

        _now = datetime.now()
        _tot = len(builder.binds)
        for _sms in builder.build():
            try:
                coefic = _tot / 60
                secs_2_add = coefic if coefic > 100 else coefic + 60
                dh_envio = datetime.now() + timedelta(seconds=int(secs_2_add))

                _sms.set_datetime_envio(dh_envio)

                logging.info("[batch_send_sms] SMS %s/%s (envio previsto: %s)" % (_log['total'] + 1,
                                                                                  _tot,
                                                                                  dh_envio.strftime("%H:%M:%S")))

                _r = self.send_sms(_sms)

                _log['total'] = _log['total'] + 1
                if _r:
                    _log['sucesso'] = _log['sucesso'] + 1

                else:
                    _log['falha'] = _log['falha'] + 1

            except Exception as e:
                _log['falha'] = _log['falha'] + 1
                _log['erros'].append({_sms.nm_celular: e})

        return _log

    def send_push(self, builder, **kwargs):

        def do_this(*args):
            func = lambda _kwargs: self._send_push(builder, *args, **_kwargs)

            r = self._do_request_with_retry(func, 201, **kwargs)
            if r:
                _json = self._get_as_json(r)
                if "stored" not in _json["message"]:
                    raise Exception("Falha ao enviar PUSH. Retorno[%s]" % _json)

                else:
                    return _json

        return self.fallback_strategy.gimme_what_2_do(do_this, self)

    def _send_push(self, push_builder, *args, **kwargs):
        assert isinstance(push_builder, PushBuilder)

        _url = self.url_push
        logging.debug("send_push [%s] Envio[%s] Dest[%s][%s]" % (_url,
                                                                 push_builder.nm_titulo,
                                                                 push_builder.nm_plataforma,
                                                                 push_builder.nm_push))

        kwargs.update({"headers": {
            'content-type': 'application/json',
            'Authorization': 'Bearer %s' % self.token,
        }})

        r = self._do_post(url=_url,
                          verify=False,
                          json=push_builder.build(),
                          **kwargs)
        return r

    def batch_send_push(self, builder, **kwargs):
        assert isinstance(builder, BatchPushBuilder)

        logging.info("[batch_send_push] Enviando [%s] push" % len(builder.binds))

        _log = {'total': 0, 'sucesso': 0, 'falha': 0, 'erros': []}

        _now = datetime.now()
        for _push in builder.build():
            logging.info("[batch_send_push] PUSH %s/%s" % (_log['total'] + 1, len(builder.binds)))

            try:
                dh_envio = datetime.now() + timedelta(seconds=1)
                data, hora = str(dh_envio).split()

                _push.set_hr_envio(hora[:8]).set_dt_envio(data)
                _r = self.send_push(_push)

                _log['total'] = _log['total'] + 1
                if _r:
                    _log['sucesso'] = _log['sucesso'] + 1

                else:
                    _log['falha'] = _log['falha'] + 1

            except Exception as e:
                _log['falha'] = _log['falha'] + 1
                _log['erros'].append({_push.nm_push: e})

        return _log
