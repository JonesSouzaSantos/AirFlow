import logging
from datetime import datetime, timedelta

import pika

from superdigital_mdc.dw.utils import Constantes


def batch_send_push(batch_push_builder):
    """
    Conecta a uma fila RabbitMQ e, com base nos objetos retornados em BatchPushBuilder.build(), constroi uma mensagem
    para enfileirar com o intuito de envio de push
    :param batch_push_builder:
    :return: Log na estrutura `{'total': 0, 'sucesso': 0, 'falha': 0, 'erros': ['$conta_corrente_id':'$stack_trace']}`
    """

    from allin.api_v2 import BatchPushBuilder
    assert isinstance(batch_push_builder, BatchPushBuilder)

    _log = {'total': 0, 'sucesso': 0, 'falha': 0, 'erros': []}

    credentials = pika.credentials.PlainCredentials(username=Constantes.DEF__RABBIT_PRD_USER,
                                                    password=Constantes.DEF__RABBIT_PRD_PASS)

    conn_params = pika.ConnectionParameters(host=Constantes.DEF__RABBIT_PRD_HOST,
                                            port=Constantes.DEF__RABBIT_PRD_PORT,
                                            virtual_host=Constantes.DEF__RABBIT_PRD_VIRTUAL_HOST,
                                            credentials=credentials)

    with pika.BlockingConnection(conn_params) as connection:
        logging.info("[batch_send_push] Abrindo Canal com fila RabbitMQ...")
        channel = connection.channel()

        channel.queue_declare(queue=Constantes.DEF__RABBIT_PRD_PUSH_QUEUE, durable=True)

        for push in batch_push_builder.build():
            logging.info("[batch_send_push] PUSH %s/%s" % (_log['total'] + 1, len(batch_push_builder.binds)))

            dh_envio = datetime.now() + timedelta(seconds=1)
            data, hora = str(dh_envio).split()
            push.set_hr_envio(hora[:8]).set_dt_envio(data)

            try:
                full_payload = {"destinationAddress": "rabbitmq://%s:%s/%s/%s" %
                                                      (Constantes.DEF__RABBIT_PRD_HOST,
                                                       Constantes.DEF__RABBIT_PRD_PORT,
                                                       Constantes.DEF__RABBIT_PRD_VIRTUAL_HOST,
                                                       Constantes.DEF__RABBIT_PRD_PUSH_QUEUE),
                                "message": push.build_4_rabbit(),
                                "headers": {},
                                "messageType": [
                                    Constantes.DEF__RABBIT_PRD_PUSH_MESSAGE_TYPE]
                                }

                channel.basic_publish(exchange=Constantes.DEF__RABBIT_PRD_PUSH_EXCHANGE,
                                      routing_key=Constantes.DEF__RABBIT_PRD_PUSH_QUEUE,
                                      body=str(full_payload).replace("'", '"'))

                _log['total'] = _log['total'] + 1
                _log['sucesso'] = _log['sucesso'] + 1

            except Exception as e:
                _log['falha'] = _log['falha'] + 1
                _log['erros'].append({push.nm_push: e})

    logging.info("[batch_send_push] Envios realizados!")

    return _log
