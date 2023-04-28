import logging
from contextlib import closing

from superdigital_de.di.repositorios import Campanha, Procedures, ParametroOperacional
from superdigital_mdc.dw import database, utils, airflow_utils


def obter_campanhas_a_segmentar(**kwargs):
    with closing(database.db_connect(**kwargs)) as conn:
        bo = BusinessObject(conn)
        campanhas = bo.obter_campanhas_a_segmentar()

        whitelist = bo.obter_segmenta_campanha_whitelist()
        _is_whitelist_exists = len(whitelist) > 0

        _ret = []
        if _is_whitelist_exists:
            _ret = [_ for _ in campanhas if whitelist.count(str(_[Campanha.ID_DEFINITION])) > 0]

        else:
            _ret = campanhas

        _ids_def = ','.join(str(_[Campanha.ID_DEFINITION]) for _ in campanhas)
        logging.info('EMAIL\n\tLista de execucoes [%s] '
                     '\n\tWhitelist [%s] '
                     '\n\tLista Final [%s]' % (_ids_def, whitelist, _ret))

        return _ret


@utils.time_this
def segmentar_campanha(campanha, **kwargs):
    with closing(database.db_connect(**kwargs)) as conn:
        bo = BusinessObject(conn)
        return bo.segmentar_campanha(campanha)


@utils.time_this
def resumo_segmentar_campanha(operator_names, **context):
    all_logs = airflow_utils.get_operators_return_value(operator_names, **context)
    log_exec = [__ if isinstance(__, dict) else _ for _ in all_logs for __ in _]

    print('\n\n# LOG COMPLETO DE EXECUCAO **************\n%s\n**************\n' % log_exec)

    return True


class BusinessObject(object):
    def __init__(self, conn):
        self.conn = conn

    def obter_campanhas_a_segmentar(self):
        campanhas = Campanha.find_by_Ativo(True, conn=self.conn)
        return campanhas

    def obter_segmenta_campanha_whitelist(self):
        _ret = ParametroOperacional.find_Valor_by_id_as_list(ParametroOperacional.P__DAG_SEGMENTA_CAMPANHA_WHITELIST,
                                                             conn=self.conn)
        return _ret

    def segmentar_campanha(self, campanha):
        id_definition = campanha[Campanha.ID_DEFINITION]
        return Procedures.SegmentacaoCampanhas.exec(id_definition=id_definition, id_execution=0, conn=self.conn)
