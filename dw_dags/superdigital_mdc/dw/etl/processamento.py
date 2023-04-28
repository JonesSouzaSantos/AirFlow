import logging
from contextlib import closing

from superdigital_de.di.repositorios import ProceduresProcessamento
from superdigital_mdc.dw import database

def executar_proc_processamento(procedure,parameters,**kwargs):
    with closing(database.db_connect(**kwargs)) as conn:
        bo = BusinessObject(conn)
        return bo.executar_proc_processamento(procedure,parameters)

class BusinessObject(object):

    def __init__(self, conn):
        self.conn = conn
        self.logger = logging.getLogger('processamento.BusinessObject')

    def executar_proc_processamento(self,procedure,parameters):

        self.logger.info('[Processamento Step] Procedure[%s] \n'
                         'Parametros [%s] \n' % (procedure, str(parameters)))


        ProceduresProcessamento(procedure).exec(parameters, conn=self.conn)

        database.db_commit(self.conn)

        return True