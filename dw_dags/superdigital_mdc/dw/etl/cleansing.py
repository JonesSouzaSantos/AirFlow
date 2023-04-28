import logging
from contextlib import closing

from superdigital_de.di.repositorios import ProceduresCleansing
from superdigital_mdc.dw import database

def executar_proc_cleansing(procedure,parameters,**kwargs):
    with closing(database.db_connect(**kwargs)) as conn:
        bo = BusinessObject(conn)
        return bo.executar_proc_cleansing(procedure,parameters)

class BusinessObject(object):

    def __init__(self, conn):
        self.conn = conn
        self.logger = logging.getLogger('cleansing.BusinessObject')

    def executar_proc_cleansing(self,procedure,parameters):

        self.logger.info('[Cleansing Step] Procedure[%s] \n'
                         'Parametros [%s] \n' % (procedure, str(parameters)))


        ProceduresCleansing(procedure).exec(parameters, conn=self.conn)

        database.db_commit(self.conn)

        return True