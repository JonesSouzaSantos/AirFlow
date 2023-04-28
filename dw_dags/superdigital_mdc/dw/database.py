import logging
import pyodbc

import pandas as pd

from superdigital_mdc.dw.utils import Constantes


# *****************************
# Database Utils
# *****************************
def db_connect(**kwargs):
    _conn = kwargs.get('conn')

    _is_open_new = _conn is None
    if _is_open_new:
        logging.debug('\tAbrindo conexao!')
        database_connection_string = kwargs.get(Constantes.KEY__CONNECTION_STRING)
        if database_connection_string:
            _conn = pyodbc.connect(database_connection_string)
        else:
            raise Warning('Conexao ou String de Conexao necessaria!')

    return _conn


def do_db_query_2_pandas(sql, **kwargs):
    _rs = pd.read_sql(sql, db_connect(**kwargs))
    df = pd.DataFrame(_rs)
    return df


def do_db_query_fetchone(sql, parameters=None, conn=None, **kwargs):
    _conn = conn or db_connect(**kwargs)

    _c = _conn.cursor()
    try:
        return _c.execute(sql, parameters).fetchone() if parameters else _c.execute(sql).fetchone()
    finally:
        close_obj(_c)


def do_db_query_fetchall(sql, parameters=None, conn=None, **kwargs):
    _conn = conn or db_connect(**kwargs)
    _c = _conn.cursor()
    try:
        _q = _c.execute(sql, parameters).fetchall() if parameters else _c.execute(sql).fetchall()

        for _ in _q:
            yield _
    finally:
        close_obj(_c)


def do_db_execute(sql, parameters=None, conn=None, **kwargs):
    _conn = conn or db_connect(**kwargs)
    _c = _conn.cursor()
    try:
        logging.debug('SQL: p[%s]\nc[%s]' % (parameters, sql))

        _cur = _c.execute(sql, parameters) if parameters else _c.execute(sql)

        try:
            return _cur.fetchone()
        except:
            return _cur.rowcount

    except Exception as e:
        raise e

    finally:
        close_obj(_c)


def do_db_callproc(sql, parameters=None, conn=None, **kwargs):
    _conn = conn or db_connect(autocommit=False, **kwargs)
    _c = _conn.cursor()
    try:
        logging.debug('SQL: p[%s]\nc[%s]' % (parameters, sql))
        _ret = _c.execute(sql, parameters) if parameters else _c.execute(sql)

        return _ret.fetchval()

    except Exception as e:
        raise e

    finally:
        close_obj(_c)


def db_commit(conn=None, **kwargs):
    _conn = conn or db_connect(**kwargs)
    _conn.commit()


def db_rollback(conn=None, **kwargs):
    _conn = conn or db_connect(**kwargs)
    _conn.rollback()


def close_obj(obj):
    if obj:
        try:
            obj.close()
        except Exception as e:
            logging.error('Erro no close do obj[%s] causa[%s]' % (obj, e))


def db_format_val(vlr):
    if not vlr:
        return 'null'
    elif 'None' == vlr:
        return 'null'

    return "'%s'" % vlr
