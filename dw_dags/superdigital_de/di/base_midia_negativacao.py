import csv
import glob
import numpy as np
import os
import pandas as pd
import pyodbc
import shutil
import sys
import warnings
from Functions.do_query import do_query
from Functions.save_data import save_data
from datetime import date


# FUNCAO SPLIT
def get_list_split(tabela, split_idx, split_size):
    lower_bound = (split_idx * split_size)
    upper_bound = ((split_idx + 1) * split_size)

    return tabela.iloc[lower_bound:upper_bound]


def MoverArquivoVelho():
    sys.path.append('..')
    # MOVER TABELAS PARA PASTA ARQUIVOS
    path = 'O:\\Mesa de Performance\\Operacional\\2 - base de campanha\\Base Midia'

    files = [f for f in glob.glob(path + "**/*.csv", recursive=True)]

    for f in files:
        shutil.move(f, 'O:\\Mesa de Performance\\Operacional\\2 - base de campanha\\Base Midia\\Arquivos')


def GeraBasesMidia(**context):
    data1 = date.today().strftime('%d')

    if data1 not in ['10', '20']:
        return True

    # MOVER TABELAS PARA PASTA ARQUIVOS
    MoverArquivoVelho()

    CONN_STR = (
        r'Driver={SQL Server};'
        r'Server=10.1.10.203;'
        r'Database=Superdigital_work;'
        r'Trusted_Connection=yes;'
    )

    # TABELA GERAL HISTORICO
    _sql = '''

    SELECT CONCAT([Email],',',SUBSTRING([Telefone],3,LEN(Telefone)),',',[PrimeiroNome],',,,') as 'Email,Phone,First Name,Last Name,ZIP,Country'
    FROM 
        [Superdigital_Work].[dbo].[CM_SegmentacaoDados] (NOLOCK)
    WHERE
        DataAtivacaoConta is not null and
        CONVERT(DATE,DataAtivacaoConta) <= CONVERT(DATE,GETDATE()) and
        Email <> '' and
        Email is not null


    '''
    tabela = do_query(_sql, CONN_STR)

    # TABELA PF HISTORICO
    _sql1 = '''

    SELECT CONCAT([Email],',',SUBSTRING([Telefone],3,LEN(Telefone)),',',[PrimeiroNome],',,,') as 'Email,Phone,First Name,Last Name,ZIP,Country'
    FROM 
        [Superdigital_Work].[dbo].[CM_SegmentacaoDados] (NOLOCK)
    WHERE
        ProdutoAgrupadoAtual = 'PF' and
        DataAtivacaoConta is not null and
        CONVERT(DATE,DataAtivacaoConta) <= CONVERT(DATE,GETDATE()) and
        Email <> '' and
        Email is not null


    '''
    tabela1 = do_query(_sql1, CONN_STR)

    # QUANTIDADE DE TABELAS
    # HISTORICO
    tam_tab = round(len(tabela.index) / 150000)

    i = 0
    splits = []
    while i < tam_tab:
        tab = get_list_split(tabela, i, 150000)
        splits.append(tab)
        i = i + 1
    # PF
    tam_tab1 = round(len(tabela1.index) / 150000)

    k = 0
    splits1 = []
    while k < tam_tab1:
        tab1 = get_list_split(tabela1, k, 150000)
        splits1.append(tab1)
        k = k + 1

    # DATA HOJE
    data = date.today().strftime('%d-%m-%Y')

    # EXPORTAR
    # CAMINHO
    diretorio_saida = "O:\\Mesa de Performance\\Operacional\\2 - base de campanha\\Base Midia"

    # HISTORICO
    j = 0
    for j in range(tam_tab):
        splits[j].to_csv(diretorio_saida + '\\Ativados Historico_' + data + '_' + str(j + 1) + '.csv', header=True,
                         sep=';', index=None, index_label=None, encoding='latin-1')

    # PF
    l = 0
    for l in range(tam_tab1):
        splits1[l].to_csv(diretorio_saida + '\\Ativados Historico PF_' + data + '_' + str(l + 1) + '.csv', header=True,
                          sep=';', index=None, index_label=None, encoding='latin-1')
