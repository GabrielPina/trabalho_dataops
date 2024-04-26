"""Este script tem a finalidade de aplicar tratamento de dados 
nos datasets manipulados"""
import os
from datetime import datetime
import unicodedata

import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

class Saneamento:

    def __init__(self, data, configs):
        self.data = data
        self.metadado =  pd.read_excel(configs["meta_path"])
        self.len_cols = max(list(self.metadado["id"]))
        self.colunas = list(self.metadado['nome_original'])
        self.colunas_new = list(self.metadado['nome'])
        self.path_work = configs["work_path"]

    def remover_caracteres_especiais(self, texto):
        """Esta função recebe uma sentença e substitui os caracteres especiais e acentuação"""    
        lista_caractetes = "'.@#$%¨&*()_+-+´`[{}~^;:,>.,/?|]"
        try:
            nova_sentenca =  ''.join(c for c in unicodedata.normalize('NFD', texto) 
                                     if unicodedata.category(c) != 'Mn')
            for item in lista_caractetes:
                nova_sentenca = nova_sentenca.replace(item, '')
        except Exception as e:
            print(e)
        return texto

    def select_rename(self):
        """Esta função renomeia as colunas do dataframe manipulado"""
        self.data = self.data.loc[:, self.colunas]
        for i in range(self.len_cols):
            self.data.rename(columns={self.colunas[i]:self.colunas_new[i]}, inplace = True)

    def tipagem(self):
        """Esta função define os tipos das colunas conforme o tipo definido
          no dicionário de dados"""
        for col in self.colunas_new:
            tipo = self.metadado.loc[self.metadado['nome'] == col]['tipo'].item()
            if tipo == "int":
                tipo = self.data[col].astype(int)
            elif tipo == "float":
                self.data[col].replace(",", ".", regex=True, inplace = True)
                self.data[col] = self.data[col].astype(float)
            elif tipo == "date":
                self.data[col] = pd.to_datetime(self.data[col]).dt.strftime('%Y-%m-%d')

    def save_work(self):
        """Esta função persiste os dados no banco de dados mysql"""
        self.data['load_date'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        con = mysql.connector.connect(
            user='root', password='root', host='mysql', port="3306", database='db')

        print("DB connected")

        engine  = create_engine("mysql+mysqlconnector://root:root@mysql/db")
        self.data.to_sql('cadastro', con=engine, if_exists='append', index=False)
        con.close()


def error_handler(exception_error, stage):
    """Esta função grava um arquivo de log contendo o lof de exceção lançada"""
    log = [stage, type(exception_error).__name__, exception_error,datetime.now()]
    logdf = pd.DataFrame(log).T

    if not os.path.exists("logs_file.txt"):
        logdf.columns = ['stage', 'type', 'error', 'datetime']
        logdf.to_csv("logs_file.txt", index=False,sep = ";")
    else:
        logdf.to_csv("logs_file.txt", index=False, mode='a', header=False, sep = ";")
