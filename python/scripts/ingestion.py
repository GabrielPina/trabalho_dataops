"Script de ingestão e preparação - projeto dataops MBADE04"

import os
# Como boas práticas, utilizamos o pacote de log, ao invés de utilizarmos o PRINT para debugar, usamos o log
import logging
from datetime import datetime
# Deixa aleatorio o nome dos arquivos que são criados na RAW
import uuid
import requests
import pandas as pd
# Serve para chamar por exemplo a URL que está dentro do .env
from dotenv import load_dotenv
# importar o util para utilizar a classe de Saneamento (tipagem e salvar arquivo)
from config import configs
import utils

config_file = configs
load_dotenv()
logging.basicConfig(level=logging.INFO)

def ingestion():
    """
    Função de ingestão dos dados
    Outputs: Salva base raw em local específico e retorna o nome do arquivo
    """

    logging.info("Iniciando a ingestão lemos")
    api_url = os.getenv('URL') # Ao invés de deixar explicito a URL, pegamos ela do .env (URL está "escondida" e lemos de uma variavel de ambiente)
    logging.info(f"URL carregada: {api_url}")

    try:
        response = requests.get(api_url, timeout=10).json()
        data = response['results']
    except Exception as exception_error:
        utils.error_handler(exception_error, 'read_api')

    df = pd.json_normalize(data)
    df['load_date'] = datetime.now().strftime("%H:%M:%S")
    file = f"{config_file['raw_path']}{str(uuid.uuid4())}.csv"

    try:
        df.to_csv(file, sep=";", index=False)
    except OSError:
        logging.info( r"Erro ao salvar o arquivo de retorno da api. Diretório de destino não existe.\n.\data\raw")
        os.makedirs(r'./data/raw')
        logging.info(r"Diretório criado './data/raw'.")
        df.to_csv(file, sep=";", index=False)
        logging.info(rf"Arquivo salvo:\n {file}")

    return file

def preparation(file):
    """
    Função de preparação dos dados: renomeia, tipagem, normaliza strings
    Arguments: file -> nome do arquivo raw
    Outputs: Salva base limpa em local específico
    """

    logging.info("Iniciando a preparação")
    df = pd.read_csv(file, sep=";")
    san = utils.Saneamento(df, config_file)
    san.select_rename()
    logging.info("Dados renomeados e selecionados")
    san.tipagem()
    logging.info("Dados tipados")
    san.sanear_colunas()
    logging.info('Ajuste de caracteres especiais na coluna rua')
    logging.info("Caracteres epeciais removidos")
    san.save_work()
    logging.info("Dados salvos")

if __name__ == '__main__':
    file_name = ingestion()
    preparation(file_name)
