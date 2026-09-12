from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import json
from dotenv import load_dotenv
import sqlite3
import pandas as pd

load_dotenv()


class Load:


    def __init__(self):

        """Inicializa as variáveis de ambiente e a conexão com o MongoDB."""

        self.mongo_uri = os.getenv("MONGODB_URI")
        self.client = MongoClient(self.mongo_uri, server_api=ServerApi("1"))

    def close(self) -> None:

        """
        Encerra a conexão ativa com o cluster do MongoDB.
        """

        self.client.close()

    def load_json(self, nome_arquivo: str, data: list[dict]) -> None:

        """
        Salva o dicionário de dados brutos em um arquivo JSON local.
        
        Args:
            nome_arquivo (str): O nome do arquivo a ser criado (sem a extensão).
            data (dict): O dicionário contendo os dados extraídos da API.
        """

        os.makedirs("jsons", exist_ok=True) 
        
        with open(f"jsons/{nome_arquivo}.json", "w", encoding="UTF-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"Dados salvos com sucesso em 'jsons/{nome_arquivo}.json'!")

    def load_mongo(self, data: list[dict], db_name: str, collection_name: str) -> None:
        """
        Insere os dados originais da API em uma coleção específica do MongoDB.
        
        Args:
            data (dict): O dicionário de dados brutos extraído da API.
            db_name (str): O nome do banco de dados no MongoDB.
            collection_name (str): O nome da coleção onde os dados serão salvos.
        """

        collection = self.client[db_name][collection_name]

        if data:
            collection.insert_one(data)

        print(f"Dados inseridos com sucesso na coleção '{collection_name}'!")

    def load_sqlite(    
        self,
        df: pd.DataFrame,
        nome_banco: str = "clima.db",
        nome_tabela: str = "historico_clima",
    ) -> None:

        """
        Salva o DataFrame transformado em uma tabela de um banco SQLite local.
        
        Args:
            df (pd.DataFrame): O DataFrame contendo os dados já limpos e estruturados.
            nome_banco (str): O nome do arquivo do banco de dados SQLite.
            nome_tabela (str): O nome da tabela onde os dados serão inseridos.
        """

        conn = sqlite3.connect(nome_banco)
        df.to_sql(nome_tabela, conn, if_exists="replace", index=False)
        conn.close()

        print(f"Dados salvos com sucesso na tabela '{nome_tabela}' do banco '{nome_banco}'!")