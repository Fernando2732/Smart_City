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

        self.mongo_uri = os.getenv("MONGODB_URI")
        self.client = MongoClient(self.mongo_uri, server_api=ServerApi("1"))

    def close(self) -> None:

        self.client.close()

    def load_json(self, nome_arquivo: str, data: list[dict]) -> None:

        os.makedirs("jsons", exist_ok=True) 
        
        with open(f"jsons/{nome_arquivo}.json", "w", encoding="UTF-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"Dados salvos com sucesso em 'jsons/{nome_arquivo}.json'!")

    def load_mongo(self, data: list[dict], db_name: str, collection_name: str) -> None:

        collection = self.client[db_name][collection_name]

        if data:
            collection.insert_many(data)

        print(f"Dados inseridos com sucesso na coleção '{collection_name}'!")
        self.close()

    def load_sqlite(
        self,
        df: pd.DataFrame,
        nome_banco: str = "clima.db",
        nome_tabela: str = "historico_clima",
    ) -> None:

        conn = sqlite3.connect(nome_banco)
        df.to_sql(nome_tabela, conn, if_exists="replace", index=False)
        conn.close()

        print(f"Dados salvos com sucesso na tabela '{nome_tabela}' do banco '{nome_banco}'!")