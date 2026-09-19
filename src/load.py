import json
import os
from typing import Any

import pandas as pd
import psycopg
from bson import ObjectId
from dotenv import load_dotenv
from psycopg import sql
from pymongo import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()


class Load:
    def __init__(self) -> None:
        mongo_uri = os.getenv("MONGODB_URI")
        if not mongo_uri:
            raise RuntimeError("Defina MONGODB_URI no arquivo .env.")

        self.neon_url = os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL")
        self.client = MongoClient(mongo_uri, server_api=ServerApi("1"))

    def close(self) -> None:
        self.client.close()

    def load_json(self, nome_arquivo: str, data: dict[str, Any]) -> None:
        os.makedirs("jsons", exist_ok=True)
        caminho = f"jsons/{nome_arquivo}.json"
        with open(caminho, "w", encoding="utf-8") as arquivo:
            json.dump(data, arquivo, ensure_ascii=False, indent=2)
        print(f"Dados salvos em '{caminho}'.")

    def load_mongo(
        self, data: dict[str, Any], db_name: str, collection_name: str
    ) -> ObjectId:
        if not data:
            raise ValueError("Não há dados para inserir no MongoDB Atlas.")

        resultado = self.client[db_name][collection_name].insert_one(data.copy())
        print(f"Dados inseridos no Atlas: '{db_name}.{collection_name}'.")
        return resultado.inserted_id

    def load_neon(
        self,
        df: pd.DataFrame,
        cidade: str,
        nome_tabela: str = "historico_clima",
    ) -> None:
        if not self.neon_url:
            raise RuntimeError(
                "Defina NEON_DATABASE_URL (ou DATABASE_URL) no arquivo .env."
            )
        if df.empty:
            raise ValueError("Não há dados transformados para inserir no NeonDB.")

        colunas = ["data_hora", "temperatura", "umidade", "chuva"]
        ausentes = set(colunas) - set(df.columns)
        if ausentes:
            raise ValueError(f"Colunas ausentes para carga no Neon: {sorted(ausentes)}")

        def valor_python(valor: Any) -> Any:
            if pd.isna(valor):
                return None
            if isinstance(valor, pd.Timestamp):
                return valor.to_pydatetime()
            return valor.item() if hasattr(valor, "item") else valor

        registros = [
            (cidade, *(valor_python(valor) for valor in linha))
            for linha in df[colunas].itertuples(index=False, name=None)
        ]

        tabela = sql.Identifier(nome_tabela)
        criar_tabela = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                cidade VARCHAR(100) NOT NULL,
                data_hora TIMESTAMP NOT NULL,
                temperatura DOUBLE PRECISION,
                umidade DOUBLE PRECISION,
                chuva DOUBLE PRECISION,
                UNIQUE (cidade, data_hora)
            )
            """
        ).format(tabela)
        inserir = sql.SQL(
            """
            INSERT INTO {} (cidade, data_hora, temperatura, umidade, chuva)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (cidade, data_hora) DO UPDATE SET
                temperatura = EXCLUDED.temperatura,
                umidade = EXCLUDED.umidade,
                chuva = EXCLUDED.chuva
            """
        ).format(tabela)

        with psycopg.connect(self.neon_url) as conexao:
            with conexao.cursor() as cursor:
                cursor.execute(criar_tabela)
                cursor.executemany(inserir, registros)

        print(
            f"{len(registros)} registros carregados no NeonDB "
            f"(tabela '{nome_tabela}')."
        )
