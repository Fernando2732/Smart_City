import os
from typing import Any

import requests
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()


class Extract:
    CIDADES = {
        "sao_paulo": {"latitude": -23.5505, "longitude": -46.6333},
        "rio_de_janeiro": {"latitude": -22.9068, "longitude": -43.1729},
        "recife": {"latitude": -8.0476, "longitude": -34.8770},
        "brasilia": {"latitude": -15.7801, "longitude": -47.9292},
    }

    def __init__(self) -> None:
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.hourly_variables = "temperature_2m,relative_humidity_2m,rain"
        self.timezone = "America/Sao_Paulo"

        mongo_uri = os.getenv("MONGODB_URI")
        if not mongo_uri:
            raise RuntimeError("Defina MONGODB_URI no arquivo .env.")

        self.client = MongoClient(mongo_uri, server_api=ServerApi("1"))

    def close(self) -> None:
        self.client.close()

    def clima(self, cidade: str, data_inicio: str, data_fim: str) -> dict[str, Any]:
        cidade_formatada = cidade.lower().replace(" ", "_")
        if cidade_formatada not in self.CIDADES:
            raise ValueError(f"Cidade inválida: {cidade}")

        coords = self.CIDADES[cidade_formatada]
        params = {
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "start_date": data_inicio,
            "end_date": data_fim,
            "hourly": self.hourly_variables,
            "timezone": self.timezone,
        }
        response = requests.get(self.base_url, params=params, timeout=30)
        response.raise_for_status()

        print(f"Dados extraídos da API Open-Meteo (cidade: {cidade}).")
        return response.json()

    def extract_document_from_mongo(
        self,
        db_name: str,
        collection_name: str,
        document_id: ObjectId,
    ) -> dict[str, Any]:
        collection = self.client[db_name][collection_name]
        documento = collection.find_one({"_id": document_id})
        if documento is None:
            raise LookupError(
                f"Documento {document_id} não encontrado em "
                f"'{db_name}.{collection_name}'."
            )

        documento.pop("_id", None)
        print(f"Dados recuperados do Atlas: '{db_name}.{collection_name}'.")
        return documento

    def extract_collection_from_mongo(
        self, db_name: str, collection_name: str
    ) -> list[dict[str, Any]]:
        documentos = list(self.client[db_name][collection_name].find())
        print(f"Dados lidos da coleção '{collection_name}'.")
        return documentos
