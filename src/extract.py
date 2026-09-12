import requests
from pymongo import MongoClient, response
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv

load_dotenv()

class Extract:

    CIDADES = {
        "sao_paulo": {"latitude": -23.5505, "longitude": -46.6333},
        "rio_de_janeiro": {"latitude": -22.9068, "longitude": -43.1729},
        "recife": {"latitude": -8.0476, "longitude": -34.8770},
        "brasilia": {"latitude": -15.7801, "longitude": -47.9292}
    }

    def __init__(self):
        """Inicializa as variáveis base para consumo da API Open-Meteo."""
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.hourly_variables = "temperature_2m,relative_humidity_2m,rain"
        self.timezone = "America/Sao_Paulo"
        self.mongo_uri = os.getenv("MONGODB_URI")
        self.client = MongoClient(self.mongo_uri, server_api=ServerApi("1"))

    def clima(
        self,
        cidade: str,
        data_inicio: str,
        data_fim: str,
    ) -> dict:
        """
        Extrai dados históricos de clima da API Open-Meteo para uma cidade específica.
        
        Args:
            cidade (str): Nome da cidade presente no dicionário (ex: 'recife').
            data_inicio (str): Data de início da busca (YYYY-MM-DD).
            data_fim (str): Data final da busca (YYYY-MM-DD).
            
        Returns:
            dict: JSON bruto de resposta da API com os dados climáticos.
        """
        cidade_formatada = cidade.lower().replace(" ", "_")
        
        if cidade_formatada not in self.CIDADES:
            raise ValueError(f"Cidade inválida: {cidade}")

        coords = self.CIDADES[cidade_formatada]

        url = (
            f"{self.base_url}?latitude={coords['latitude']}&longitude={coords['longitude']}"
            f"&start_date={data_inicio}&end_date={data_fim}"
            f"&hourly={self.hourly_variables}&timezone={self.timezone}"
        )
        
        response = requests.get(url)
        response.raise_for_status()

        print(f"Dados extraídos com sucesso da API Open-Meteo (cidade: {cidade})!")

        return response.json()

    def extract_collection_from_mongo(self, db_name: str, collection_name: str) -> list[dict]:

        collection = self.client[db_name][collection_name]
        documentos = list(collection.find())

        print(f"Dados lidos com sucesso da coleção '{collection_name}'!")
        return response.json()