from typing import Any

import pandas as pd
from bson import ObjectId
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task

from src.extract import Extract
from src.load import Load
from src.transform import Transform

load_dotenv()


@task(retries=3, retry_delay_seconds=10, persist_result=False)
def extract(cidade: str, data_inicio: str, data_fim: str) -> dict[str, Any]:
    logger = get_run_logger()
    extractor = Extract()
    try:
        dados = extractor.clima(cidade, data_inicio, data_fim)
    finally:
        extractor.close()

    quantidade = len(dados.get("hourly", {}).get("time", []))
    logger.info("%s medições horárias extraídas para %s.", quantidade, cidade)
    return dados


@task(retries=0, persist_result=False)
def load_data(
    data: dict[str, Any], db_name: str, collection_name: str
) -> ObjectId:
    logger = get_run_logger()
    loader = Load()
    try:
        documento_id = loader.load_mongo(data, db_name, collection_name)
    finally:
        loader.close()

    logger.info("Documento bruto inserido em %s.%s.", db_name, collection_name)
    return documento_id


@task(retries=0, persist_result=False)
def transform(
    db_name: str, collection_name: str, document_id: ObjectId
) -> pd.DataFrame:
    logger = get_run_logger()
    extractor = Extract()
    try:
        dados = extractor.extract_document_from_mongo(
            db_name=db_name,
            collection_name=collection_name,
            document_id=document_id,
        )
    finally:
        extractor.close()

    df = Transform().transform_clima(dados)
    logger.info("%s linhas transformadas de %s.%s.", len(df), db_name, collection_name)
    return df


@task(retries=3, retry_delay_seconds=10, persist_result=False)
def load_neon(df: pd.DataFrame, cidade: str) -> None:
    logger = get_run_logger()
    loader = Load()
    try:
        loader.load_neon(df, cidade=cidade)
    finally:
        loader.close()

    logger.info("%s linhas carregadas em historico_clima para %s.", len(df), cidade)


@flow(name="ETL Clima Open-Meteo", retries=0, log_prints=True, persist_result=False)
def etl_clima_flow(
    cidade: str = "recife",
    data_inicio: str = "2024-01-01",
    data_fim: str = "2024-01-07",
) -> None:
    logger = get_run_logger()
    db_mongo = "cidade_inteligente"
    collection_mongo = f"clima_{cidade}"
    logger.info("Iniciando ETL de %s, de %s a %s.", cidade, data_inicio, data_fim)

    try:
        logger.info("1/4 - Extraindo dados da API...")
        dados_api = extract(cidade, data_inicio, data_fim)

        logger.info("2/4 - Armazenando dados brutos no MongoDB Atlas...")
        documento_id = load_data(dados_api, db_mongo, collection_mongo)

        logger.info("3/4 - Recuperando do Atlas e transformando...")
        df = transform(db_mongo, collection_mongo, documento_id)

        logger.info("4/4 - Armazenando dados transformados no NeonDB...")
        load_neon(df, cidade)
    except Exception:
        logger.error("ETL interrompida por falha; consulte a task que falhou no Prefect.")
        raise

    logger.info("Pipeline concluída: API -> MongoDB Atlas -> Transformação -> NeonDB.")
