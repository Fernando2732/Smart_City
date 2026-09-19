from src.extract import Extract
from src.load import Load
from src.transform import Transform


def executar_pipeline() -> None:
    cidade = "recife"
    data_inicio = "2024-01-01"
    data_fim = "2024-01-07"
    db_mongo = "cidade_inteligente"
    collection_mongo = f"clima_{cidade}"

    extractor = Extract()
    loader = Load()

    try:
        print("1/4 - Extraindo dados da API...")
        dados_api = extractor.clima(cidade, data_inicio, data_fim)

        print("2/4 - Armazenando dados brutos no MongoDB Atlas...")
        documento_id = loader.load_mongo(
            dados_api,
            db_name=db_mongo,
            collection_name=collection_mongo,
        )

        print("3/4 - Recuperando do Atlas e transformando...")
        dados_atlas = extractor.extract_document_from_mongo(
            db_name=db_mongo,
            collection_name=collection_mongo,
            document_id=documento_id,
        )
        df = Transform().transform_clima(dados_atlas)

        print("4/4 - Armazenando dados transformados no NeonDB...")
        loader.load_neon(df, cidade=cidade)

        print("Pipeline concluída: API -> MongoDB Atlas -> Transformação -> NeonDB.")
    finally:
        extractor.close()
        loader.close()


if __name__ == "__main__":
    executar_pipeline()
