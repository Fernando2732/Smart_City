from src.extract import Extract
from src.transform import Transform
from src.load import Load


def executar_pipeline() -> None:
    """Executa as etapas de extração, transformação e carga dos dados climáticos."""
    cidade = "recife"
    data_inicio = "2024-01-01"
    data_fim = "2024-01-07"

    print("Iniciando Extração...")
    extractor = Extract()
    dados_brutos = extractor.clima(cidade, data_inicio, data_fim)

    print("Iniciando Transformação...")
    transformer = Transform()
    df = transformer.transform_clima(dados_brutos)

    print("Iniciando Carga...")
    loader = Load()
    loader.load_json(f"clima_{cidade}", dados_brutos)
    loader.load_mongo(
        dados_brutos,
        db_name="cidade_inteligente",
        collection_name=f"clima_{cidade}",
    )
    loader.load_sqlite(df, nome_banco="clima.db", nome_tabela="historico_clima")
    loader.close()
    
    print("Pipeline executada com sucesso!")


if __name__ == "__main__":
    executar_pipeline()