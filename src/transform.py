import pandas as pd


class Transform:

    def __init__(self):
        pass

    def transform_clima(self, data: list[dict]) -> pd.DataFrame:

        dados_por_hora = data[0]["hourly"]

        df = pd.DataFrame(dados_por_hora)

        df.rename(
            columns={
                "time": "data_hora",
                "temperature_2m": "temperatura",
                "relative_humidity_2m": "umidade",
                "rain": "chuva"
            },
            inplace=True
        )

        df["data_hora"] = pd.to_datetime(df["data_hora"])
        df["temperatura"] = df["temperatura"].astype(float)
        df["umidade"] = df["umidade"].astype(float)
        df["chuva"] = df["chuva"].fillna(0.0).astype(float) # Substitui NaN por 0

        print("Dados transformados com sucesso!")
        return df