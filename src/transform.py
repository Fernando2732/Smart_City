import pandas as pd


class Transform:

    def __init__(self):
        pass

    def transform_clima(self, data: dict) -> pd.DataFrame:
        dados_por_hora = data["hourly"]

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
        df["chuva"] = df["chuva"].astype(float)

        print("Dados transformados com sucesso!")
        return df
