import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from src.load import Load
from src.transform import Transform


class TransformTest(unittest.TestCase):
    def test_transforma_resposta_horaria(self) -> None:
        bruto = {
            "hourly": {
                "time": ["2024-01-01T00:00", "2024-01-01T01:00", "2024-01-01T02:00"],
                "temperature_2m": [27.2, None, 26.8],
                "relative_humidity_2m": [None, 82, 80],
                "rain": [None, 1.5, 0.0],
            }
        }

        resultado = Transform().transform_clima(bruto)

        self.assertEqual(
            list(resultado.columns),
            ["data_hora", "temperatura", "umidade", "chuva"],
        )
        self.assertEqual(len(resultado), len(bruto["hourly"]["time"]))
        self.assertTrue(pd.isna(resultado.loc[0, "chuva"]))
        self.assertTrue(pd.isna(resultado.loc[1, "temperatura"]))
        self.assertTrue(pd.isna(resultado.loc[0, "umidade"]))
        self.assertEqual(resultado["chuva"].iloc[1:].tolist(), [1.5, 0.0])
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(resultado["data_hora"]))


class LoadNeonTest(unittest.TestCase):
    def test_cria_tabela_e_envia_registros(self) -> None:
        loader = Load.__new__(Load)
        loader.neon_url = "postgresql://teste:teste@localhost/teste"
        df = pd.DataFrame(
            {
                "data_hora": pd.to_datetime(["2024-01-01T00:00"]),
                "temperatura": [27.2],
                "umidade": [80.0],
                "chuva": [float("nan")],
            }
        )

        conexao = MagicMock()
        cursor = conexao.__enter__.return_value.cursor.return_value.__enter__.return_value
        with patch("src.load.psycopg.connect", return_value=conexao) as conectar:
            loader.load_neon(df, cidade="recife")

        conectar.assert_called_once_with(loader.neon_url)
        cursor.execute.assert_called_once()
        cursor.executemany.assert_called_once()
        registros = cursor.executemany.call_args.args[1]
        self.assertEqual(len(registros), 1)
        self.assertEqual(registros[0][0], "recife")
        self.assertIsNone(registros[0][-1])


class LoadMongoTest(unittest.TestCase):
    def test_marca_carga_sem_alterar_dado_original(self) -> None:
        loader = Load.__new__(Load)
        loader.client = MagicMock()
        colecao = loader.client["cidade_inteligente"]["clima_recife"]
        bruto = {"hourly": {"time": ["2024-01-01T00:00"]}}

        resultado = loader.load_mongo(bruto, "cidade_inteligente", "clima_recife")

        documento = colecao.insert_one.call_args.args[0]
        self.assertEqual(documento["hourly"], bruto["hourly"])
        self.assertEqual(documento["data_extracao"].utcoffset().total_seconds(), 0)
        self.assertNotIn("data_extracao", bruto)
        self.assertEqual(resultado, colecao.insert_one.return_value.inserted_id)


if __name__ == "__main__":
    unittest.main()
