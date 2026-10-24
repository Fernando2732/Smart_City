import runpy
import unittest
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
from bson import ObjectId
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter
from prefect.settings import (
    PREFECT_LOGGING_LEVEL,
    PREFECT_SERVER_ANALYTICS_ENABLED,
    temporary_settings,
)
from prefect.testing.utilities import prefect_test_harness

import orchestration_prefect as orchestration


class OrchestrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        contexts = ExitStack()
        cls.addClassCleanup(contexts.close)
        contexts.enter_context(
            temporary_settings(
                updates={
                    PREFECT_SERVER_ANALYTICS_ENABLED: False,
                    PREFECT_LOGGING_LEVEL: "CRITICAL",
                }
            )
        )
        contexts.enter_context(prefect_test_harness(server_startup_timeout=60))

    def setUp(self) -> None:
        contexts = self.enterContext(ExitStack())
        self.extractor = contexts.enter_context(
            patch.object(orchestration, "Extract")
        ).return_value
        self.loader = contexts.enter_context(
            patch.object(orchestration, "Load")
        ).return_value
        # Mantém o engine e os retries reais, eliminando apenas a espera no teste.
        for name in ("extract", "load_neon"):
            original = getattr(orchestration, name)
            contexts.enter_context(
                patch.object(
                    orchestration,
                    name,
                    original.with_options(retry_delay_seconds=0),
                )
            )

        self.document_id = ObjectId()
        self.api_data = {
            "hourly": {
                "time": ["2024-01-01T00:00"],
                "temperature_2m": [27.2],
                "relative_humidity_2m": [80],
                "rain": [None],
            }
        }
        # Valor diferente para comprovar que a transformação lê o Atlas.
        self.atlas_data = {
            "hourly": {**self.api_data["hourly"], "temperature_2m": [28.4]}
        }
        self.events = []

        def extract(*args, **kwargs):
            self.events.append("api")
            return self.api_data

        def load_mongo(*args, **kwargs):
            self.events.append("mongo")
            return self.document_id

        def read_mongo(*args, **kwargs):
            self.events.append("leitura_atlas")
            return self.atlas_data

        def load_neon(*args, **kwargs):
            self.events.append("neon")

        self.extractor.clima.side_effect = extract
        self.loader.load_mongo.side_effect = load_mongo
        self.extractor.extract_document_from_mongo.side_effect = read_mongo
        self.loader.load_neon.side_effect = load_neon

    def test_flow_sequencial_transforma_documento_inserido_e_registra_tasks(self):
        state = orchestration.etl_clima_flow(return_state=True)

        self.assertTrue(state.is_completed())
        self.assertEqual(self.events, ["api", "mongo", "leitura_atlas", "neon"])
        self.extractor.clima.assert_called_once_with(
            "recife", "2024-01-01", "2024-01-07"
        )
        self.loader.load_mongo.assert_called_once_with(
            self.api_data, "cidade_inteligente", "clima_recife"
        )
        self.extractor.extract_document_from_mongo.assert_called_once_with(
            db_name="cidade_inteligente",
            collection_name="clima_recife",
            document_id=self.document_id,
        )
        self.extractor.extract_collection_from_mongo.assert_not_called()
        df = self.loader.load_neon.call_args.args[0]
        self.assertEqual(df["temperatura"].tolist(), [28.4])
        self.assertTrue(pd.isna(df["chuva"].iloc[0]))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df["data_hora"]))
        self.assertEqual(self.loader.load_neon.call_args.kwargs, {"cidade": "recife"})
        self.assertEqual(self.extractor.close.call_count, 2)
        self.assertEqual(self.loader.close.call_count, 2)

        with get_client(sync_client=True) as client:
            runs = client.read_task_runs(
                flow_run_filter=FlowRunFilter(
                    id={"any_": [state.state_details.flow_run_id]}
                )
            )
        self.assertEqual(len(runs), 4)
        self.assertTrue(all(run.state.is_completed() for run in runs))

    def test_parametros_do_flow_chegam_as_etapas(self):
        state = orchestration.etl_clima_flow(
            cidade="brasilia",
            data_inicio="2024-02-01",
            data_fim="2024-02-02",
            return_state=True,
        )

        self.assertTrue(state.is_completed())
        self.extractor.clima.assert_called_once_with(
            "brasilia", "2024-02-01", "2024-02-02"
        )
        self.assertEqual(self.loader.load_mongo.call_args.args[2], "clima_brasilia")
        self.assertEqual(self.loader.load_neon.call_args.kwargs, {"cidade": "brasilia"})

    def test_retries_da_extracao_e_neon_nao_repetem_insercao_mongo(self):
        self.extractor.clima.side_effect = [TimeoutError("API indisponível"), self.api_data]
        self.loader.load_neon.side_effect = [ConnectionError("Neon indisponível"), None]

        state = orchestration.etl_clima_flow(return_state=True)

        self.assertTrue(state.is_completed())
        self.assertEqual(self.extractor.clima.call_count, 2)
        self.assertEqual(self.loader.load_neon.call_count, 2)
        self.loader.load_mongo.assert_called_once()
        self.extractor.extract_document_from_mongo.assert_called_once()
        self.assertEqual(self.extractor.close.call_count, 3)
        self.assertEqual(self.loader.close.call_count, 3)

    def test_falha_na_extracao_interrompe_flow_apos_retries(self):
        self.extractor.clima.side_effect = TimeoutError("API indisponível")

        state = orchestration.etl_clima_flow(return_state=True)

        self.assertTrue(state.is_failed())
        self.assertEqual(self.extractor.clima.call_count, 4)
        self.assertEqual(self.extractor.close.call_count, 4)
        self.loader.load_mongo.assert_not_called()
        self.extractor.extract_document_from_mongo.assert_not_called()
        self.loader.load_neon.assert_not_called()

    def test_falha_no_mongo_nao_repete_escrita_nem_executa_dependentes(self):
        self.loader.load_mongo.side_effect = ConnectionError("Atlas indisponível")

        state = orchestration.etl_clima_flow(return_state=True)

        self.assertTrue(state.is_failed())
        with self.assertRaises(ConnectionError):
            state.result()
        self.loader.load_mongo.assert_called_once()
        self.extractor.extract_document_from_mongo.assert_not_called()
        self.loader.load_neon.assert_not_called()
        self.loader.close.assert_called_once()
        self.extractor.close.assert_called_once()

    def test_falha_na_leitura_do_atlas_impede_carga_no_neon(self):
        self.extractor.extract_document_from_mongo.side_effect = LookupError(
            "Documento não encontrado"
        )

        state = orchestration.etl_clima_flow(return_state=True)

        self.assertTrue(state.is_failed())
        self.loader.load_neon.assert_not_called()
        self.assertEqual(self.extractor.close.call_count, 2)

    def test_falha_na_transformacao_impede_carga_no_neon(self):
        self.atlas_data.clear()

        state = orchestration.etl_clima_flow(return_state=True)

        self.assertTrue(state.is_failed())
        self.loader.load_neon.assert_not_called()
        self.assertEqual(self.extractor.close.call_count, 2)

    def test_falha_no_neon_esgota_retries_sem_repetir_etapas_anteriores(self):
        self.loader.load_neon.side_effect = ConnectionError("Neon indisponível")

        state = orchestration.etl_clima_flow(return_state=True)

        self.assertTrue(state.is_failed())
        self.assertEqual(self.loader.load_neon.call_count, 4)
        self.loader.load_mongo.assert_called_once()
        self.extractor.extract_document_from_mongo.assert_called_once()
        self.assertEqual(self.loader.close.call_count, 5)


class EntryPointTest(unittest.TestCase):
    def test_run_etl_chama_flow_apenas_como_script(self):
        with patch.object(orchestration, "etl_clima_flow", new=MagicMock()) as flow:
            runpy.run_module("run_etl", run_name="teste_importacao")
            flow.assert_not_called()
            runpy.run_module("run_etl", run_name="__main__")
            flow.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
