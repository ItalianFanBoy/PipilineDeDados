from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from . import stages
from .config import PipelineConfig

logger = logging.getLogger(__name__)


@dataclass
class PipelineArtifacts:
    sales: pd.DataFrame
    inventory: pd.DataFrame
    metrics: pd.DataFrame


class PipelineRunner:
    """Orquestra a execução das etapas da pipeline."""

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def run(self) -> PipelineArtifacts:
        logger.info("Iniciando pipeline")
        self.config.ensure_output_dirs()

        raw_frames = stages.ingest.load_csv_sources(
            sales_path=self.config.sales_path,
            inventory_path=self.config.inventory_path,
        )
        processed_frames = stages.transform.transform_frames(raw_frames)

        stages.load.write_sqlite(
            processed_frames,
            db_path=self.config.database_path,
            replace=self.config.replace_existing,
        )
        stages.load.export_metrics_parquet(
            processed_frames["metrics"], self.config.metrics_export_path
        )

        logger.info("Pipeline finalizada com sucesso")
        return PipelineArtifacts(**processed_frames)


__all__ = ["PipelineRunner", "PipelineArtifacts"]
