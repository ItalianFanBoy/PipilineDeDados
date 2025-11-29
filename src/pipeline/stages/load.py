from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


def write_sqlite(frames: Dict[str, pd.DataFrame], db_path: Path, replace: bool = True) -> None:
    logger.info("Gravando tabelas no SQLite em %s", db_path)
    conn = sqlite3.connect(db_path)
    try:
        for name, df in frames.items():
            table_name = f"staging_{name}" if name != "metrics" else "curated_metrics"
            df.to_sql(table_name, conn, if_exists="replace" if replace else "append", index=False)
            logger.info("Tabela %s gravada (%s linhas)", table_name, len(df))
    finally:
        conn.close()


def export_metrics_parquet(metrics: pd.DataFrame, export_path: Path) -> None:
    logger.info("Exportando métricas para %s", export_path)
    metrics.to_parquet(export_path, index=False)
