from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


def load_csv_sources(sales_path: Path, inventory_path: Path) -> Dict[str, pd.DataFrame]:
    """Carrega datasets brutos a partir de CSV."""

    logger.info("Lendo arquivos CSV: %s e %s", sales_path, inventory_path)
    sales_df = pd.read_csv(sales_path)
    inventory_df = pd.read_csv(inventory_path)

    logger.info("Registros carregados - vendas: %s, inventário: %s", len(sales_df), len(inventory_df))
    return {"sales": sales_df, "inventory": inventory_df}
