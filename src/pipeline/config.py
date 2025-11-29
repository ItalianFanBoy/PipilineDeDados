from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml


@dataclass
class PipelineConfig:
    """Configuração da pipeline carregada a partir de YAML."""

    sales_path: Path
    inventory_path: Path
    output_dir: Path
    database_path: Path
    metrics_export_path: Path
    replace_existing: bool = True
    log_level: str = "INFO"

    @classmethod
    def from_yaml(cls, path: Path) -> "PipelineConfig":
        with path.open("r", encoding="utf-8") as f:
            raw: Dict[str, Any] = yaml.safe_load(f)

        paths = raw.get("paths", {})
        load_cfg = raw.get("load", {})
        logging_cfg = raw.get("logging", {})

        return cls(
            sales_path=Path(paths["sales"]),
            inventory_path=Path(paths["inventory"]),
            output_dir=Path(paths["output_dir"]),
            database_path=Path(paths["database"]),
            metrics_export_path=Path(paths["metrics_export"]),
            replace_existing=bool(load_cfg.get("replace_existing", True)),
            log_level=str(logging_cfg.get("level", "INFO")),
        )

    def ensure_output_dirs(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.metrics_export_path.parent.mkdir(parents=True, exist_ok=True)


def setup_logging(level: str) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
