from __future__ import annotations

import argparse
from pathlib import Path

from .config import PipelineConfig, setup_logging
from .orchestrator import PipelineRunner


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Executa a pipeline de dados")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/pipeline.yaml"),
        help="Caminho para o arquivo de configuração YAML.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = PipelineConfig.from_yaml(args.config)
    setup_logging(config.log_level)

    runner = PipelineRunner(config)
    runner.run()


if __name__ == "__main__":
    main()
