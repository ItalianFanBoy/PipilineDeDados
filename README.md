# Pipeline de Dados

Esta aplicação implementa uma pipeline analítica para dados de vendas e inventário. O fluxo inclui extração de arquivos CSV brutos, padronização/limpeza, enriquecimento com métricas agregadas e carregamento em um banco SQLite, além de exportação em formato Parquet. O objetivo é fornecer um exemplo completo e reproduzível de orquestração de pipeline local.

## Visão geral das etapas
1. **Ingestão**: leitura de datasets CSV configuráveis.
2. **Transformação**: tratamento de tipos, normalização de colunas e cálculo de métricas (receita, giro de estoque, rupturas).
3. **Carga**: persistência dos dados tratados em tabelas SQLite e exportação de tabelas derivadas para Parquet.
4. **Orquestração**: execução sequencial com logging estruturado e controle de diretórios de trabalho.

## Instalação

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Execução da pipeline

```bash
PYTHONPATH=src python -m pipeline.main --config config/pipeline.yaml
```

Saídas principais (configuráveis):
- `data/output/pipeline.db`: banco SQLite com as tabelas `staging_sales`, `staging_inventory`, `curated_metrics`.
- `data/output/metrics.parquet`: exportação da tabela de métricas para análise rápida.

## Testes

```bash
pytest
```

## Estrutura de diretórios
- `src/pipeline`: código da pipeline e orquestrador.
- `config/pipeline.yaml`: caminhos de entrada e saída.
- `data/raw`: arquivos CSV de exemplo para execução rápida.
- `data/output`: diretório onde os artefatos processados são gravados.
