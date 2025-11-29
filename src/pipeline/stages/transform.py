from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


def normalize_sales(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Normalizando dados de vendas")
    sales = df.copy()
    sales["date"] = pd.to_datetime(sales["date"], errors="coerce")
    sales["store"] = sales["store"].str.strip()
    sales["sku"] = sales["sku"].str.upper()
    sales["unit_price"] = sales["unit_price"].astype(float)
    sales["units"] = sales["units"].astype(int)
    sales["revenue"] = sales["units"] * sales["unit_price"]
    return sales


def normalize_inventory(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Normalizando dados de inventário")
    inventory = df.copy()
    inventory["store"] = inventory["store"].str.strip()
    inventory["sku"] = inventory["sku"].str.upper()
    inventory["on_hand"] = inventory["on_hand"].astype(int)
    inventory["reorder_point"] = inventory["reorder_point"].astype(int)
    return inventory


def compute_metrics(sales: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    logger.info("Calculando métricas derivadas")
    merged = sales.merge(inventory, on=["sku", "store"], how="left", validate="many_to_one")
    merged["days_since_order"] = (sales["date"].max() - merged["date"]).dt.days

    agg = (
        merged.groupby(["sku", "store", "category"], dropna=False)
        .agg(
            orders=("order_id", "nunique"),
            units_sold=("units", "sum"),
            revenue=("revenue", "sum"),
            avg_price=("unit_price", "mean"),
            stock_on_hand=("on_hand", "last"),
            reorder_point=("reorder_point", "last"),
            max_days_since_order=("days_since_order", "max"),
        )
        .reset_index()
    )

    agg["stock_cover_days"] = agg.apply(_estimate_stock_cover, axis=1)
    agg["out_of_stock_risk"] = agg["stock_on_hand"] < agg["reorder_point"]
    return agg


def _estimate_stock_cover(row: pd.Series) -> float:
    daily_units = row["units_sold"] / max(row["max_days_since_order"], 1)
    if daily_units == 0:
        return float("inf")
    return row["stock_on_hand"] / daily_units


def transform_frames(frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    sales = normalize_sales(frames["sales"])
    inventory = normalize_inventory(frames["inventory"])
    metrics = compute_metrics(sales, inventory)
    return {"sales": sales, "inventory": inventory, "metrics": metrics}
