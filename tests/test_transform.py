import pandas as pd

from pipeline.stages import transform


def test_normalize_sales_adds_revenue():
    df = pd.DataFrame(
        {"order_id": [1], "date": ["2024-01-01"], "store": [" Loja "], "sku": ["sn-1"], "units": [2], "unit_price": [10.0]}
    )
    result = transform.normalize_sales(df)
    assert result.loc[0, "revenue"] == 20.0
    assert result.loc[0, "sku"] == "SN-1"


def test_compute_metrics_flags_risk_and_cover():
    sales = pd.DataFrame(
        {
            "order_id": [1, 2],
            "date": pd.to_datetime(["2024-01-01", "2024-01-03"]),
            "store": ["Loja" , "Loja"],
            "sku": ["SKU-1", "SKU-1"],
            "units": [3, 1],
            "unit_price": [10.0, 12.0],
            "revenue": [30.0, 12.0],
        }
    )
    inventory = pd.DataFrame(
        {"sku": ["SKU-1"], "store": ["Loja"], "on_hand": [5], "reorder_point": [6], "category": ["Cat"]}
    )

    metrics = transform.compute_metrics(sales, inventory)

    assert metrics.loc[0, "units_sold"] == 4
    assert metrics.loc[0, "out_of_stock_risk"] is True
    assert metrics.loc[0, "stock_cover_days"] > 0
