from __future__ import annotations

from typing import Any, Tuple

import pandas as pd
from sqlalchemy import create_engine

from config import RAW_DIR, CLEAN_DB_PATH, INVALID_EMAIL_PLACEHOLDER


def extract() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    customers = pd.read_csv(RAW_DIR / "customers.csv")
    products = pd.read_csv(RAW_DIR / "products.csv")
    orders = pd.read_csv(RAW_DIR / "orders.csv")
    return customers, products, orders


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Strip whitespace
    for col in ["first_name", "last_name", "email", "country"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Lowercase emails
    if "email" in df.columns:
        df["email"] = df["email"].str.lower()

    # Drop rows with missing or obviously invalid email
    if "email" in df.columns:
        mask_valid_email = df["email"].str.contains("@", na=False)
        df = df[mask_valid_email].copy()

    # Normalise created_at dates to YYYY-MM-DD
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", dayfirst=True)
        df["created_at"] = df["created_at"].dt.strftime("%Y-%m-%d")

    # Deduplicate, keeping first occurrence per customer_id
    if "customer_id" in df.columns:
        df = df.drop_duplicates(subset=["customer_id"])
    else:
        df = df.drop_duplicates()

    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in ["name", "category", "currency"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Normalise price with comma or dot as decimal separator
    if "price" in df.columns:
        df["price"] = (
            df["price"]
            .astype(str)
            .str.replace(",", ".", regex=False)
        )
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # in_stock as integer
    if "in_stock" in df.columns:
        df["in_stock"] = pd.to_numeric(df["in_stock"], errors="coerce").fillna(0).astype(int)

    # Deduplicate on product_id if present
    if "product_id" in df.columns:
        df = df.drop_duplicates(subset=["product_id"])
    else:
        df = df.drop_duplicates()

    return df


def clean_orders(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    df = orders.copy()

    # Strip whitespace in status
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()

    # Normalise order_date
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce", dayfirst=True)
        df["order_date"] = df["order_date"].dt.strftime("%Y-%m-%d")

    # Numeric conversions
    for col in ["quantity", "unit_price", "total_amount"]:
        if col in df.columns:
            series = df[col].astype(str)
            # total_amount may have comma as decimal separator
            series = series.str.replace(",", ".", regex=False)
            df[col] = pd.to_numeric(series, errors="coerce")

    # Recalculate total_amount where possible
    if {"quantity", "unit_price", "total_amount"} <= set(df.columns):
        mask = df["quantity"].notna() & df["unit_price"].notna()
        df.loc[mask, "total_amount"] = df.loc[mask, "quantity"] * df.loc[mask, "unit_price"]

    # Drop rows with invalid dates or totals
    if "order_date" in df.columns:
        df = df[df["order_date"].notna()]
    if "total_amount" in df.columns:
        df = df[df["total_amount"].notna()]

    # Enforce referential integrity: keep only orders with valid customer_id and product_id
    valid_customer_ids = set(customers["customer_id"].astype(str))
    valid_product_ids = set(products["product_id"].astype(str))

    if "customer_id" in df.columns:
        df["customer_id"] = df["customer_id"].astype(str)
        df = df[df["customer_id"].isin(valid_customer_ids)]

    if "product_id" in df.columns:
        df["product_id"] = df["product_id"].astype(str)
        df = df[df["product_id"].isin(valid_product_ids)]

    # Deduplicate on order_id if present
    if "order_id" in df.columns:
        df = df.drop_duplicates(subset=["order_id"])
    else:
        df = df.drop_duplicates()

    return df


def load_to_sqlite(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    orders: pd.DataFrame,
) -> None:
    engine = create_engine(f"sqlite:///{CLEAN_DB_PATH}")

    # Design the cleaned schema by selecting and ordering columns explicitly
    customers_cols = ["customer_id", "first_name", "last_name", "email", "phone", "country", "created_at"]
    products_cols = ["product_id", "name", "category", "price", "currency", "in_stock"]
    orders_cols = ["order_id", "customer_id", "product_id", "order_date", "status", "quantity", "unit_price", "total_amount"]

    customers_out = customers[[c for c in customers_cols if c in customers.columns]]
    products_out = products[[c for c in products_cols if c in products.columns]]
    orders_out = orders[[c for c in orders_cols if c in orders.columns]]

    with engine.begin() as conn:
        customers_out.to_sql("customers", conn, if_exists="replace", index=False)
        products_out.to_sql("products", conn, if_exists="replace", index=False)
        orders_out.to_sql("orders", conn, if_exists="replace", index=False)


def main() -> None:
    raw_customers, raw_products, raw_orders = extract()
    print(f"Extracted customers={len(raw_customers)}, products={len(raw_products)}, orders={len(raw_orders)}")

    customers = clean_customers(raw_customers)
    products = clean_products(raw_products)
    orders = clean_orders(raw_orders, customers=customers, products=products)

    print(f"After cleaning: customers={len(customers)}, products={len(products)}, orders={len(orders)}")

    load_to_sqlite(customers, products, orders)
    print(f"Loaded cleaned data into SQLite at {CLEAN_DB_PATH}")


if __name__ == "__main__":
    main()

