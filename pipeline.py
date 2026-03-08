from __future__ import annotations
from typing import Tuple

import pandas as pd
import phonenumbers
from phonenumbers import NumberParseException
import pycountry
from sqlalchemy import create_engine

from config import RAW_DIR, CLEAN_DB_PATH, INVALID_EMAIL_PLACEHOLDER

def _country_name_to_region(country: object) -> str | None:
    """Best-effort mapping from country name to ISO 2-letter region code.
    Returns None when a mapping cannot be determined; in that case, parsing
    relies on the phone number including an explicit country code (e.g. +44...).
    """
    if country is None:
        return None
    name = str(country).strip()
    if not name:
        return None
    # Normalise whitespace
    name = " ".join(name.split())
    original = name
    # Handle names with extra description in parentheses, e.g. "Bouvet Island (Bouvetoya)"
    base = name.split("(", 1)[0].strip()
    normalized = base.replace("&", "and")
    # First try to resolve via pycountry using a normalised name
    try:
        match = pycountry.countries.lookup(normalized)
        return match.alpha_2
    except LookupError:
        pass
    # Then try the full original name in case pycountry knows that variant
    try:
        match = pycountry.countries.lookup(name)
        return match.alpha_2
    except LookupError:
        pass
    # Fallback overrides for exotic / legacy names as they appear in our data
    overrides = {
        "Holy See (Vatican City State)": "VA",
        "Lao People's Democratic Republic": "LA",
        "Libyan Arab Jamahiriya": "LY",
        "Svalbard & Jan Mayen Islands": "SJ",
        "United States Minor Outlying Islands": "UM",
        "Netherlands Antilles": "AN",
        "Antarctica (the territory South of 60 deg S)": "AQ",
    }
    if original in overrides:
        return overrides[original]
    if normalized in overrides:
        return overrides[normalized]
    return None

def normalize_phone_number(
    raw_phone: object,
    country: object,
    *,
    default_region: str | None = None,
) -> str | None:
    """Normalize a single phone number to E.164 using Google's phonenumbers.
    - Uses the country column (best-effort) to choose a default region.
    - Falls back to default_region when the country cannot be mapped.
    - Returns None when the number cannot be parsed or is invalid.
    """
    if raw_phone is None:
        return None
    s = str(raw_phone).strip()
    if not s:
        return None
    region = _country_name_to_region(country)
    if region is None:
        region = default_region
    try:
        parsed = phonenumbers.parse(s, region)
    except NumberParseException:
        return None
    if not phonenumbers.is_valid_number(parsed):
        return None
    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)

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

    # Normalise phone numbers based on country information where available.
    # Preserve the original raw phone column and write E.164 values into phone_normalized.
    if "phone" in df.columns:
        if "country" in df.columns:
            df["phone_normalized"] = [
                normalize_phone_number(phone, country)
                for phone, country in zip(df["phone"], df["country"])
            ]
        else:
            df["phone_normalized"] = [
                normalize_phone_number(phone, None) for phone in df["phone"]
            ]

    # Lowercase emails and mark invalid ones with a placeholder
    if "email" in df.columns:
        df["email"] = df["email"].str.lower()
        mask_valid_email = df["email"].str.contains("@", na=False)
        df.loc[~mask_valid_email, "email"] = INVALID_EMAIL_PLACEHOLDER

    # Normalise created_at dates to YYYY-MM-DD
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", dayfirst=True)
        df["created_at"] = df["created_at"].dt.strftime("%Y-%m-%d")

    # Deduplicate, keeping last occurrence based on created_at per customer_id
    if "customer_id" in df.columns:
        if "created_at" in df.columns:
            df = df.sort_values("created_at")
        df = df.drop_duplicates(subset=["customer_id"], keep="last")
    else:
        df = df.drop_duplicates(keep="last")

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
        df = df.drop_duplicates(subset=["product_id"], keep="last")
    else:
        df = df.drop_duplicates(keep="last")

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

    # Keep rows with invalid dates or totals (they remain as NaT/NaN)

    # Enforce referential integrity: keep orders but set invalid IDs to None
    valid_customer_ids = set(customers["customer_id"].astype(str))
    valid_product_ids = set(products["product_id"].astype(str))

    if "customer_id" in df.columns:
        df["customer_id"] = df["customer_id"].astype(str)
        df.loc[~df["customer_id"].isin(valid_customer_ids), "customer_id"] = None

    if "product_id" in df.columns:
        df["product_id"] = df["product_id"].astype(str)
        df.loc[~df["product_id"].isin(valid_product_ids), "product_id"] = None

    # Deduplicate on order_id if present
    if "order_id" in df.columns:
        if "order_date" in df.columns:
            df = df.sort_values("order_date")
        df = df.drop_duplicates(subset=["order_id"], keep="last")
    else:
        df = df.drop_duplicates(keep="last")

    return df


def load_to_sqlite(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    orders: pd.DataFrame,
) -> None:
    engine = create_engine(f"sqlite:///{CLEAN_DB_PATH}")

    # Design the cleaned schema by selecting and ordering columns explicitly
    customers_cols = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "phone_normalized",
        "country",
        "created_at",
    ]
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

