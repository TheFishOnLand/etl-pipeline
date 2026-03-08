from __future__ import annotations

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Raw data directory
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"

# Output SQLite database for cleaned data
CLEAN_DB_PATH = BASE_DIR / "cleaned.db"

# Row counts for synthetic data
NUM_CUSTOMERS = 500
NUM_PRODUCTS = 100
NUM_ORDERS = 1000


# Placeholder string to mark obviously invalid emails during cleaning
INVALID_EMAIL_PLACEHOLDER = "INVALID_EMAIL"

