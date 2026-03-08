# ETL pipeline with randomised e-commerce data (Python + SQL)

This project generates randomised, intentionally "dirty" e-commerce data and then runs an ETL pipeline to clean and load it into a SQLite database.

## Prerequisites

- Python 3.10+ installed and available on your PATH

## Setup

```bash
cd etl-pipeline
python -m venv .venv
# On Windows PowerShell
.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
```

## Generate raw data

```bash
python generate_data.py
```

This will create CSV files in `data/raw/`:

- `customers.csv`
- `products.csv`
- `orders.csv`

The data intentionally contains:

- Duplicates
- Missing values
- Inconsistent date and number formats
- Some invalid foreign keys

## Run the ETL pipeline

```bash
python pipeline.py
```

This will:

1. Extract data from `data/raw/*.csv` into pandas DataFrames.
2. Transform and clean the data effectively:
   - **Phone Normalisation**: Uses `phonenumbers` and `pycountry` to validate and convert raw phone strings into E.164 formats mapped to their country codes.
   - **Format Normalisation**: Stripes trailing spaces, enforces specific date formats (`YYYY-MM-DD`), and fixes decimal separators on financial data. 
   - **Invalid Email Tagging**: Scans addresses and replaces invalid ones with an `INVALID_EMAIL` placeholder to preserve the rest of the customer row.
   - **Deduplication**: Uniquely sorts duplicate IDs by their modification dates (`created_at` / `order_date`) and keeps the **latest** occurrence per ID. 
   - **Data Preservation**: Rows with invalid dates, failing totals, or broken referential keys (`customer_id`, `product_id`) do *not* get dropped. The invalid fields are securely replaced with `NaN` / `None` so entire orders are preserved without foreign key errors.
3. Load the cleaned data into a SQLite database file `cleaned.db` in the project root.

You can inspect `cleaned.db` using any SQLite viewer or via pandas:

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect("cleaned.db")
df_customers = pd.read_sql("SELECT * FROM customers", conn)
print(df_customers.head())
```

