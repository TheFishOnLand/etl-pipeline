from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

from faker import Faker

from config import BASE_DIR, RAW_DIR, NUM_CUSTOMERS, NUM_PRODUCTS, NUM_ORDERS


fake = Faker()
Faker.seed(42)
random.seed(42)


def ensure_directories() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def generate_customers(num_customers: int) -> List[Dict[str, str]]:
    customers: List[Dict[str, str]] = []
    for customer_id in range(1, num_customers + 1):
        created_at = fake.date_between(start_date="-2y", end_date="today")
        customers.append(
            {
                "customer_id": str(customer_id),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "country": fake.country(),
                "created_at": created_at.strftime("%Y-%m-%d"),
            }
        )

    # Introduce duplicates and dirty data
    for _ in range(int(num_customers * 0.1)):
        # duplicate some existing customers
        c = random.choice(customers).copy()
        # sometimes change case and add spaces
        c["first_name"] = f"  {c['first_name'].upper()}" if random.random() < 0.5 else c["first_name"]
        c["last_name"] = f"{c['last_name'].lower()}  " if random.random() < 0.5 else c["last_name"]
        customers.append(c)

    for c in customers:
        # Sometimes blank email or invalid format
        r = random.random()
        if r < 0.05:
            c["email"] = ""
        elif r < 0.1:
            c["email"] = c["email"].replace("@", "_at_")

        # Sometimes random different date format
        if random.random() < 0.15:
            dt = fake.date_between(start_date="-2y", end_date="today")
            if random.random() < 0.5:
                c["created_at"] = dt.strftime("%d-%m-%Y")
            else:
                c["created_at"] = dt.strftime("%m/%d/%Y")

    return customers


def generate_products(num_products: int) -> List[Dict[str, str]]:
    categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
    products: List[Dict[str, str]] = []
    for product_id in range(1, num_products + 1):
        price = round(random.uniform(5, 500), 2)
        products.append(
            {
                "product_id": str(product_id),
                "name": fake.word().title(),
                "category": random.choice(categories),
                "price": f"{price:.2f}",
                "currency": "USD",
                "in_stock": str(random.randint(0, 500)),
            }
        )

    # Dirty data: duplicates and number formats
    for _ in range(int(num_products * 0.1)):
        p = random.choice(products).copy()
        products.append(p)

    for p in products:
        # Sometimes use comma as decimal separator
        if random.random() < 0.1:
            p["price"] = p["price"].replace(".", ",")

        # Sometimes blank or weird category
        if random.random() < 0.05:
            p["category"] = ""
        elif random.random() < 0.1:
            p["category"] = "Miscellaneous  "

    return products


def generate_orders(num_orders: int, max_customer_id: int, max_product_id: int) -> List[Dict[str, str]]:
    statuses = ["pending", "shipped", "delivered", "cancelled"]
    orders: List[Dict[str, str]] = []
    start_date = datetime.now() - timedelta(days=365)

    for order_id in range(1, num_orders + 1):
        order_date = start_date + timedelta(days=random.randint(0, 365))

        # Sometimes create invalid customer_id / product_id
        if random.random() < 0.1:
            customer_id = max_customer_id + random.randint(1, 50)  # invalid FK
        else:
            customer_id = random.randint(1, max_customer_id)

        product_id = (
            max_product_id + random.randint(1, 20) if random.random() < 0.1 else random.randint(1, max_product_id)
        )
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(5, 500), 2)
        total_amount = quantity * unit_price

        orders.append(
            {
                "order_id": str(order_id),
                "customer_id": str(customer_id),
                "product_id": str(product_id),
                "order_date": order_date.strftime("%Y-%m-%d"),
                "status": random.choice(statuses),
                "quantity": str(quantity),
                "unit_price": f"{unit_price:.2f}",
                "total_amount": f"{total_amount:.2f}",
            }
        )

    # Dirty data, duplicates and date/number formats
    for _ in range(int(num_orders * 0.05)):
        o = random.choice(orders).copy()
        orders.append(o)

    for o in orders:
        if random.random() < 0.1:
            # Alternate date formats
            dt = fake.date_between(start_date="-1y", end_date="today")
            if random.random() < 0.5:
                o["order_date"] = dt.strftime("%d-%m-%Y")
            else:
                o["order_date"] = dt.strftime("%m/%d/%Y")

        # Sometimes use comma decimal separator
        if random.random() < 0.15:
            o["total_amount"] = o["total_amount"].replace(".", ",")

    return orders


def write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    ensure_directories()

    customers = generate_customers(NUM_CUSTOMERS)
    products = generate_products(NUM_PRODUCTS)
    orders = generate_orders(NUM_ORDERS, max_customer_id=NUM_CUSTOMERS, max_product_id=NUM_PRODUCTS)

    write_csv(RAW_DIR / "customers.csv", customers)
    write_csv(RAW_DIR / "products.csv", products)
    write_csv(RAW_DIR / "orders.csv", orders)

    print(f"Generated {len(customers)} customers, {len(products)} products, {len(orders)} orders in {RAW_DIR}")


if __name__ == "__main__":
    main()

