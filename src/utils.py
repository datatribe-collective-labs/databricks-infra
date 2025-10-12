"""
Utility functions for the Databricks infrastructure project.

This module provides helper functions for data generation, file management,
and other common tasks used throughout the project.
"""

import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


def generate_sample_data(output_dir: str = "course/datasets") -> None:
    """
    Generate comprehensive sample datasets for the Databricks course.
    
    Args:
        output_dir: Directory to save the generated datasets
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Generate customers CSV
    customers_data = _generate_customers_data()
    customers_df = pd.DataFrame(customers_data)
    customers_df.to_csv(output_path / "customers.csv", index=False)
    
    # Generate products CSV  
    products_data = _generate_products_data()
    products_df = pd.DataFrame(products_data)
    products_df.to_csv(output_path / "products.csv", index=False)
    
    # Generate items JSON
    items_data = _generate_items_data()
    with open(output_path / "items.json", "w") as f:
        json.dump(items_data, f, indent=2)
    
    # Generate orders JSON
    orders_data = _generate_orders_data()
    with open(output_path / "orders.json", "w") as f:
        json.dump(orders_data, f, indent=2)
    
    print(f"✅ Sample datasets generated in {output_path}")


def _generate_customers_data(num_customers: int = 15) -> List[Dict[str, Any]]:
    """Generate sample customer data."""
    regions = ["North America", "Europe", "Asia Pacific", "South America", "Africa", "Australia"]
    tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    
    customers = []
    for i in range(1, num_customers + 1):
        customer_id = 1000 + i
        customers.append({
            "customer_id": customer_id,
            "customer_name": f"Customer {i}",
            "email": f"customer{customer_id}@email.com",
            "region": random.choice(regions),
            "signup_date": (datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d'),
            "total_orders": random.randint(0, 50),
            "lifetime_value": round(random.uniform(100, 5000), 2),
            "customer_tier": random.choice(tiers),
            "is_active": random.choice([True, False])
        })
    
    return customers


def _generate_products_data(num_products: int = 15) -> List[Dict[str, Any]]:
    """Generate sample product data."""
    categories = ["Electronics", "Home & Kitchen", "Sports & Outdoors", "Books", "Clothing", "Health & Beauty"]
    brands = ["TechCorp", "HomeMax", "SportsPro", "ReadWell", "FashionStyle", "HealthFirst"]
    
    products = []
    for i in range(1, num_products + 1):
        product_id = f"PROD-{1000 + i}"
        price = round(random.uniform(10, 200), 2)
        cost = round(price * random.uniform(0.3, 0.7), 2)
        
        products.append({
            "product_id": product_id,
            "product_name": f"Product {i}",
            "category": random.choice(categories),
            "brand": random.choice(brands),
            "price": price,
            "cost": cost,
            "inventory_count": random.randint(0, 300),
            "created_date": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
            "is_active": random.choice([True, True, True, False]),  # 75% active
            "rating": round(random.uniform(1.0, 5.0), 1),
            "review_count": random.randint(0, 500),
            "weight_kg": round(random.uniform(0.1, 10.0), 2),
            "dimensions_cm": f"{random.randint(10,50)}x{random.randint(10,50)}x{random.randint(5,30)}"
        })
    
    return products


def _generate_items_data(num_items: int = 10) -> List[Dict[str, Any]]:
    """Generate sample item data with nested structures."""
    categories = ["Electronics", "Home & Kitchen", "Sports & Outdoors", "Books", "Clothing", "Health & Beauty"]
    brands = ["TechCorp", "HomeMax", "SportsPro", "ReadWell", "FashionStyle", "HealthFirst"]
    colors = ["Black", "White", "Red", "Blue", "Green", "Silver", None]
    warehouses = ["WH-EAST-01", "WH-WEST-01", "WH-CENTRAL-01"]
    
    items = []
    for i in range(1, num_items + 1):
        item_id = f"ITM-{str(i).zfill(3)}"
        product_id = f"PROD-{1000 + i}"
        base_price = round(random.uniform(15, 200), 2)
        cost = round(base_price * random.uniform(0.3, 0.7), 2)
        sale_price = round(base_price * random.uniform(0.8, 0.95), 2) if random.random() > 0.6 else None
        
        items.append({
            "item_id": item_id,
            "product_id": product_id,
            "sku": f"{product_id.replace('-', '-')}-{str(i).zfill(3)}",
            "variant": {
                "color": random.choice(colors),
                "size": random.choice(["S", "M", "L", "XL", "Standard", "Large"]),
                "weight": f"{random.randint(100, 2000)}g"
            },
            "pricing": {
                "base_price": base_price,
                "sale_price": sale_price,
                "cost": cost,
                "currency": "USD"
            },
            "inventory": {
                "warehouse_id": random.choice(warehouses),
                "location": f"{random.choice(['A','B','C','D','E'])}-{random.randint(1,25):02d}-{random.choice(['A','B','C'])}",
                "quantity_available": random.randint(0, 300),
                "quantity_reserved": random.randint(0, 10),
                "reorder_level": random.randint(10, 50)
            },
            "attributes": {
                "brand": random.choice(brands),
                "category": random.choice(categories),
                "subcategory": random.choice(["Audio", "Appliances", "Footwear", "Technology", "Shirts", "Supplements"]),
                "material": random.choice(["Cotton", "Plastic", "Metal", "Leather", "Fabric", None]),
                "wireless": random.choice([True, False, None]),
                "bluetooth_version": random.choice(["5.0", "5.1", "4.2", None])
            },
            "created_at": (datetime.now() - timedelta(days=random.randint(1, 365))).isoformat() + "Z",
            "updated_at": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat() + "Z",
            "is_active": random.choice([True, True, True, False])
        })
    
    return items


def _generate_orders_data(num_orders: int = 5) -> List[Dict[str, Any]]:
    """Generate sample order data with nested structures."""
    statuses = ["completed", "shipped", "processing", "cancelled"]
    payment_methods = ["credit_card", "paypal", "debit_card"]
    shipping_methods = ["standard", "express", "overnight"]
    
    orders = []
    for i in range(1, num_orders + 1):
        order_id = f"ORD-2023-{str(i).zfill(3)}"
        customer_id = 1000 + random.randint(1, 15)
        
        # Generate 1-3 items per order
        num_items = random.randint(1, 3)
        items = []
        total_amount = 0
        
        for j in range(num_items):
            item_price = round(random.uniform(10, 200), 2)
            quantity = random.randint(1, 3)
            discount = random.choice([0.0, 0.05, 0.10, 0.15, 0.20])
            line_total = quantity * item_price * (1 - discount)
            total_amount += line_total
            
            items.append({
                "product_id": f"PROD-{1000 + random.randint(1, 15)}",
                "product_name": f"Product {random.randint(1, 15)}",
                "category": random.choice(["Electronics", "Home & Kitchen", "Sports & Outdoors"]),
                "quantity": quantity,
                "unit_price": item_price,
                "discount": discount
            })
        
        shipping_cost = round(random.uniform(5, 25), 2)
        total_amount += shipping_cost
        
        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat() + "Z",
            "order_status": random.choice(statuses),
            "items": items,
            "shipping": {
                "address": {
                    "street": f"{random.randint(100, 999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm'])} St",
                    "city": random.choice(["New York", "London", "Sydney", "Toronto", "Berlin"]),
                    "state": random.choice(["NY", "England", "NSW", "ON", "Berlin"]),
                    "zip": f"{random.randint(10000, 99999)}",
                    "country": random.choice(["USA", "UK", "Australia", "Canada", "Germany"])
                },
                "method": random.choice(shipping_methods),
                "cost": shipping_cost
            },
            "payment": {
                "method": random.choice(payment_methods),
                "last_four": f"{random.randint(1000, 9999)}",
                "amount": round(total_amount, 2)
            },
            "metadata": {
                "source": random.choice(["web", "mobile_app"]),
                "campaign": random.choice(["summer_sale_2023", "new_user_discount", "loyalty_program"]),
                "referrer": random.choice(["google_ads", "facebook", "direct", "email"])
            }
        })
    
    return orders


def validate_file_size(file_path: str, max_size_mb: float = 10.0) -> bool:
    """
    Validate that a file doesn't exceed the maximum size limit.
    
    Args:
        file_path: Path to the file to validate
        max_size_mb: Maximum allowed file size in MB
        
    Returns:
        True if file is within size limit, False otherwise
    """
    if not os.path.exists(file_path):
        print(f"Warning: File {file_path} does not exist")
        return False
    
    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    if size_mb > max_size_mb:
        print(f"Error: {file_path} is {size_mb:.2f}MB, exceeds {max_size_mb}MB limit")
        return False
    
    return True


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.parent


def setup_directories() -> None:
    """Create necessary project directories."""
    project_root = get_project_root()
    
    directories = [
        "course/datasets",
        "course/notebooks/01_week",
        "course/notebooks/02_week", 
        "course/notebooks/03_week",
        "course/notebooks/04_week",
        "course/notebooks/05_week",
        "tests",
        "docs"
    ]
    
    for directory in directories:
        (project_root / directory).mkdir(parents=True, exist_ok=True)
    
    print("✅ Project directories created")


if __name__ == "__main__":
    # Generate sample data when run directly
    setup_directories()
    generate_sample_data()