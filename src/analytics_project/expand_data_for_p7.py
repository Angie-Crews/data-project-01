"""
expand_data_for_p7.py

Generate expanded synthetic data for P7 Customer Segmentation Analysis.
Creates realistic data with temporal patterns, customer behaviors, and business scenarios.

New Features:
- 5,000+ sales transactions (vs. 1,509)
- 23 months of data (Jan 2024 - Nov 2025)
- 400 customers (vs. 179) with age distribution
- Realistic purchase patterns and seasonality
- Customer segments (New, Regular, VIP, At-Risk)
- Profit margins and payment methods
"""

import pathlib
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Ensure project root is in sys.path
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent))
from utils_logger import logger, init_logger

# Constants
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
BACKUP_DIR = DATA_DIR / "backup_p6_data"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
BACKUP_DIR.mkdir(exist_ok=True)

# Configuration
NUM_CUSTOMERS = 400
NUM_PRODUCTS = 120
NUM_TRANSACTIONS = 5000
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 11, 30)

# Reference data
REGIONS = ["West", "East", "Central", "North", "South"]
CATEGORIES = ["Electronics", "Clothing", "Home", "Office"]
CUSTOMER_STATUSES = ["New", "Regular", "VIP", "At-Risk"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Cash", "Apple Pay"]
SUPPLIERS = ["TechSource", "GadgetPro", "ComfortCo", "HomeLine", "OfficeWorks", "GlobalTrade"]
SALES_REPS = [
    "J. Alvarez",
    "M. Chen",
    "S. Patel",
    "K. Nguyen",
    "A. Torres",
    "D. Williams",
    "R. Johnson",
    "L. Martinez",
]

# First names and last names for generating customer names
FIRST_NAMES = [
    "James",
    "Mary",
    "John",
    "Patricia",
    "Robert",
    "Jennifer",
    "Michael",
    "Linda",
    "William",
    "Elizabeth",
    "David",
    "Barbara",
    "Richard",
    "Susan",
    "Joseph",
    "Jessica",
    "Thomas",
    "Sarah",
    "Charles",
    "Karen",
    "Christopher",
    "Nancy",
    "Daniel",
    "Lisa",
    "Matthew",
    "Betty",
    "Anthony",
    "Margaret",
    "Mark",
    "Sandra",
    "Donald",
    "Ashley",
    "Steven",
    "Kimberly",
    "Paul",
    "Emily",
    "Andrew",
    "Donna",
    "Joshua",
    "Michelle",
    "Kenneth",
    "Dorothy",
    "Kevin",
    "Carol",
    "Brian",
    "Amanda",
    "George",
    "Melissa",
    "Edward",
    "Deborah",
    "Ronald",
    "Stephanie",
    "Timothy",
    "Rebecca",
    "Jason",
    "Sharon",
    "Jeffrey",
    "Laura",
    "Ryan",
    "Cynthia",
    "Jacob",
    "Kathleen",
    "Gary",
    "Amy",
    "Nicholas",
    "Shirley",
    "Eric",
    "Angela",
    "Jonathan",
    "Helen",
    "Stephen",
    "Anna",
    "Larry",
    "Brenda",
    "Justin",
    "Pamela",
    "Scott",
    "Nicole",
    "Brandon",
    "Emma",
    "Benjamin",
    "Samantha",
    "Samuel",
    "Katherine",
    "Raymond",
    "Christine",
    "Gregory",
    "Debra",
]

LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Perez",
    "Thompson",
    "White",
    "Harris",
    "Sanchez",
    "Clark",
    "Ramirez",
    "Lewis",
    "Robinson",
    "Walker",
    "Young",
    "Allen",
    "King",
    "Wright",
    "Scott",
    "Torres",
    "Nguyen",
    "Hill",
    "Flores",
    "Green",
    "Adams",
    "Nelson",
    "Baker",
    "Hall",
    "Rivera",
    "Campbell",
    "Mitchell",
    "Carter",
    "Roberts",
    "Gomez",
    "Phillips",
    "Evans",
    "Turner",
    "Diaz",
    "Parker",
    "Cruz",
    "Edwards",
    "Collins",
    "Reyes",
    "Stewart",
    "Morris",
    "Morales",
    "Murphy",
    "Cook",
    "Rogers",
    "Gutierrez",
    "Ortiz",
    "Morgan",
    "Cooper",
    "Peterson",
    "Bailey",
    "Reed",
    "Kelly",
    "Howard",
    "Ramos",
    "Kim",
    "Cox",
    "Ward",
    "Richardson",
    "Watson",
    "Brooks",
    "Chavez",
    "Wood",
    "James",
    "Bennett",
    "Gray",
    "Mendoza",
]


def backup_existing_data():
    """Backup current data files before generating new ones."""
    logger.info("=" * 60)
    logger.info("BACKING UP EXISTING DATA")
    logger.info("=" * 60)

    files_to_backup = ["customers_data.csv", "products_data.csv", "sales_data.csv"]

    backup_count = 0
    for filename in files_to_backup:
        source = RAW_DATA_DIR / filename
        if source.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{filename.replace('.csv', '')}_{timestamp}_p6_backup.csv"
            destination = BACKUP_DIR / backup_name

            # Copy file
            import shutil

            shutil.copy2(source, destination)
            logger.info(f"✅ Backed up: {filename} → {backup_name}")
            backup_count += 1
        else:
            logger.warning(f"⚠️  File not found: {filename} (skipping backup)")

    logger.info(f"Total files backed up: {backup_count}")
    logger.info(f"Backup location: {BACKUP_DIR}")
    logger.info("=" * 60)


def generate_customers_data():
    """Generate expanded customer data with realistic demographics."""
    logger.info("=" * 60)
    logger.info("GENERATING CUSTOMERS DATA")
    logger.info("=" * 60)

    customers = []

    for i in range(NUM_CUSTOMERS):
        customer_id = 1000 + i

        # Generate realistic name
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        customer_name = f"{first_name} {last_name}"

        # Regional distribution with East bias (from P6 insights)
        region = np.random.choice(
            REGIONS,
            p=[0.15, 0.40, 0.15, 0.15, 0.15],  # East gets 40%, others 15%
        )

        # Customer since date (realistic join distribution)
        # More recent customers, some long-term
        days_back = np.random.exponential(scale=365)  # Exponential distribution
        days_back = min(days_back, 1825)  # Max 5 years
        customer_since = END_DATE - timedelta(days=int(days_back))
        customer_since_str = customer_since.strftime("%m/%d/%Y")

        # Customer age (18-75, normal distribution)
        customer_age = int(np.random.normal(42, 15))
        customer_age = max(18, min(75, customer_age))  # Clip to reasonable range

        # Customer status based on tenure and spending
        tenure_days = (END_DATE - customer_since).days
        if tenure_days < 90:
            status = "New"
        elif tenure_days > 730:  # 2+ years
            status = np.random.choice(["VIP", "Regular", "At-Risk"], p=[0.2, 0.6, 0.2])
        else:
            status = np.random.choice(["Regular", "VIP"], p=[0.8, 0.2])

        # TotalSpend placeholder (will be calculated from transactions)
        total_spend = 0.0

        customers.append(
            {
                "CustomerID": customer_id,
                "CustomerName": customer_name,
                "Region": region,
                "CustomerSince": customer_since_str,
                "CustomerAge": customer_age,
                "TotalSpend": total_spend,
                "CustomerStatus": status,
            }
        )

    df = pd.DataFrame(customers)

    logger.info(f"Generated {len(df)} customers")
    logger.info(
        f"Age distribution: min={df['CustomerAge'].min()}, max={df['CustomerAge'].max()}, mean={df['CustomerAge'].mean():.1f}"
    )
    logger.info(f"Regional distribution:\n{df['Region'].value_counts()}")
    logger.info(f"Status distribution:\n{df['CustomerStatus'].value_counts()}")

    return df


def generate_products_data():
    """Generate expanded product data with better category alignment."""
    logger.info("=" * 60)
    logger.info("GENERATING PRODUCTS DATA")
    logger.info("=" * 60)

    products = []

    # Product name templates by category
    product_templates = {
        "Electronics": [
            "Laptop",
            "Smartphone",
            "Tablet",
            "Monitor",
            "Keyboard",
            "Mouse",
            "Headphones",
            "Speaker",
            "Camera",
            "Smartwatch",
            "Router",
            "USB Drive",
            "Webcam",
            "Microphone",
            "Gaming Console",
            "TV",
            "Printer",
            "Scanner",
        ],
        "Clothing": [
            "T-Shirt",
            "Jeans",
            "Dress",
            "Jacket",
            "Sweater",
            "Shorts",
            "Skirt",
            "Blazer",
            "Coat",
            "Hoodie",
            "Polo Shirt",
            "Pants",
            "Suit",
            "Cardigan",
            "Tank Top",
            "Leggings",
            "Socks",
            "Scarf",
        ],
        "Home": [
            "Sofa",
            "Coffee Table",
            "Lamp",
            "Rug",
            "Bed Frame",
            "Mattress",
            "Dining Chair",
            "Bookshelf",
            "Desk",
            "Mirror",
            "Curtains",
            "Pillow",
            "Blanket",
            "Vase",
            "Clock",
            "Picture Frame",
            "Storage Box",
            "Ottoman",
        ],
        "Office": [
            "Office Chair",
            "File Cabinet",
            "Whiteboard",
            "Stapler",
            "Desk Organizer",
            "Calculator",
            "Paper Shredder",
            "Pen Set",
            "Notebook",
            "Folder",
            "Binder",
            "Label Maker",
            "Desk Lamp",
            "Paper Tray",
            "Business Card Holder",
            "Calendar",
            "Clipboard",
            "Tape Dispenser",
        ],
    }

    # Price ranges by category
    price_ranges = {
        "Electronics": (50, 2000),
        "Clothing": (15, 300),
        "Home": (30, 1500),
        "Office": (10, 500),
    }

    products_per_category = NUM_PRODUCTS // len(CATEGORIES)

    product_id = 2000
    for category in CATEGORIES:
        templates = product_templates[category]
        min_price, max_price = price_ranges[category]

        for i in range(products_per_category):
            # Generate product name with variation
            base_name = random.choice(templates)
            variation = random.choice(
                ["Pro", "Plus", "Elite", "Standard", "Deluxe", "Premium", "Basic"]
            )
            product_name = f"{base_name} {variation}"

            # Price based on category range (slightly higher prices for "Pro", "Premium", etc.)
            if variation in ["Pro", "Premium", "Deluxe", "Elite"]:
                price_multiplier = random.uniform(0.7, 1.0)  # Upper 30% of range
            else:
                price_multiplier = random.uniform(0.3, 0.7)  # Middle to lower range

            unit_price = min_price + (max_price - min_price) * price_multiplier
            unit_price = round(unit_price, 2)

            # Stock quantity (some products low stock, some high)
            stock_quantity = int(np.random.gamma(shape=2, scale=50))
            stock_quantity = max(0, min(500, stock_quantity))

            # Product size (for potential future analysis)
            sizes = ["Small", "Medium", "Large", "XL", "N/A"]
            product_size = random.choice(sizes)

            supplier_name = random.choice(SUPPLIERS)

            products.append(
                {
                    "ProductID": product_id,
                    "ProductName": product_name,
                    "ProductCategory": category,
                    "UnitPrice": unit_price,
                    "StockQuantity": stock_quantity,
                    "ProductSize": product_size,
                    "SupplierName": supplier_name,
                }
            )

            product_id += 1

    df = pd.DataFrame(products)

    logger.info(f"Generated {len(df)} products")
    logger.info(f"Price range: ${df['UnitPrice'].min():.2f} - ${df['UnitPrice'].max():.2f}")
    logger.info(f"Category distribution:\n{df['ProductCategory'].value_counts()}")
    logger.info(f"Supplier distribution:\n{df['SupplierName'].value_counts()}")

    return df


def generate_sales_data(customers_df, products_df):
    """Generate expanded sales data with temporal patterns and seasonality."""
    logger.info("=" * 60)
    logger.info("GENERATING SALES DATA")
    logger.info("=" * 60)

    sales = []
    customer_purchase_counts = {cid: 0 for cid in customers_df['CustomerID']}
    customer_total_spend = {cid: 0.0 for cid in customers_df['CustomerID']}

    # Create date distribution with seasonality
    # More sales in Nov-Dec (holidays), lower in Jan-Feb
    date_weights = []
    dates = []
    current_date = START_DATE
    while current_date <= END_DATE:
        dates.append(current_date)

        # Seasonal weight (higher in Nov-Dec)
        month = current_date.month
        if month in [11, 12]:  # Holiday season
            weight = 2.0
        elif month in [1, 2]:  # Post-holiday slump
            weight = 0.6
        elif month in [6, 7]:  # Summer
            weight = 1.3
        else:  # Regular months
            weight = 1.0

        # Weekend boost
        if current_date.weekday() >= 5:  # Saturday, Sunday
            weight *= 1.2

        date_weights.append(weight)
        current_date += timedelta(days=1)

    # Normalize weights
    date_weights = np.array(date_weights)
    date_weights = date_weights / date_weights.sum()

    for transaction_id in range(1, NUM_TRANSACTIONS + 1):
        # Select date based on seasonal weights
        transaction_date = np.random.choice(dates, p=date_weights)
        transaction_date_str = transaction_date.strftime("%m/%d/%Y")

        # Select customer (bias towards repeat customers)
        # 80% of sales from 20% of customers (Pareto principle)
        if random.random() < 0.8:
            # Select from top 20% frequent customers
            sorted_customers = sorted(
                customer_purchase_counts.items(), key=lambda x: x[1], reverse=True
            )
            top_20_percent = max(1, len(sorted_customers) // 5)
            customer_id = random.choice([cid for cid, _ in sorted_customers[:top_20_percent]])
        else:
            # Select from any customer
            customer_id = random.choice(customers_df['CustomerID'].tolist())

        # Get customer info
        customer_info = customers_df[customers_df['CustomerID'] == customer_id].iloc[0]
        customer_region = customer_info['Region']
        customer_age = customer_info['CustomerAge']

        # Select product with category preferences by demographics
        # Younger customers prefer Electronics, older prefer Home
        category_prefs = {
            (18, 30): {"Electronics": 0.5, "Clothing": 0.3, "Home": 0.1, "Office": 0.1},
            (31, 45): {"Electronics": 0.3, "Clothing": 0.3, "Home": 0.25, "Office": 0.15},
            (46, 60): {"Electronics": 0.2, "Clothing": 0.2, "Home": 0.4, "Office": 0.2},
            (61, 75): {"Electronics": 0.15, "Clothing": 0.15, "Home": 0.5, "Office": 0.2},
        }

        # Find age bracket
        for (age_min, age_max), prefs in category_prefs.items():
            if age_min <= customer_age <= age_max:
                category = np.random.choice(list(prefs.keys()), p=list(prefs.values()))
                break
        else:
            category = random.choice(CATEGORIES)

        # Regional category preferences (from P6 insights)
        if customer_region == "East" and category == "Home":
            # Boost Home sales in East
            pass
        elif customer_region == "West" and random.random() < 0.3:
            # West prefers Electronics
            category = "Electronics"

        # Select product from category
        category_products = products_df[products_df['ProductCategory'] == category]
        if len(category_products) == 0:
            category_products = products_df
        product = category_products.sample(n=1).iloc[0]
        product_id = product['ProductID']
        unit_price = product['UnitPrice']

        # Quantity (most transactions are 1-3 items, occasional bulk)
        if random.random() < 0.1:  # 10% bulk orders
            quantity_sold = random.randint(5, 15)
        else:
            quantity_sold = random.randint(1, 4)

        # Total amount with occasional discounts
        base_amount = unit_price * quantity_sold
        if random.random() < 0.15:  # 15% chance of discount
            discount_factor = random.uniform(0.85, 0.95)  # 5-15% off
            total_amount = base_amount * discount_factor
        else:
            total_amount = base_amount
        total_amount = round(total_amount, 2)

        # Store ID (5 stores)
        store_id = random.randint(401, 405)

        # Campaign ID (4 campaigns, 40% no campaign)
        campaign_id = random.choices([0, 1, 2, 3, 4], weights=[0.4, 0.15, 0.15, 0.15, 0.15])[0]

        # Payment method
        payment_method = random.choice(PAYMENT_METHODS)

        # Sales rep
        sales_rep = random.choice(SALES_REPS)

        # Update customer tracking
        customer_purchase_counts[customer_id] += 1
        customer_total_spend[customer_id] += total_amount

        sales.append(
            {
                "TransactionID": transaction_id,
                "TransactionDate": transaction_date_str,
                "CustomerID": customer_id,
                "ProductID": product_id,
                "StoreID": store_id,
                "CampaignID": campaign_id,
                "TotalAmount": total_amount,
                "QuantitySold": quantity_sold,
                "PaymentMethod": payment_method,
                "SalesRepresentative": sales_rep,
            }
        )

    df = pd.DataFrame(sales)

    # Update customer TotalSpend
    for customer_id, total_spend in customer_total_spend.items():
        customers_df.loc[customers_df['CustomerID'] == customer_id, 'TotalSpend'] = round(
            total_spend, 2
        )

    logger.info(f"Generated {len(df)} sales transactions")
    logger.info(f"Date range: {df['TransactionDate'].min()} to {df['TransactionDate'].max()}")
    logger.info(f"Revenue range: ${df['TotalAmount'].min():.2f} - ${df['TotalAmount'].max():.2f}")
    logger.info(f"Total revenue: ${df['TotalAmount'].sum():.2f}")
    logger.info(f"Average transaction: ${df['TotalAmount'].mean():.2f}")
    logger.info(f"Payment method distribution:\n{df['PaymentMethod'].value_counts()}")

    # Show customer purchase frequency
    top_customers = sorted(customer_purchase_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    logger.info(f"Top 5 customers by purchase count: {top_customers}")

    return df


def save_data(customers_df, products_df, sales_df):
    """Save generated data to CSV files."""
    logger.info("=" * 60)
    logger.info("SAVING DATA FILES")
    logger.info("=" * 60)

    # Save customers
    customers_file = RAW_DATA_DIR / "customers_data.csv"
    customers_df.to_csv(customers_file, index=False)
    logger.info(f"✅ Saved: {customers_file} ({len(customers_df)} records)")

    # Save products
    products_file = RAW_DATA_DIR / "products_data.csv"
    products_df.to_csv(products_file, index=False)
    logger.info(f"✅ Saved: {products_file} ({len(products_df)} records)")

    # Save sales
    sales_file = RAW_DATA_DIR / "sales_data.csv"
    sales_df.to_csv(sales_file, index=False)
    logger.info(f"✅ Saved: {sales_file} ({len(sales_df)} records)")

    logger.info("=" * 60)
    logger.info("DATA GENERATION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"📊 Summary:")
    logger.info(f"   • Customers: {len(customers_df)} (was 179)")
    logger.info(f"   • Products: {len(products_df)} (was 100)")
    logger.info(f"   • Sales: {len(sales_df)} (was 1,509)")
    logger.info(f"   • Total Revenue: ${sales_df['TotalAmount'].sum():,.2f}")
    logger.info(
        f"   • Date Range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}"
    )
    logger.info(f"   • Backup Location: {BACKUP_DIR}")
    logger.info("=" * 60)


def main():
    """Main execution function."""
    init_logger()

    logger.info("=" * 60)
    logger.info("SMART STORE DATA EXPANSION FOR P7")
    logger.info("=" * 60)
    logger.info(f"Target customers: {NUM_CUSTOMERS}")
    logger.info(f"Target products: {NUM_PRODUCTS}")
    logger.info(f"Target transactions: {NUM_TRANSACTIONS}")
    logger.info(f"Date range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    logger.info("=" * 60)

    # Step 1: Backup existing data
    backup_existing_data()

    # Step 2: Generate new data
    logger.info("\n🔄 Generating expanded datasets...")

    customers_df = generate_customers_data()
    products_df = generate_products_data()
    sales_df = generate_sales_data(customers_df, products_df)

    # Step 3: Save data
    save_data(customers_df, products_df, sales_df)

    logger.info("\n✅ Data expansion complete!")
    logger.info("Next steps:")
    logger.info(
        "1. Run data preparation scripts: python src\\analytics_project\\run_all_data_prep.py"
    )
    logger.info("2. Rebuild data warehouse: python src\\analytics_project\\create_warehouse.py")
    logger.info("3. Load new data: python src\\analytics_project\\load_warehouse.py")
    logger.info("4. Verify with queries: python src\\analytics_project\\query_warehouse.py")


if __name__ == "__main__":
    main()
