"""
CSV Data Import Script
Imports CSV files from the 'files' folder into MongoDB with proper ObjectId conversion.

This script:
1. Creates "Westman's Bagel & Coffee - Capitol Hill" restaurant
2. Imports all CSV files with UUID → ObjectId conversion
3. Preserves relationships between collections
4. Validates data integrity

Run this after backing up your current database.
"""

import os
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId
import uuid

# Load environment
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME", "invoice_processing_db")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Path to CSV files
CSV_DIR = Path(__file__).parent / "files"

# UUID to ObjectId mapping (for maintaining relationships)
uuid_to_oid_map = {}


def uuid_to_objectid(uuid_str):
    """Convert UUID string to ObjectId, maintaining mapping for relationships."""
    if not uuid_str or pd.isna(uuid_str):
        return None
    
    uuid_str = str(uuid_str).strip()
    
    # If it's already an ObjectId format (24 hex chars), use it directly
    if len(uuid_str) == 24:
        try:
            return ObjectId(uuid_str)
        except:
            pass
    
    # Check if we've already mapped this UUID
    if uuid_str in uuid_to_oid_map:
        return uuid_to_oid_map[uuid_str]
    
    # Create new ObjectId and store mapping
    new_oid = ObjectId()
    uuid_to_oid_map[uuid_str] = new_oid
    return new_oid


def parse_date(date_str):
    """Parse date string to datetime object."""
    if pd.isna(date_str):
        return None
    try:
        return pd.to_datetime(date_str)
    except:
        return None


def safe_float(value):
    """Convert value to float safely."""
    if pd.isna(value):
        return 0.0
    try:
        return float(value)
    except:
        return 0.0


def safe_int(value):
    """Convert value to int safely."""
    if pd.isna(value):
        return 0
    try:
        return int(value)
    except:
        return 0


def create_capitol_hill_restaurant():
    """Create the 'Westman's Bagel & Coffee - Capitol Hill' restaurant."""
    print("\n=== Creating Capitol Hill Restaurant ===")
    
    # Use the specific ObjectId from CSV files
    capitol_hill_oid = ObjectId("507f1f77bcf86cd799439011")
    
    # Check if it already exists
    existing = db.restaurants.find_one({"_id": capitol_hill_oid})
    if existing:
        print(f"✅ Restaurant already exists: {existing['name']}")
        return capitol_hill_oid
    
    restaurant_doc = {
        "_id": capitol_hill_oid,
        "name": "Westman's Bagel & Coffee - Capitol Hill",
        "location_name": "Capitol Hill",
        "address": "123 Capitol Hill Blvd, Seattle, WA 98102",
        "phone_number": "(206) 555-0142",
        "restaurant_type": "Coffee Shop & Bakery",
        "created_at": datetime.now()
    }
    
    db.restaurants.insert_one(restaurant_doc)
    print(f"✅ Created restaurant: {restaurant_doc['name']}")
    print(f"   ObjectId: {capitol_hill_oid}")
    
    return capitol_hill_oid


def import_restaurants():
    """Import restaurants from CSV."""
    print("\n=== Importing Restaurants ===")
    
    csv_path = CSV_DIR / "restaurants.csv"
    if not csv_path.exists():
        print("⚠️  restaurants.csv not found, skipping")
        return
    
    df = pd.read_csv(csv_path)
    print(f"Found {len(df)} restaurants in CSV")
    
    imported = 0
    for _, row in df.iterrows():
        # Convert UUID to ObjectId
        oid = uuid_to_objectid(row['_id'])
        
        restaurant_doc = {
            "_id": oid,
            "name": row.get('name', 'Unknown'),
            "location_name": row.get('location_name', ''),
            "address": row.get('address', ''),
            "phone_number": row.get('phone_number', ''),
            "restaurant_type": row.get('restaurant_type', ''),
            "created_at": parse_date(row.get('created_at')) or datetime.now()
        }
        
        # Insert or update
        db.restaurants.update_one(
            {"_id": oid},
            {"$set": restaurant_doc},
            upsert=True
        )
        imported += 1
    
    print(f"✅ Imported {imported} restaurants")


def import_vendors():
    """Import vendors from CSV."""
    print("\n=== Importing Vendors ===")
    
    csv_path = CSV_DIR / "vendors.csv"
    if not csv_path.exists():
        print("⚠️  vendors.csv not found, skipping")
        return
    
    df = pd.read_csv(csv_path)
    print(f"Found {len(df)} vendors in CSV")
    
    imported = 0
    for _, row in df.iterrows():
        oid = uuid_to_objectid(row['_id'])
        
        vendor_doc = {
            "_id": oid,
            "name": row.get('name', 'Unknown'),
            "contact_info": row.get('contact_info', ''),
            "category": row.get('category', ''),
            "payment_terms": row.get('payment_terms', ''),
            "created_at": parse_date(row.get('created_at')) or datetime.now()
        }
        
        db.vendors.update_one(
            {"_id": oid},
            {"$set": vendor_doc},
            upsert=True
        )
        imported += 1
    
    print(f"✅ Imported {imported} vendors")


def import_categories():
    """Import categories from CSV."""
    print("\n=== Importing Categories ===")
    
    csv_path = CSV_DIR / "categories.csv"
    if not csv_path.exists():
        print("⚠️  categories.csv not found, skipping")
        return
    
    df = pd.read_csv(csv_path)
    print(f"Found {len(df)} categories in CSV")
    
    imported = 0
    for _, row in df.iterrows():
        category_doc = {
            "name": row.get('name', 'Uncategorized'),
            "type": row.get('type', '')
        }
        
        # Check if exists by name first
        existing = db.categories.find_one({"name": category_doc["name"]})
        if existing:
            # Update existing (don't modify _id)
            db.categories.update_one(
                {"name": category_doc["name"]},
                {"$set": category_doc}
            )
        else:
            # Insert new with _id
            oid = uuid_to_objectid(row.get('_id')) if '_id' in row and row['_id'] else ObjectId()
            category_doc["_id"] = oid
            db.categories.insert_one(category_doc)
        
        imported += 1
    
    print(f"✅ Imported {imported} categories")


def import_invoices():
    """Import invoices from CSV."""
    print("\n=== Importing Invoices ===")
    
    csv_path = CSV_DIR / "invoices.csv"
    if not csv_path.exists():
        print("⚠️  invoices.csv not found, skipping")
        return
    
    df = pd.read_csv(csv_path)
    print(f"Found {len(df)} invoices in CSV")
    
    imported = 0
    for _, row in df.iterrows():
        oid = uuid_to_objectid(row['_id'])
        restaurant_oid = uuid_to_objectid(row['restaurant_id'])
        vendor_oid = uuid_to_objectid(row['vendor_id'])
        
        invoice_doc = {
            "_id": oid,
            "filename": row.get('filename', ''),
            "restaurant_id": restaurant_oid,
            "vendor_id": vendor_oid,
            "invoice_number": str(row.get('invoice_number', '')),
            "invoice_date": parse_date(row.get('invoice_date')),
            "invoice_total_amount": safe_float(row.get('invoice_total_amount')),
            "text_length": safe_int(row.get('text_length')),
            "page_count": safe_int(row.get('page_count')),
            "extraction_timestamp": parse_date(row.get('extraction_timestamp')) or datetime.now(),
            "order_date": parse_date(row.get('order_date'))
        }
        
        db.invoices.update_one(
            {"_id": oid},
            {"$set": invoice_doc},
            upsert=True
        )
        imported += 1
    
    print(f"✅ Imported {imported} invoices")


def import_line_items():
    """Import line items from CSV."""
    print("\n=== Importing Line Items ===")
    
    csv_path = CSV_DIR / "line_items.csv"
    if not csv_path.exists():
        print("⚠️  line_items.csv not found, skipping")
        return
    
    df = pd.read_csv(csv_path)
    print(f"Found {len(df)} line items in CSV")
    
    imported = 0
    for _, row in df.iterrows():
        oid = uuid_to_objectid(row['_id'])
        invoice_oid = uuid_to_objectid(row['invoice_id'])
        
        line_item_doc = {
            "_id": oid,
            "invoice_id": invoice_oid,
            "vendor_name": row.get('vendor_name', ''),
            "category": row.get('category', 'Uncategorized'),
            "quantity": safe_float(row.get('quantity')),
            "unit": row.get('unit', ''),
            "description": row.get('description', ''),
            "unit_price": safe_float(row.get('unit_price')),
            "line_total": safe_float(row.get('line_total')),
            "line_number": safe_float(row.get('line_number'))
        }
        
        db.line_items.update_one(
            {"_id": oid},
            {"$set": line_item_doc},
            upsert=True
        )
        imported += 1
    
    print(f"✅ Imported {imported} line items")


def import_sales():
    """Import sales data from CSV."""
    print("\n=== Importing Sales Data ===")
    
    csv_path = CSV_DIR / "sales.csv"
    if not csv_path.exists():
        print("⚠️  sales.csv not found, skipping")
        return
    
    df = pd.read_csv(csv_path)
    print(f"Found {len(df)} sales records in CSV")
    
    imported = 0
    for _, row in df.iterrows():
        oid = uuid_to_objectid(row.get('_id')) if '_id' in row and row['_id'] else ObjectId()
        restaurant_oid = uuid_to_objectid(row.get('restaurant_id'))
        
        sales_doc = {
            "_id": oid,
            "restaurant_id": restaurant_oid,
            "date": parse_date(row.get('date')),
            "revenue": safe_float(row.get('revenue')),
            "covers": safe_int(row.get('covers'))
        }
        
        db.sales.update_one(
            {"_id": oid},
            {"$set": sales_doc},
            upsert=True
        )
        imported += 1
    
    print(f"✅ Imported {imported} sales records")


def import_misc_collections():
    """Import other collections like item_lookup_map, vendor_regex_templates."""
    print("\n=== Importing Miscellaneous Collections ===")
    
    # Item lookup map
    csv_path = CSV_DIR / "item_lookup_map.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        print(f"Found {len(df)} item lookup entries")
        
        for _, row in df.iterrows():
            doc = {
                "item_pattern": row.get('item_pattern', ''),
                "category": row.get('category', 'Uncategorized')
            }
            
            # Check if exists
            existing = db.item_lookup_map.find_one({"item_pattern": doc["item_pattern"]})
            if existing:
                db.item_lookup_map.update_one(
                    {"item_pattern": doc["item_pattern"]},
                    {"$set": doc}
                )
            else:
                doc["_id"] = ObjectId()
                db.item_lookup_map.insert_one(doc)
        print(f"✅ Imported item_lookup_map")
    
    # Vendor regex templates
    csv_path = CSV_DIR / "vendor_regex_templates.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        print(f"Found {len(df)} vendor regex templates")
        
        for _, row in df.iterrows():
            oid = uuid_to_objectid(row.get('_id')) if '_id' in row else ObjectId()
            vendor_oid = uuid_to_objectid(row.get('vendor_id'))
            
            doc = {
                "_id": oid,
                "vendor_id": vendor_oid,
                "pattern": row.get('pattern', ''),
                "confidence": safe_float(row.get('confidence'))
            }
            db.vendor_regex_templates.update_one(
                {"_id": oid},
                {"$set": doc},
                upsert=True
            )
        print(f"✅ Imported vendor_regex_templates")


def verify_import():
    """Verify the import was successful."""
    print("\n=== Verification ===")
    
    counts = {
        "restaurants": db.restaurants.count_documents({}),
        "vendors": db.vendors.count_documents({}),
        "categories": db.categories.count_documents({}),
        "invoices": db.invoices.count_documents({}),
        "line_items": db.line_items.count_documents({}),
        "sales": db.sales.count_documents({})
    }
    
    for collection, count in counts.items():
        print(f"  {collection}: {count} documents")
    
    # Check Capitol Hill specifically
    capitol_hill = db.restaurants.find_one({"name": {"$regex": "Capitol Hill", "$options": "i"}})
    if capitol_hill:
        print(f"\n✅ Capitol Hill restaurant found: {capitol_hill['name']}")
        print(f"   ObjectId: {capitol_hill['_id']}")
        
        # Check invoices for this restaurant
        invoice_count = db.invoices.count_documents({"restaurant_id": capitol_hill['_id']})
        print(f"   Invoices: {invoice_count}")
    else:
        print("\n⚠️  Capitol Hill restaurant not found!")


def main():
    print("╔═══════════════════════════════════════════════════════════╗")
    print("║         CSV Data Import Script                            ║")
    print("║         Importing files folder data to MongoDB            ║")
    print("╚═══════════════════════════════════════════════════════════╝")
    
    response = input("\n⚠️  This will add/update data in your database. Continue? (yes/no): ")
    if response.lower() != "yes":
        print("❌ Import cancelled.")
        return
    
    print("\n🚀 Starting import...")
    
    try:
        # Import in order (respecting foreign key relationships)
        create_capitol_hill_restaurant()
        import_restaurants()
        import_vendors()
        import_categories()
        import_invoices()
        import_line_items()
        import_sales()
        import_misc_collections()
        
        verify_import()
        
        print("\n" + "="*60)
        print("✅ Import completed successfully!")
        print("="*60)
        print(f"\n📊 UUID to ObjectId mappings created: {len(uuid_to_oid_map)}")
        
    except Exception as e:
        print(f"\n❌ Import failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()


if __name__ == "__main__":
    main()
