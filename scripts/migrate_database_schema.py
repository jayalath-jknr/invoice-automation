"""
Database Schema Migration Script
- Standardize date field to 'invoice_date' (remove 'date')
- Convert Decimal128 to float for all monetary fields
- Remove 'total_amount' field (keep invoice_total_amount)
"""

from pymongo import MongoClient
from dotenv import load_dotenv
from bson import Decimal128
import os

load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME", "invoice_processing_db")

client = MongoClient(MONGODB_URI)
db = client[DB_NAME]

def decimal128_to_float(value):
    """Convert Decimal128 to float."""
    if value is None:
        return 0.0
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    return float(value)

def migrate_invoices():
    """
    1. Standardize date field: date -> invoice_date
    2. Convert Decimal128 to float in invoice_total_amount
    3. Remove redundant 'total_amount' field
    """
    print("\n=== Migrating Invoices Collection ===")
    
    invoices = list(db.invoices.find({}))
    print(f"Found {len(invoices)} invoices to process")
    
    updated_count = 0
    for invoice in invoices:
        updates = {}
        unset_fields = {}
        
        # 1. Standardize date field
        if "date" in invoice and "invoice_date" not in invoice:
            # Move 'date' to 'invoice_date'
            updates["invoice_date"] = invoice["date"]
            unset_fields["date"] = ""
            print(f"  Invoice {invoice['_id']}: Moving 'date' to 'invoice_date'")
        elif "date" in invoice and "invoice_date" in invoice:
            # Both exist - remove 'date', keep 'invoice_date'
            unset_fields["date"] = ""
            print(f"  Invoice {invoice['_id']}: Removing duplicate 'date' field")
        
        # 2. Convert Decimal128 to float for invoice_total_amount
        if "invoice_total_amount" in invoice:
            if isinstance(invoice["invoice_total_amount"], Decimal128):
                updates["invoice_total_amount"] = decimal128_to_float(invoice["invoice_total_amount"])
                print(f"  Invoice {invoice['_id']}: Converting invoice_total_amount to float")
        
        # 3. Remove redundant 'total_amount' field if it exists
        if "total_amount" in invoice:
            unset_fields["total_amount"] = ""
            print(f"  Invoice {invoice['_id']}: Removing redundant 'total_amount' field")
        
        # Apply updates
        if updates or unset_fields:
            update_doc = {}
            if updates:
                update_doc["$set"] = updates
            if unset_fields:
                update_doc["$unset"] = unset_fields
            
            db.invoices.update_one({"_id": invoice["_id"]}, update_doc)
            updated_count += 1
    
    print(f"\n✅ Updated {updated_count} invoices")

def migrate_line_items():
    """
    Convert Decimal128 to float in:
    - unit_price
    - line_total
    - line_number
    """
    print("\n=== Migrating Line Items Collection ===")
    
    line_items = list(db.line_items.find({}))
    print(f"Found {len(line_items)} line items to process")
    
    updated_count = 0
    for item in line_items:
        updates = {}
        
        # Convert Decimal128 fields to float
        if "unit_price" in item and isinstance(item["unit_price"], Decimal128):
            updates["unit_price"] = decimal128_to_float(item["unit_price"])
        
        if "line_total" in item and isinstance(item["line_total"], Decimal128):
            updates["line_total"] = decimal128_to_float(item["line_total"])
        
        if "line_number" in item and isinstance(item["line_number"], Decimal128):
            updates["line_number"] = decimal128_to_float(item["line_number"])
        
        # Apply updates
        if updates:
            db.line_items.update_one({"_id": item["_id"]}, {"$set": updates})
            updated_count += 1
            if updated_count % 50 == 0:
                print(f"  Processed {updated_count} line items...")
    
    print(f"\n✅ Updated {updated_count} line items")

def verify_migration():
    """Verify the migration was successful."""
    print("\n=== Verification ===")
    
    # Check invoices
    invoices_with_date = db.invoices.count_documents({"date": {"$exists": True}})
    invoices_with_invoice_date = db.invoices.count_documents({"invoice_date": {"$exists": True}})
    invoices_with_decimal_total = db.invoices.count_documents({"invoice_total_amount": {"$type": "decimal"}})
    
    print(f"\nInvoices:")
    print(f"  - With 'date' field: {invoices_with_date} (should be 0)")
    print(f"  - With 'invoice_date' field: {invoices_with_invoice_date}")
    print(f"  - With Decimal128 invoice_total_amount: {invoices_with_decimal_total} (should be 0)")
    
    # Check line items
    line_items_with_decimal_price = db.line_items.count_documents({"unit_price": {"$type": "decimal"}})
    line_items_with_decimal_total = db.line_items.count_documents({"line_total": {"$type": "decimal"}})
    line_items_with_decimal_line_num = db.line_items.count_documents({"line_number": {"$type": "decimal"}})
    
    print(f"\nLine Items:")
    print(f"  - With Decimal128 unit_price: {line_items_with_decimal_price} (should be 0)")
    print(f"  - With Decimal128 line_total: {line_items_with_decimal_total} (should be 0)")
    print(f"  - With Decimal128 line_number: {line_items_with_decimal_line_num} (should be 0)")
    
    # Sample check
    sample_invoice = db.invoices.find_one({})
    if sample_invoice:
        print(f"\nSample Invoice:")
        print(f"  - invoice_date: {sample_invoice.get('invoice_date')} (type: {type(sample_invoice.get('invoice_date'))})")
        print(f"  - invoice_total_amount: {sample_invoice.get('invoice_total_amount')} (type: {type(sample_invoice.get('invoice_total_amount'))})")

def main():
    print("╔═══════════════════════════════════════════════════════════╗")
    print("║      Database Schema Migration Script                     ║")
    print("║      Standardizing to documentation schema                ║")
    print("╚═══════════════════════════════════════════════════════════╝")
    
    response = input("\n⚠️  This will modify your database. Continue? (yes/no): ")
    if response.lower() != "yes":
        print("❌ Migration cancelled.")
        return
    
    print("\n🚀 Starting migration...")
    
    try:
        migrate_invoices()
        migrate_line_items()
        verify_migration()
        
        print("\n" + "="*60)
        print("✅ Migration completed successfully!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()

if __name__ == "__main__":
    main()
