"""
Add dummy restaurant data to the database for testing.
Run this script once to populate the restaurants collection.
"""

from src.storage.database import db
from bson import ObjectId

def add_dummy_restaurants():
    """Add sample restaurants to the database."""
    
    # Check if restaurants already exist
    existing_count = db.restaurants.count_documents({})
    
    if existing_count > 0:
        print(f"Found {existing_count} existing restaurants.")
        response = input("Do you want to add more restaurants anyway? (y/n): ")
        if response.lower() != 'y':
            print("Cancelled.")
            return
    
    # Sample restaurants
    restaurants = [
        {
            "name": "The Gourmet Kitchen",
            "address": "123 Main Street, Downtown",
            "city": "Atlanta",
            "state": "GA",
            "zip": "30301",
            "phone": "(404) 555-0101",
            "email": "contact@gourmetkitchen.com",
            "type": "Fine Dining",
            "is_active": True
        },
        {
            "name": "Pizza Paradise",
            "address": "456 Oak Avenue",
            "city": "Atlanta",
            "state": "GA",
            "zip": "30302",
            "phone": "(404) 555-0102",
            "email": "info@pizzaparadise.com",
            "type": "Pizzeria",
            "is_active": True
        },
        {
            "name": "Sushi Supreme",
            "address": "789 Peachtree Street",
            "city": "Atlanta",
            "state": "GA",
            "zip": "30303",
            "phone": "(404) 555-0103",
            "email": "orders@sushisupreme.com",
            "type": "Japanese",
            "is_active": True
        },
        {
            "name": "Burger Haven",
            "address": "321 Maple Drive",
            "city": "Atlanta",
            "state": "GA",
            "zip": "30304",
            "phone": "(404) 555-0104",
            "email": "hello@burgerhaven.com",
            "type": "Fast Casual",
            "is_active": True
        },
        {
            "name": "Taco Fiesta",
            "address": "654 Elm Street",
            "city": "Atlanta",
            "state": "GA",
            "zip": "30305",
            "phone": "(404) 555-0105",
            "email": "contact@tacofiesta.com",
            "type": "Mexican",
            "is_active": True
        }
    ]
    
    print(f"\nAdding {len(restaurants)} restaurants to database...")
    
    inserted_ids = []
    for restaurant in restaurants:
        result = db.restaurants.insert_one(restaurant)
        inserted_ids.append(result.inserted_id)
        print(f"✓ Added: {restaurant['name']} (ID: {result.inserted_id})")
    
    print(f"\n✅ Successfully added {len(inserted_ids)} restaurants!")
    print("\nRestaurant IDs:")
    for i, (name, rest_id) in enumerate(zip([r["name"] for r in restaurants], inserted_ids), 1):
        print(f"  {i}. {name}: {rest_id}")
    
    # Now update some invoices to have restaurant_id
    print("\n" + "="*60)
    print("Updating sample invoices with restaurant IDs...")
    
    # Get some invoices
    invoices = list(db.invoices.find({}).limit(20))
    
    if not invoices:
        print("No invoices found to update.")
        return
    
    # Distribute invoices across restaurants
    import random
    updated_count = 0
    
    for invoice in invoices:
        # Randomly assign a restaurant
        random_restaurant_id = random.choice(inserted_ids)
        
        db.invoices.update_one(
            {"_id": invoice["_id"]},
            {"$set": {"restaurant_id": random_restaurant_id}}
        )
        updated_count += 1
    
    print(f"✓ Updated {updated_count} invoices with restaurant assignments")
    print("\n✅ Done! You can now use the Price Variations page.")


if __name__ == "__main__":
    print("="*60)
    print("Add Dummy Restaurants Script")
    print("="*60)
    
    try:
        add_dummy_restaurants()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
