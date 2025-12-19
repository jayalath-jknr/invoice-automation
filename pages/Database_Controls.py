import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any
from bson import ObjectId
from bson.decimal128 import Decimal128

from src.storage.database import (
    db,
    cleanup_old_temp_uploads,
    create_vendor,
    get_all_vendors,
    save_vendor_regex_template,
    get_vendor_regex_patterns,
    insert_master_category,
    get_all_category_names
)

st.set_page_config(page_title="Database Controls", page_icon="🔧", layout="wide")

st.title("🔧 Database Administration")
st.markdown("System maintenance, vendor management, category management, and bulk operations")

# Initialize session state
if "admin_tab" not in st.session_state:
    st.session_state.admin_tab = "Maintenance"

# Tab selection
tab1, tab2, tab3, tab4 = st.tabs(["🧹 Maintenance", "👥 Vendor Management", "🏷️ Category Management", "📦 Bulk Operations"])

# TAB 1: MAINTENANCE
with tab1:
    st.header("🧹 Database Maintenance")
    
    st.markdown("### 📊 Database Statistics")
    
    try:
        # Get collection counts
        invoices_count = db.invoices.count_documents({})
        line_items_count = db.line_items.count_documents({})
        vendors_count = db.vendors.count_documents({})
        restaurants_count = db.restaurants.count_documents({})
        categories_count = db.categories.count_documents({})
        temp_uploads_count = db.temp_uploads.count_documents({})
        
        col1, col2, col3 = st.columns(3)
        col1.metric("📄 Invoices", f"{invoices_count:,}")
        col2.metric("📦 Line Items", f"{line_items_count:,}")
        col3.metric("👥 Vendors", f"{vendors_count:,}")
        
        col4, col5, col6 = st.columns(3)
        col4.metric("🏢 Restaurants", f"{restaurants_count:,}")
        col5.metric("🏷️ Categories", f"{categories_count:,}")
        col6.metric("⏳ Temp Uploads", f"{temp_uploads_count:,}")
        
    except Exception as e:
        st.error(f"Error fetching statistics: {str(e)}")
    
    st.divider()
    
    # Cleanup operations
    st.markdown("### 🧹 Cleanup Operations")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Clean Temporary Uploads")
        st.markdown("Remove old temporary upload sessions from the database")
        
        days_to_keep = st.number_input(
            "Keep uploads from last (days)",
            min_value=1,
            max_value=365,
            value=7,
            help="Delete temporary uploads older than this many days"
        )
        
        if st.button("🗑️ Clean Old Temp Uploads", type="primary", use_container_width=True):
            try:
                with st.spinner("Cleaning temporary uploads..."):
                    deleted_count = cleanup_old_temp_uploads(days=days_to_keep)
                    st.success(f"✅ Cleaned {deleted_count} temporary upload(s) older than {days_to_keep} days")
            except Exception as e:
                st.error(f"Error during cleanup: {str(e)}")
    
    with col2:
        st.markdown("#### Database Info")
        st.markdown("View database connection and storage details")
        
        if st.button("📊 Show Database Info", use_container_width=True):
            try:
                # Get database stats
                db_stats = db.command("dbStats")
                
                st.json({
                    "Database Name": db.name,
                    "Collections": db_stats.get("collections", "N/A"),
                    "Data Size": f"{db_stats.get('dataSize', 0) / 1024 / 1024:.2f} MB",
                    "Storage Size": f"{db_stats.get('storageSize', 0) / 1024 / 1024:.2f} MB",
                    "Indexes": db_stats.get("indexes", "N/A"),
                    "Index Size": f"{db_stats.get('indexSize', 0) / 1024 / 1024:.2f} MB"
                })
            except Exception as e:
                st.error(f"Error fetching database info: {str(e)}")

# TAB 2: VENDOR MANAGEMENT
with tab2:
    st.header("👥 Vendor Management")
    
    # List existing vendors
    st.markdown("### 📋 Existing Vendors")
    
    try:
        vendors = get_all_vendors()
        
        if vendors:
            # Create display dataframe
            vendor_data = []
            for vendor in vendors:
                # Get additional vendor details
                full_vendor = db.vendors.find_one({"_id": vendor["_id"]})
                if full_vendor:
                    vendor_data.append({
                        "Name": vendor["name"],
                        "Email": full_vendor.get("email", ""),
                        "Phone": full_vendor.get("phone", ""),
                        "Website": full_vendor.get("website", ""),
                        "Active": "✅" if full_vendor.get("is_active", True) else "❌",
                        "_id": str(vendor["_id"])
                    })
            
            vendor_df = pd.DataFrame(vendor_data)
            
            # Display vendors
            st.dataframe(
                vendor_df.drop(columns=["_id"]),
                use_container_width=True,
                hide_index=True
            )
            
            st.info(f"📊 Total Vendors: {len(vendors)}")
        else:
            st.info("No vendors found in the database.")
    
    except Exception as e:
        st.error(f"Error loading vendors: {str(e)}")
    
    st.divider()
    
    # Add new vendor
    st.markdown("### ➕ Add New Vendor")
    
    col1, col2 = st.columns(2)
    
    with col1:
        new_vendor_name = st.text_input("Vendor Name *", placeholder="e.g., US Foods")
        new_vendor_email = st.text_input("Email", placeholder="contact@vendor.com")
        new_vendor_phone = st.text_input("Phone", placeholder="(555) 123-4567")
    
    with col2:
        new_vendor_website = st.text_input("Website", placeholder="https://vendor.com")
        new_vendor_address = st.text_area("Address", placeholder="123 Main St, City, State ZIP")
        new_vendor_active = st.checkbox("Active Vendor", value=True)
    
    if st.button("💾 Add Vendor", type="primary"):
        if not new_vendor_name:
            st.error("❌ Vendor name is required")
        else:
            try:
                vendor_data = {
                    "name": new_vendor_name,
                    "email": new_vendor_email if new_vendor_email else None,
                    "phone": new_vendor_phone if new_vendor_phone else None,
                    "website": new_vendor_website if new_vendor_website else None,
                    "address": new_vendor_address if new_vendor_address else None,
                    "is_active": new_vendor_active
                }
                
                vendor_id = create_vendor(vendor_data)
                
                if vendor_id:
                    st.success(f"✅ Vendor added successfully! ID: {vendor_id}")
                    st.rerun()
                else:
                    st.error("❌ Error adding vendor: Failed to create vendor")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    st.divider()
    
    # Manage vendor regex templates
    st.markdown("### 🔧 Vendor Regex Templates")
    st.markdown("Configure extraction patterns for specific vendors (advanced)")
    
    if vendors:
        selected_vendor_name = st.selectbox(
            "Select Vendor",
            [""] + [v["name"] for v in vendors],
            help="Choose a vendor to view or edit regex templates"
        )
        
        if selected_vendor_name:
            selected_vendor = next((v for v in vendors if v["name"] == selected_vendor_name), None)
            
            if selected_vendor:
                vendor_id = str(selected_vendor["_id"])
                
                # Load existing patterns
                existing_patterns = get_vendor_regex_patterns(vendor_id)
                
                if existing_patterns:
                    st.info(f"✅ Regex templates exist for {selected_vendor_name}")
                    
                    with st.expander("View Existing Patterns"):
                        pattern_labels = [
                            "Invoice Number",
                            "Invoice Date",
                            "Invoice Total Amount",
                            "Order Date",
                            "Line Item Block Start",
                            "Line Item Block End",
                            "Quantity",
                            "Description",
                            "Unit",
                            "Unit Price",
                            "Line Total"
                        ]
                        
                        for idx, (label, pattern) in enumerate(zip(pattern_labels, existing_patterns)):
                            st.text_input(f"{idx}. {label}", value=pattern, disabled=True, key=f"existing_{idx}")
                else:
                    st.warning(f"⚠️ No regex templates found for {selected_vendor_name}")
                
                st.markdown("**Note:** Regex template management requires technical knowledge. Contact system administrator for pattern configuration.")

# TAB 3: CATEGORY MANAGEMENT
with tab3:
    st.header("🏷️ Category Management")
    
    # List existing categories
    st.markdown("### 📋 Existing Categories")
    
    try:
        # Get all categories from database
        categories = list(db.categories.find({}))
        
        if categories:
            # Create display dataframe
            cat_data = []
            for cat in categories:
                cat_data.append({
                    "Name": cat.get("name", ""),
                    "Type": cat.get("type", ""),
                    "_id": str(cat["_id"])
                })
            
            cat_df = pd.DataFrame(cat_data)
            
            # Check if all types are empty
            has_types = cat_df["Type"].notna().any() and (cat_df["Type"] != "").any()
            
            if has_types:
                # Group by type
                for cat_type in cat_df["Type"].unique():
                    if cat_type:  # Skip empty types
                        with st.expander(f"📁 {cat_type}", expanded=True):
                            type_cats = cat_df[cat_df["Type"] == cat_type].drop(columns=["_id", "Type"])
                            st.dataframe(
                                type_cats,
                                width='stretch',
                                hide_index=True
                            )
                
                # Show uncategorized if any
                uncategorized = cat_df[(cat_df["Type"].isna()) | (cat_df["Type"] == "")]
                if not uncategorized.empty:
                    with st.expander("📁 Uncategorized", expanded=True):
                        st.dataframe(
                            uncategorized.drop(columns=["_id", "Type"]),
                            width='stretch',
                            hide_index=True
                        )
            else:
                # Display all categories in a single table if no types
                st.dataframe(
                    cat_df.drop(columns=["_id"]),
                    width='stretch',
                    hide_index=True
                )
            
            st.info(f"📊 Total Categories: {len(categories)}")
        else:
            st.info("No categories found in the database.")
    
    except Exception as e:
        st.error(f"Error loading categories: {str(e)}")
    
    st.divider()
    
    # Add new category
    st.markdown("### ➕ Add New Category")
    
    col1, col2 = st.columns(2)
    
    with col1:
        new_cat_name = st.text_input("Category Name *", placeholder="e.g., Dairy Products")
    
    with col2:
        new_cat_type = st.selectbox(
            "Category Type *",
            ["", "Food", "Beverage", "Supplies", "Equipment", "Service", "Other"],
            help="Select the type of category"
        )
    
    if st.button("💾 Add Category", type="primary"):
        if not new_cat_name or not new_cat_type:
            st.error("❌ Category name and type are required")
        else:
            try:
                # Insert category into database
                cat_doc = {
                    "name": new_cat_name,
                    "type": new_cat_type
                }
                
                result = db.categories.insert_one(cat_doc)
                
                if result.inserted_id:
                    st.success(f"✅ Category added successfully! ID: {result.inserted_id}")
                    # Also add to master category list
                    insert_master_category(new_cat_name)
                    st.rerun()
                else:
                    st.error("❌ Error adding category")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    st.divider()
    
    # Category mapping info
    st.markdown("### 🔗 Category Mapping")
    st.markdown("Line items are automatically categorized during invoice processing using LLM-based categorization.")
    
    try:
        # Show sample of categorized items
        sample_items = list(db.line_items.find({"category": {"$exists": True, "$ne": None}}).limit(10))
        
        if sample_items:
            st.markdown("**Sample Categorized Items:**")
            sample_data = []
            for item in sample_items:
                sample_data.append({
                    "Description": item.get("description", "")[:50],
                    "Category": item.get("category", "Uncategorized")
                })
            
            st.dataframe(pd.DataFrame(sample_data), use_container_width=True, hide_index=True)
        else:
            st.info("No categorized items found. Categories are assigned during invoice processing.")
    
    except Exception as e:
        st.error(f"Error loading sample: {str(e)}")

# TAB 4: BULK OPERATIONS
with tab4:
    st.header("📦 Bulk Operations")
    
    st.markdown("### 📥 Export Data")
    st.markdown("Export database collections to CSV format")
    
    # Export invoices
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Export Invoices")
        
        export_date_range = st.selectbox(
            "Date Range",
            ["Last 30 Days", "Last 90 Days", "This Year", "All Time"],
            key="export_date"
        )
        
        if st.button("📥 Export Invoices to CSV", use_container_width=True):
            try:
                with st.spinner("Preparing export..."):
                    # Build query based on date range
                    query = {}
                    if export_date_range == "Last 30 Days":
                        cutoff = datetime.now() - timedelta(days=30)
                        query["invoice_date"] = {"$gte": cutoff}
                    elif export_date_range == "Last 90 Days":
                        cutoff = datetime.now() - timedelta(days=90)
                        query["invoice_date"] = {"$gte": cutoff}
                    elif export_date_range == "This Year":
                        cutoff = datetime(datetime.now().year, 1, 1)
                        query["invoice_date"] = {"$gte": cutoff}
                    
                    # Fetch invoices
                    invoices = list(db.invoices.find(query))
                    
                    if not invoices:
                        st.warning("No invoices found for the selected date range.")
                    else:
                        # Prepare data
                        export_data = []
                        for inv in invoices:
                            # Get vendor name
                            vendor = db.vendors.find_one({"_id": inv.get("vendor_id")})
                            vendor_name = vendor["name"] if vendor else "Unknown"
                            
                            # Convert Decimal128 to float
                            total = inv.get("invoice_total_amount", 0)
                            if isinstance(total, Decimal128):
                                total = float(total.to_decimal())
                            
                            export_data.append({
                                "Invoice ID": str(inv["_id"]),
                                "Invoice Number": inv.get("invoice_number", ""),
                                "Date": inv.get("invoice_date", "").strftime("%Y-%m-%d") if isinstance(inv.get("invoice_date"), datetime) else "",
                                "Vendor": vendor_name,
                                "Total Amount": total,
                                "Order Number": inv.get("order_number", "")
                            })
                        
                        export_df = pd.DataFrame(export_data)
                        csv = export_df.to_csv(index=False)
                        
                        st.download_button(
                            label=f"💾 Download {len(invoices)} Invoices",
                            data=csv,
                            file_name=f"invoices_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                        
                        st.success(f"✅ Ready to download {len(invoices)} invoices")
            
            except Exception as e:
                st.error(f"Error exporting invoices: {str(e)}")
    
    with col2:
        st.markdown("#### Export Line Items")
        
        export_li_date_range = st.selectbox(
            "Date Range",
            ["Last 30 Days", "Last 90 Days", "This Year", "All Time"],
            key="export_li_date"
        )
        
        if st.button("📥 Export Line Items to CSV", use_container_width=True):
            try:
                with st.spinner("Preparing export..."):
                    # Build query for invoices based on date range
                    invoice_query = {}
                    if export_li_date_range == "Last 30 Days":
                        cutoff = datetime.now() - timedelta(days=30)
                        invoice_query["invoice_date"] = {"$gte": cutoff}
                    elif export_li_date_range == "Last 90 Days":
                        cutoff = datetime.now() - timedelta(days=90)
                        invoice_query["invoice_date"] = {"$gte": cutoff}
                    elif export_li_date_range == "This Year":
                        cutoff = datetime(datetime.now().year, 1, 1)
                        invoice_query["invoice_date"] = {"$gte": cutoff}
                    
                    # Get invoice IDs
                    invoices = list(db.invoices.find(invoice_query, {"_id": 1, "invoice_number": 1, "vendor_id": 1}))
                    invoice_ids = [inv["_id"] for inv in invoices]
                    
                    # Create lookup dict for invoice numbers
                    inv_lookup = {inv["_id"]: inv for inv in invoices}
                    
                    if not invoice_ids:
                        st.warning("No invoices found for the selected date range.")
                    else:
                        # Fetch line items
                        line_items = list(db.line_items.find({"invoice_id": {"$in": invoice_ids}}))
                        
                        if not line_items:
                            st.warning("No line items found.")
                        else:
                            # Prepare data
                            export_data = []
                            for li in line_items:
                                invoice = inv_lookup.get(li.get("invoice_id"))
                                
                                # Get vendor name
                                vendor_name = "Unknown"
                                if invoice:
                                    vendor = db.vendors.find_one({"_id": invoice.get("vendor_id")})
                                    vendor_name = vendor["name"] if vendor else "Unknown"
                                
                                # Convert Decimal128 fields
                                quantity = li.get("quantity", 0)
                                if isinstance(quantity, Decimal128):
                                    quantity = float(quantity.to_decimal())
                                
                                unit_price = li.get("unit_price", 0)
                                if isinstance(unit_price, Decimal128):
                                    unit_price = float(unit_price.to_decimal())
                                
                                line_total = li.get("line_total", 0)
                                if isinstance(line_total, Decimal128):
                                    line_total = float(line_total.to_decimal())
                                
                                export_data.append({
                                    "Invoice Number": invoice.get("invoice_number", "") if invoice else "",
                                    "Vendor": vendor_name,
                                    "Line Number": li.get("line_number", ""),
                                    "Description": li.get("description", ""),
                                    "Quantity": quantity,
                                    "Unit": li.get("unit", ""),
                                    "Unit Price": unit_price,
                                    "Line Total": line_total,
                                    "Category": li.get("category", "")
                                })
                            
                            export_df = pd.DataFrame(export_data)
                            csv = export_df.to_csv(index=False)
                            
                            st.download_button(
                                label=f"💾 Download {len(line_items)} Line Items",
                                data=csv,
                                file_name=f"line_items_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                            
                            st.success(f"✅ Ready to download {len(line_items)} line items")
            
            except Exception as e:
                st.error(f"Error exporting line items: {str(e)}")
    
    st.divider()
    
    # Bulk delete warning
    st.markdown("### 🗑️ Bulk Delete Operations")
    st.warning("⚠️ **Caution:** Bulk delete operations are permanent and cannot be undone. Use with extreme care.")
    st.markdown("For safety, bulk delete operations should be performed manually through MongoDB tools or contact system administrator.")

# Footer
st.divider()
st.caption(f"🔧 Database Administration | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Help section
with st.expander("ℹ️ Help & Information"):
    st.markdown("""
    ### Database Administration Guide
    
    #### **Maintenance Tab**
    - **Database Statistics**: View document counts for all collections
    - **Cleanup Operations**: Remove old temporary upload sessions
    - **Database Info**: View storage and index details
    
    #### **Vendor Management Tab**
    - **View Vendors**: Browse all vendors in the database
    - **Add Vendor**: Create new vendor records manually
    - **Regex Templates**: Configure advanced extraction patterns (admin only)
    
    #### **Category Management Tab**
    - **View Categories**: Browse categories organized by type
    - **Add Category**: Create new category types
    - **Category Mapping**: Items are auto-categorized during processing
    
    #### **Bulk Operations Tab**
    - **Export Invoices**: Download invoice data as CSV
    - **Export Line Items**: Download line item data as CSV
    - **Date Ranges**: Filter exports by time period
    
    #### **Best Practices**
    - Run cleanup operations weekly to maintain performance
    - Backup data before bulk operations
    - Verify vendor information after manual entry
    - Use consistent category naming conventions
    """)
