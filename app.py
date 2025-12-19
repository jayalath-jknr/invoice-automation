import os
import streamlit as st
from pathlib import Path

# Ensure working directory is set to the app's location for cross-platform compatibility
APP_DIR = Path(__file__).parent.resolve()
os.chdir(APP_DIR)

# Define pages using relative paths (works now that cwd is set correctly)
upload = st.Page("pages/Upload_Invoices.py", icon='💼')

view_invoices = st.Page("pages/View_Invoices.py", icon='🎓') 

view_price_variations = st.Page("pages/View_Price_Variations.py", icon='📋') # For analysis report
database_controls = st.Page("pages/Database_Controls.py", icon='🧪') # Demo analysis report
dashboard = st.Page("pages/Dashboard.py", icon='📋')


# Group pages
pg = st.navigation({
    "Upload": [upload],
    "Analysis": [view_invoices, view_price_variations, dashboard], # Grouped analysis report
    "DB": [database_controls], 

})

# Run the navigation
pg.run()