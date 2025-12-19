## Author : Nithisha
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import logging
from datetime import datetime, timedelta
from src.storage.database import (
    db,
    get_vendor_name_by_id,
    get_invoice_line_items_joined,
    get_sales_data
)

# Configure logger
logger = logging.getLogger(__name__)

# ---------------------------
# Load & Prepare Data
# ---------------------------
@st.cache_data
def load_data():
    """Load invoice and line item data from MongoDB (real data, not demo)."""
    try:
        # Fetch invoices and line items from their collections
        invoices_list = list(db["invoices"].find({}))
        line_items_list = list(db["line_items"].find({}))

        if not invoices_list:
            st.error("No invoices found in database.")
            return None

        if not line_items_list:
            st.error("No line items found in database.")
            return None

        invoices = pd.DataFrame(invoices_list)
        line_items = pd.DataFrame(line_items_list)

    except Exception as e:
        st.error(f"Could not load data from database: {e}")
        return None

    # Normalize common invoice field names
    # Standardize to 'date' column for consistency in charts
    if "invoice_date" in invoices.columns:
        invoices["date"] = invoices["invoice_date"]
    elif "date" not in invoices.columns:
        invoices["date"] = pd.NaT  # Fallback if neither exists

    # Prefer invoice_total_amount if present
    if "invoice_total_amount" in invoices.columns:
        invoices["total_amount"] = invoices["invoice_total_amount"]
    elif "total_amount" not in invoices.columns:
        invoices["total_amount"] = np.nan

    # Ensure invoice date conversion early
    if "date" in invoices.columns:
        invoices["date"] = pd.to_datetime(invoices["date"], errors="coerce")

    # Merge - handle both ObjectId and string IDs
    # Convert ObjectId to string for merging
    invoices["_id_str"] = invoices["_id"].astype(str)
    if "invoice_id" in line_items.columns:
        line_items["invoice_id_str"] = line_items["invoice_id"].astype(str)
    elif "invoiceId" in line_items.columns:  # fallback
        line_items["invoice_id_str"] = line_items["invoiceId"].astype(str)
    else:
        st.error("Line items missing invoice_id field; cannot merge.")
        return None
    
    df = pd.merge(
        line_items,
        invoices[["_id_str", "vendor_id", "restaurant_id", "date", "total_amount"]],
        left_on="invoice_id_str",
        right_on="_id_str",
        how="left",
        suffixes=("_line", "_invoice")
    )

    if df.empty:
        st.warning("Merged dataset is empty.")
        return None

    # Ensure numeric columns - handle Decimal128 and float from MongoDB
    def safe_numeric(val):
        if pd.isna(val):
            return np.nan
        if hasattr(val, 'to_decimal'):  # Decimal128 (legacy)
            return float(val.to_decimal())
        try:
            return float(val)
        except (ValueError, TypeError) as e:
            logger.debug(f"Could not convert value '{val}' to numeric: {e}")
            return np.nan
    
    df["line_total"] = df["line_total"].apply(safe_numeric)
    df["unit_price"] = df["unit_price"].apply(safe_numeric)
    df["quantity"] = df["quantity"].apply(safe_numeric)
    
    # Ensure total_amount is numeric
    if "total_amount" in df.columns:
        df["total_amount"] = df["total_amount"].apply(safe_numeric)

    # Ensure date column and month period
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        # Drop rows without dates for time-based charts but keep for non-time charts
        df["month"] = df["date"].dt.to_period("M").astype(str)
    else:
        df["date"] = pd.NaT
        df["month"] = "Unknown"

    # vendor name lookup (robust)
    def safe_vendor(v):
        try:
            if pd.isna(v):
                return "Unknown"
            name = get_vendor_name_by_id(str(v))
            return name if name else "Unknown"
        except Exception as e:
            logger.warning(f"Error looking up vendor ID '{v}': {e}")
            return "Unknown"

    df["vendor_name"] = df["vendor_id"].apply(safe_vendor)

    # restaurant name lookup (robust)
    def safe_restaurant(r):
        try:
            if pd.isna(r):
                return "Unknown"
            restaurant = db["restaurants"].find_one({"_id": r})
            return restaurant["name"] if restaurant and "name" in restaurant else "Unknown"
        except Exception as e:
            logger.warning(f"Error looking up restaurant ID '{r}': {e}")
            return "Unknown"

    df["restaurant_name"] = df["restaurant_id"].apply(safe_restaurant)

    # Ensure category & description
    if "category" not in df.columns or df["category"].isnull().all():
        # create category column frinvoice_id_stription as fallback
        df["category"] = df.get("category", pd.Series(["Uncategorized"] * len(df)))
        # If category missing entirely, set 'Uncategorized' and we'll fallback in charts
        df["category"].fillna("Uncategorized", inplace=True)

    if "description" not in df.columns:
        df["description"] = "Unknown"

    # Ensure invoice id column exists
    if "invoice_id" not in df.columns:
        df["invoice_id"] = df.get("_id_line", np.nan)

    return df

@st.cache_data(ttl=300)
def load_sales_data(start_date, end_date, restaurant_ids=None):
    """Load sales data for food cost % calculation."""
    try:
        return get_sales_data(start_date, end_date, restaurant_ids)
    except Exception as e:
        logger.error(f"Failed to load sales data for {start_date} to {end_date}: {e}")
        st.warning(f"Could not load sales data: {e}")
        return pd.DataFrame(columns=["date", "location", "revenue", "covers"])

df = load_data()
if df is None:
    st.stop()

# ---------------------------
# Sidebar Filters
# ---------------------------
st.title("Invoice Analysis Dashboard — Comprehensive Analytics")

st.sidebar.header("Filters")

# Date range handling with validation
valid_dates = df["date"].dropna()
if valid_dates.empty:
    st.sidebar.error("⚠️ No valid dates found in invoices. Date filtering disabled.")
    min_date = pd.to_datetime("2020-01-01")
    max_date = pd.to_datetime("2025-12-31")
    use_date_filter = False
else:
    min_date = valid_dates.min()
    max_date = valid_dates.max()
    
    # Ensure min and max are different
    if min_date == max_date:
        st.sidebar.warning(f"⚠️ All invoices have the same date ({min_date.strftime('%Y-%m-%d')}). Date filtering disabled.")
        use_date_filter = False
    else:
        use_date_filter = True

# Only show slider if we have a valid date range
if use_date_filter:
    date_range = st.sidebar.slider(
        "Date range",
        min_value=min_date.to_pydatetime(),
        max_value=max_date.to_pydatetime(),
        value=(min_date.to_pydatetime(), max_date.to_pydatetime()),
        format="YYYY-MM-DD"
    )
    df_filtered = df[(df["date"] >= date_range[0]) & (df["date"] <= date_range[1])].copy()
else:
    st.sidebar.info(f"📅 Showing all data (dates: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')})")
    date_range = (min_date.to_pydatetime(), max_date.to_pydatetime())
    df_filtered = df.copy()

vendors = sorted(df["vendor_name"].dropna().unique().tolist())
selected_vendor = st.sidebar.selectbox("Select Vendor", ["All"] + vendors)

# Restaurant filter
restaurants = sorted([str(r) for r in df["restaurant_name"].dropna().unique().tolist()])
selected_restaurant = st.sidebar.selectbox("Select Restaurant", ["All"] + restaurants)

# Budget input
monthly_budget = st.sidebar.number_input(
    "Monthly purchasing budget (overall)",
    min_value=0.0,
    value=40000.0,
    step=1000.0,
    help="Used for Budget vs Actual indicators",
)

price_alert_threshold = st.sidebar.slider(
    "Price alert threshold (% change)",
    min_value=5,
    max_value=30,
    value=15,
    step=1,
)

# Apply filters
df_view = df_filtered.copy()
if selected_vendor != "All":
    df_view = df_view[df_view["vendor_name"] == selected_vendor]
if selected_restaurant != "All":
    df_view = df_view[df_view["restaurant_name"] == selected_restaurant]

if df_view.empty:
    st.warning("No data for chosen filters.")
    st.stop()

# Display filter info
st.sidebar.info(f"📍 Available restaurants: {', '.join(restaurants)}")

# Load sales data for food cost % calculation
restaurant_ids_filter = None
if selected_restaurant != "All":
    # Get restaurant _id from database
    try:
        restaurant_doc = db["restaurants"].find_one({"name": selected_restaurant})
        if restaurant_doc:
            restaurant_ids_filter = [str(restaurant_doc["_id"])]
    except Exception as e:
        logger.error(f"Failed to look up restaurant '{selected_restaurant}': {e}")
        st.warning(f"Could not filter by restaurant: {e}")

sales_df = load_sales_data(date_range[0], date_range[1], restaurant_ids_filter)

# ---------------------------
# Helper Functions
# ---------------------------
def safe_metric(value, fmt="{:,.0f}", default="N/A"):
    if value is None or (isinstance(value, (int, float)) and np.isnan(value)):
        return default
    try:
        return fmt.format(value)
    except Exception as e:
        logger.debug(f"Could not format value '{value}' with format '{fmt}': {e}")
        return str(value)

def safe_group_sum(df_in, by, value="line_total"):
    if df_in.empty:
        return pd.Series(dtype=float)
    g = df_in.groupby(by)[value].sum()
    return g.sort_values(ascending=False)

def sort_month_index(series_or_df):
    out = series_or_df.copy()
    try:
        out.index = pd.to_datetime(out.index)
        out = out.sort_index()
    except Exception as e:
        logger.debug(f"Could not sort month index: {e}")
    return out

# ---------------------------
# FRIEND'S ADVANCED ANALYTICS (from Cost_Analytics dashboard)
# ---------------------------

st.header("📊 Advanced Cost Analytics")

# Calculate KPIs for advanced analytics
period_end = pd.to_datetime(date_range[1])
last7_start = period_end - timedelta(days=6)
prev7_start = last7_start - timedelta(days=7)
prev7_end = last7_start - timedelta(days=1)

last7_inv = df_view[(df_view["date"] >= last7_start) & (df_view["date"] <= period_end)]
prev7_inv = df_view[(df_view["date"] >= prev7_start) & (df_view["date"] <= prev7_end)]

last7_spend = last7_inv["line_total"].sum()
prev7_spend = prev7_inv["line_total"].sum()

# Monthly spend current vs previous month
end_month = period_end.to_period("M")
current_month_start = end_month.to_timestamp()
prev_month = (end_month - 1).to_timestamp()
prev_month_end = current_month_start - pd.Timedelta(days=1)
prev_month_start = prev_month

current_month_inv = df_view[(df_view["date"] >= current_month_start) & (df_view["date"] <= period_end)]
prev_month_inv = df_view[(df_view["date"] >= prev_month_start) & (df_view["date"] <= prev_month_end)]

current_month_spend = current_month_inv["line_total"].sum()
prev_month_spend = prev_month_inv["line_total"].sum()

# Revenue & covers for cost % / cost per cover
total_purchases = df_view["line_total"].sum()
total_revenue = sales_df["revenue"].sum() if "revenue" in sales_df.columns else np.nan
total_covers = sales_df["covers"].sum() if "covers" in sales_df.columns else np.nan

food_cost_pct = (total_purchases / total_revenue * 100) if total_revenue and total_revenue > 0 else np.nan
cost_per_cover = (total_purchases / total_covers) if total_covers and total_covers > 0 else np.nan

# Key KPIs Row
st.subheader("Key Performance Indicators")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Food Cost %", value=safe_metric(food_cost_pct, "{:,.1f}%"))

month_delta = ((current_month_spend - prev_month_spend) / prev_month_spend * 100 if prev_month_spend > 0 else None)
with col2:
    st.metric(
        "Total Spend (Current Month)",
        value=safe_metric(current_month_spend, "${:,.0f}"),
        delta=safe_metric(month_delta, "{:+.1f}%") if month_delta is not None else "N/A",
    )

w_delta = ((last7_spend - prev7_spend) / prev7_spend * 100 if prev7_spend > 0 else None)
with col3:
    st.metric(
        "Spend (Last 7 Days)",
        value=safe_metric(last7_spend, "${:,.0f}"),
        delta=safe_metric(w_delta, "{:+.1f}%") if w_delta is not None else "N/A",
    )

with col4:
    st.metric("Cost per Cover", value=safe_metric(cost_per_cover, "${:,.2f}"))

st.markdown("---")

# Cost Structure & Opportunities
st.subheader("Cost Structure & Savings Opportunities")
c1, c2 = st.columns(2)

# Cost by category
with c1:
    st.caption("💰 Cost by Category")
    if df_view.empty:
        st.write("No data.")
    else:
        cat_df = df_view.groupby("category")["line_total"].sum().reset_index().sort_values("line_total", ascending=False)
        cat_chart = (
            alt.Chart(cat_df)
            .mark_arc(innerRadius=50)
            .encode(
                theta="line_total:Q",
                color="category:N",
                tooltip=["category", "line_total"],
            )
            .properties(height=280)
        )
        st.altair_chart(cat_chart, use_container_width=True)
        st.dataframe(cat_df.rename(columns={"line_total": "Total Spend"}))

# Top cost drivers
with c2:
    st.caption("🔝 Top Cost Drivers (Items by Spend)")
    if df_view.empty:
        st.write("No data.")
    else:
        top_items = df_view.groupby("description")["line_total"].sum().reset_index().sort_values("line_total", ascending=False).head(15)
        bar_chart = (
            alt.Chart(top_items)
            .mark_bar()
            .encode(
                x=alt.X("line_total:Q", title="Total Spend"),
                y=alt.Y("description:N", sort="-x", title="Item"),
                tooltip=["description", "line_total"],
            )
            .properties(height=280)
        )
        st.altair_chart(bar_chart, use_container_width=True)

st.markdown("")

c3, c4 = st.columns(2)

# Price Alerts
with c3:
    st.caption(f"🚨 Price Alerts (>{price_alert_threshold}% change)")
    if df_view.empty:
        st.write("No data.")
    else:
        price_df = (
            df_view.assign(month=lambda d: d["date"].dt.to_period("M").dt.to_timestamp())
            .groupby(["description", "month"])["unit_price"]
            .mean()
            .reset_index()
        )
        price_df.sort_values(["description", "month"], inplace=True)
        price_df["prev_price"] = price_df.groupby("description")["unit_price"].shift(1)
        price_df["pct_change"] = np.where(
            price_df["prev_price"] > 0,
            (price_df["unit_price"] - price_df["prev_price"]) / price_df["prev_price"] * 100,
            np.nan,
        )

        alerts = price_df.loc[price_df["pct_change"].abs() >= price_alert_threshold].copy()
        alerts["direction"] = np.where(alerts["pct_change"] > 0, "Up", "Down")

        if alerts.empty:
            st.success("No significant price changes detected for the selected period.")
        else:
            alerts_display = alerts.sort_values("pct_change", ascending=False)
            alerts_display["pct_change"] = alerts_display["pct_change"].map(lambda x: f"{x:+.1f}%")
            st.dataframe(
                alerts_display[["description", "month", "unit_price", "pct_change", "direction"]].rename(
                    columns={
                        "description": "Item",
                        "month": "Month",
                        "unit_price": "Avg Price",
                        "pct_change": "Change",
                    }
                )
            )

# Savings Opportunities
with c4:
    st.caption("💡 Savings Opportunities (Vendor Price Comparison)")
    if df_view.empty:
        st.write("No data.")
    else:
        grp = (
            df_view.groupby(["description", "vendor_name"])
            .agg(avg_price=("unit_price", "mean"), total_qty=("quantity", "sum"))
            .reset_index()
        )

        savings_rows = []
        for item, sub in grp.groupby("description"):
            sub_sorted = sub.sort_values("avg_price")
            if len(sub_sorted) < 2:
                continue
            best_vendor_row = sub_sorted.iloc[0]
            best_price = best_vendor_row["avg_price"]
            best_vendor = best_vendor_row["vendor_name"]

            for _, row in sub_sorted.iloc[1:].iterrows():
                price_diff = row["avg_price"] - best_price
                if price_diff <= 0:
                    continue
                potential_savings = price_diff * row["total_qty"]
                savings_rows.append(
                    {
                        "Item": item,
                        "Current Vendor": row["vendor_name"],
                        "Current Avg Price": row["avg_price"],
                        "Best Vendor": best_vendor,
                        "Best Avg Price": best_price,
                        "Potential Savings": potential_savings,
                    }
                )

        if not savings_rows:
            st.info("No clear savings opportunities found for the selected filters.")
        else:
            savings_df = pd.DataFrame(savings_rows)
            savings_df.sort_values("Potential Savings", ascending=False, inplace=True)
            st.dataframe(savings_df.head(15))

st.markdown("---")

# Budget vs Actual
st.subheader("📊 Budget vs Actual (Current Month)")
if current_month_spend == 0:
    st.info("No spend in the current month for the selected filters.")
else:
    progress = (current_month_spend / monthly_budget * 100 if monthly_budget > 0 else np.nan)
    st.write(f"**Current Month Spend:** {safe_metric(current_month_spend, '${:,.0f}')}")

    if not np.isnan(progress):
        st.progress(min(1.0, progress / 100))
        st.write(f"**Budget Usage:** {progress:.1f}% of {safe_metric(monthly_budget, '${:,.0f}')}")
        if progress <= 80:
            st.success("You are within budget so far this month.")
        elif progress <= 100:
            st.warning("You are approaching your monthly budget.")
        else:
            st.error("You have exceeded your monthly budget.")

st.markdown("---")

# ---------------------------
# ORIGINAL COMPREHENSIVE GRAPHS
# ---------------------------

if selected_vendor == "All":
    st.header("📈 All Vendors — Expanded Graphs")

    # Row 1
    st.markdown("### Total Spend Trend")
    s = df_view.groupby("month")["line_total"].sum().reset_index()
    if s.empty or s["line_total"].dropna().empty:
        st.warning("Not enough time-series spend data to plot Total Spend Trend.")
    else:
        s["month"] = pd.to_datetime(s["month"])
        chart = (
            alt.Chart(s)
            .mark_line(point=True)
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("line_total:Q", title="Total Spend"),
                tooltip=[alt.Tooltip("month:T", format="%Y-%m"), alt.Tooltip("line_total:Q", format="$,.2f")]
            )
            .properties(height=300, title="Total Spend Trend (All Vendors)")
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown("### Vendor Contribution (Top Vendors)")
    grouped = df_view.groupby("vendor_name")["line_total"].sum().sort_values(ascending=False).reset_index()
    if grouped.empty:
        st.warning("No vendor spend to show vendor contribution.")
    else:
        top = grouped.head(10).copy()
        others = grouped.iloc[10:]["line_total"].sum()
        if others > 0:
            top = pd.concat([top, pd.DataFrame([{"vendor_name": "Other", "line_total": others}])], ignore_index=True)
        
        chart = (
            alt.Chart(top)
            .mark_arc(innerRadius=80)
            .encode(
                theta=alt.Theta("line_total:Q", stack=True),
                color=alt.Color("vendor_name:N", legend=alt.Legend(title="Vendor")),
                tooltip=[
                    alt.Tooltip("vendor_name:N", title="Vendor"),
                    alt.Tooltip("line_total:Q", format="$,.2f", title="Spend"),
                    alt.Tooltip("line_total:Q", format=".1%", aggregate="sum", title="% of Total")
                ]
            )
            .properties(height=350, title="Vendor Contribution Share (Top 10 + Other)")
        )
        st.altair_chart(chart, use_container_width=True)

    # Row 2
    st.markdown("### Top Items by Spend (Global)")
    grouped = df_view.groupby("description")["line_total"].sum().sort_values(ascending=False).head(20).reset_index()
    if grouped.empty:
        st.warning("No item-level spend data available.")
    else:
        chart = (
            alt.Chart(grouped)
            .mark_bar()
            .encode(
                x=alt.X("line_total:Q", title="Total Spend"),
                y=alt.Y("description:N", sort="-x", title="Item Description"),
                color=alt.Color("line_total:Q", scale=alt.Scale(scheme="viridis"), legend=None),
                tooltip=[alt.Tooltip("description:N"), alt.Tooltip("line_total:Q", format="$,.2f")]
            )
            .properties(height=500, title="Top 20 Items by Spend (All Vendors)")
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown("### Category Share Over Time (Stacked Area)")
    cat_time = df_view.groupby(["month","category"])["line_total"].sum().reset_index()
    if cat_time.empty:
        st.warning("Not enough data for category share over time.")
    else:
        # Get top categories
        top_cats = df_view.groupby("category")["line_total"].sum().sort_values(ascending=False).head(8).index
        cat_time["category_display"] = cat_time["category"].apply(lambda x: x if x in top_cats else "Other")
        cat_time_agg = cat_time.groupby(["month", "category_display"])["line_total"].sum().reset_index()
        cat_time_agg["month"] = pd.to_datetime(cat_time_agg["month"])
        
        chart = (
            alt.Chart(cat_time_agg)
            .mark_area(opacity=0.7)
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("line_total:Q", title="Spend", stack=True),
                color=alt.Color("category_display:N", legend=alt.Legend(title="Category")),
                tooltip=[
                    alt.Tooltip("month:T", format="%Y-%m"),
                    alt.Tooltip("category_display:N", title="Category"),
                    alt.Tooltip("line_total:Q", format="$,.2f")
                ]
            )
            .properties(height=400, title="Category Share Over Time (Stacked Area)")
        )
        st.altair_chart(chart, use_container_width=True)

    # Row 3
    st.markdown("### Restaurant Spend Ranking (All Vendors)")
    grouped = df_view.groupby("restaurant_name")["line_total"].sum().sort_values(ascending=False).head(15).reset_index()
    if grouped.empty:
        st.warning("No restaurant spend data.")
    else:
        chart = (
            alt.Chart(grouped)
            .mark_bar()
            .encode(
                x=alt.X("line_total:Q", title="Total Spend"),
                y=alt.Y("restaurant_name:N", sort="-x", title="Restaurant"),
                color=alt.Color("line_total:Q", scale=alt.Scale(scheme="redyellowblue"), legend=None),
                tooltip=[alt.Tooltip("restaurant_name:N"), alt.Tooltip("line_total:Q", format="$,.2f")]
            )
            .properties(height=400, title="Top 15 Restaurants by Spend (All Vendors)")
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown("### Average Unit Price Trend (Price Inflation)")
    if df_view["unit_price"].dropna().empty:
        st.warning("No unit_price data available to show inflation trend.")
    else:
        avg_price = df_view.groupby("month")["unit_price"].mean().reset_index()
        if avg_price.empty:
            st.warning("Not enough unit_price time-series data.")
        else:
            avg_price["month"] = pd.to_datetime(avg_price["month"])
            chart = (
                alt.Chart(avg_price)
                .mark_line(point=True, color="steelblue")
                .encode(
                    x=alt.X("month:T", title="Month"),
                    y=alt.Y("unit_price:Q", title="Avg Unit Price"),
                    tooltip=[alt.Tooltip("month:T", format="%Y-%m"), alt.Tooltip("unit_price:Q", format="$,.2f")]
                )
                .properties(height=300, title="Average Unit Price Over Time (All Vendors)")
            )
            st.altair_chart(chart, use_container_width=True)

    # Row 4
    st.markdown("### Unit Price Distribution (Boxplot)")
    if df_view["unit_price"].dropna().shape[0] < 10:
        st.warning("Too few unit_price points for a meaningful boxplot.")
    else:
        if df_view["category"].nunique() > 1 and df_view["category"].notna().sum() > 10:
            data = df_view[["category", "unit_price"]].dropna()
            top_cats = data["category"].value_counts().head(8).index
            data = data[data["category"].isin(top_cats)]
            chart = (
                alt.Chart(data)
                .mark_boxplot(
                    size=40,
                    extent="min-max",
                    median={"stroke": "white", "strokeWidth": 3},
                    box={"strokeWidth": 2},
                    outliers={"size": 50, "opacity": 0.8}
                )
                .encode(
                    x=alt.X("category:N", title="Category", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("unit_price:Q", title="Unit Price", scale=alt.Scale(zero=False)),
                    color=alt.Color("category:N", scale=alt.Scale(scheme="category20"), legend=None)
                )
                .properties(height=450, title="Unit Price Distribution by Category (Top categories)")
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            data = df_view[["description", "unit_price"]].dropna()
            top_items = data["description"].value_counts().head(8).index
            data = data[data["description"].isin(top_items)]
            chart = (
                alt.Chart(data)
                .mark_boxplot(
                    size=40,
                    extent="min-max",
                    median={"stroke": "white", "strokeWidth": 3},
                    box={"strokeWidth": 2},
                    outliers={"size": 50, "opacity": 0.8}
                )
                .encode(
                    x=alt.X("description:N", title="Item", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("unit_price:Q", title="Unit Price", scale=alt.Scale(zero=False)),
                    color=alt.Color("description:N", scale=alt.Scale(scheme="category20"), legend=None)
                )
                .properties(height=450, title="Unit Price Distribution by Item (Top items)")
            )
            st.altair_chart(chart, use_container_width=True)

    st.markdown("### Invoice Count per Vendor Over Time")
    if "invoice_id" not in df_view.columns:
        st.warning("No invoice_id available for invoice-count chart.")
    else:
        inv_count = df_view.groupby(["month", "vendor_name"])["invoice_id"].nunique().reset_index()
        inv_count.columns = ["month", "vendor_name", "invoice_count"]
        if inv_count.empty:
            st.warning("Not enough invoice-count time-series data.")
        else:
            # Get top 10 vendors by total invoice count
            top_vendors = inv_count.groupby("vendor_name")["invoice_count"].sum().sort_values(ascending=False).head(10).index
            inv_count_top = inv_count[inv_count["vendor_name"].isin(top_vendors)].copy()
            inv_count_top["month"] = pd.to_datetime(inv_count_top["month"])
            
            chart = (
                alt.Chart(inv_count_top)
                .mark_line(point=True)
                .encode(
                    x=alt.X("month:T", title="Month"),
                    y=alt.Y("invoice_count:Q", title="Invoice Count"),
                    color=alt.Color("vendor_name:N", legend=alt.Legend(title="Vendor")),
                    tooltip=[
                        alt.Tooltip("month:T", format="%Y-%m"),
                        alt.Tooltip("vendor_name:N"),
                        alt.Tooltip("invoice_count:Q")
                    ]
                )
                .properties(height=400, title="Invoice Count Over Time (Top 10 Vendors)")
            )
            st.altair_chart(chart, use_container_width=True)

    st.markdown("### Category Trend (Line, per-category)")
    try:
        cat_trend = df_view.groupby(["month","category"])["line_total"].sum().reset_index()
        if not cat_trend.empty:
            cat_trend["month"] = pd.to_datetime(cat_trend["month"])
            chart = (
                alt.Chart(cat_trend)
                .mark_line(point=True)
                .encode(
                    x=alt.X("month:T", title="Month"),
                    y=alt.Y("line_total:Q", title="Spend"),
                    color=alt.Color("category:N", legend=alt.Legend(title="Category")),
                    tooltip=[
                        alt.Tooltip("month:T", format="%Y-%m"),
                        alt.Tooltip("category:N"),
                        alt.Tooltip("line_total:Q", format="$,.2f")
                    ]
                )
                .properties(height=400, title="Category Spend Over Time (All Vendors)")
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.warning("Not enough category trend data.")
    except Exception as e:
        logger.error(f"Failed to render category trend chart: {e}")
        st.error(f"Unable to render category trend: {e}")

else:
    st.header(f"📊 Vendor-specific Graphs — {selected_vendor}")

    st.markdown("### Monthly Spend Trend")
    s = df_view.groupby("month")["line_total"].sum().reset_index()
    if s.empty:
        st.warning("Not enough monthly spend data for vendor.")
    else:
        s["month"] = pd.to_datetime(s["month"])
        chart = (
            alt.Chart(s)
            .mark_line(point=True, color="#1f77b4")
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("line_total:Q", title="Total Spend"),
                tooltip=[alt.Tooltip("month:T", format="%Y-%m"), alt.Tooltip("line_total:Q", format="$,.2f")]
            )
            .properties(height=300, title=f"Monthly Spend Trend ({selected_vendor})")
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown("### Cost Driver — Category (or item fallback)")
    grouped = df_view.groupby("category")["line_total"].sum().sort_values(ascending=False).reset_index()
    if grouped.empty or (grouped["category"] == "Uncategorized").all():
        grouped = df_view.groupby("description")["line_total"].sum().sort_values(ascending=False).head(12).reset_index()
        grouped.columns = ["item", "line_total"]
        title = "Cost Driver — Items (category missing)"
        x_field = "item:N"
    else:
        grouped.columns = ["item", "line_total"]
        title = f"Cost Driver — Category ({selected_vendor})"
        x_field = "item:N"
    if grouped.empty:
        st.warning("No cost-driver data for vendor.")
    else:
        chart = (
            alt.Chart(grouped)
            .mark_bar()
            .encode(
                x=alt.X("line_total:Q", title="Total Spend"),
                y=alt.Y(x_field, sort="-x", title="Category / Item"),
                color=alt.Color("line_total:Q", scale=alt.Scale(scheme="magma"), legend=None),
                tooltip=[alt.Tooltip("item:N"), alt.Tooltip("line_total:Q", format="$,.2f")]
            )
            .properties(height=400, title=title)
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown("### Top Items (Vendor)")
    grouped = df_view.groupby("description")["line_total"].sum().sort_values(ascending=False).head(12).reset_index()
    if grouped.empty:
        st.warning("No item-level data for vendor.")
    else:
        chart = (
            alt.Chart(grouped)
            .mark_bar()
            .encode(
                x=alt.X("line_total:Q", title="Spend"),
                y=alt.Y("description:N", sort="-x", title="Item Description"),
                color=alt.Color("line_total:Q", scale=alt.Scale(scheme="turbo"), legend=None),
                tooltip=[alt.Tooltip("description:N"), alt.Tooltip("line_total:Q", format="$,.2f")]
            )
            .properties(height=400, title=f"Top Items by Spend ({selected_vendor})")
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown("### Category Trend (Vendor)")
    cat_trend = df_view.groupby(["month","category"])["line_total"].sum().reset_index()
    if cat_trend.empty:
        st.warning("Not enough category time data for vendor.")
    else:
        cat_trend["month"] = pd.to_datetime(cat_trend["month"])
        chart = (
            alt.Chart(cat_trend)
            .mark_line(point=True)
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("line_total:Q", title="Spend"),
                color=alt.Color("category:N", legend=alt.Legend(title="Category")),
                tooltip=[
                    alt.Tooltip("month:T", format="%Y-%m"),
                    alt.Tooltip("category:N"),
                    alt.Tooltip("line_total:Q", format="$,.2f")
                ]
            )
            .properties(height=400, title=f"Category Spend Over Time ({selected_vendor})")
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown("### Top Restaurants (Vendor)")
    grouped = df_view.groupby("restaurant_name")["line_total"].sum().sort_values(ascending=False).head(10).reset_index()
    if grouped.empty:
        st.warning("No restaurant-level spend for vendor.")
    else:
        chart = (
            alt.Chart(grouped)
            .mark_bar()
            .encode(
                x=alt.X("line_total:Q", title="Total Spend"),
                y=alt.Y("restaurant_name:N", sort="-x", title="Restaurant"),
                color=alt.Color("line_total:Q", scale=alt.Scale(scheme="redyellowblue"), legend=None),
                tooltip=[alt.Tooltip("restaurant_name:N"), alt.Tooltip("line_total:Q", format="$,.2f")]
            )
            .properties(height=350, title=f"Top Restaurants by Spend ({selected_vendor})")
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown("### Unit Price Distribution (Vendor)")
    if df_view["unit_price"].dropna().shape[0] >= 5:
        if df_view["category"].nunique() > 1:
            top_cats = df_view["category"].value_counts().head(6).index
            data = df_view[df_view["category"].isin(top_cats)][["category", "unit_price"]].dropna()
            chart = (
                alt.Chart(data)
                .mark_boxplot(
                    size=45,
                    extent="min-max",
                    median={"stroke": "white", "strokeWidth": 3},
                    box={"strokeWidth": 2},
                    outliers={"size": 50, "opacity": 0.8}
                )
                .encode(
                    x=alt.X("category:N", title="Category", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("unit_price:Q", title="Unit Price", scale=alt.Scale(zero=False)),
                    color=alt.Color("category:N", scale=alt.Scale(scheme="category20"), legend=None)
                )
                .properties(height=450, title=f"Unit Price Distribution by Category ({selected_vendor})")
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            top_items = df_view["description"].value_counts().head(8).index
            data = df_view[df_view["description"].isin(top_items)][["description", "unit_price"]].dropna()
            chart = (
                alt.Chart(data)
                .mark_boxplot(
                    size=45,
                    extent="min-max",
                    median={"stroke": "white", "strokeWidth": 3},
                    box={"strokeWidth": 2},
                    outliers={"size": 50, "opacity": 0.8}
                )
                .encode(
                    x=alt.X("description:N", title="Item", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("unit_price:Q", title="Unit Price", scale=alt.Scale(zero=False)),
                    color=alt.Color("description:N", scale=alt.Scale(scheme="category20"), legend=None)
                )
                .properties(height=450, title=f"Unit Price Distribution by Item ({selected_vendor})")
            )
            st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("Not enough unit_price points for vendor unit-price distribution.")

    st.markdown("### Invoice Size Distribution (Vendor)")
    invoices = df_view.groupby("invoice_id")["line_total"].sum().dropna().reset_index()
    invoices.columns = ["invoice_id", "invoice_total"]
    if invoices.shape[0] >= 3:
        chart = (
            alt.Chart(invoices)
            .mark_bar()
            .encode(
                x=alt.X("invoice_total:Q", bin=alt.Bin(maxbins=20), title="Invoice Total"),
                y=alt.Y("count()", title="Count"),
                tooltip=[alt.Tooltip("invoice_total:Q", bin=True, title="Invoice Range"), alt.Tooltip("count()", title="Count")]
            )
            .properties(height=350, title=f"Distribution of Spend per Invoice ({selected_vendor})")
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("Not enough invoice-level data for distribution plot.")
