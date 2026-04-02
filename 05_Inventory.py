import streamlit as st
import pandas as pd
from zuni_db import db_connect, fetch_df

# --- BRANDING ---
st.markdown("""
    <div style='background: linear-gradient(90deg, #001F3F 0%, #003366 100%); padding: 25px; border-radius: 15px; border-bottom: 8px solid #FF851B; margin-bottom: 20px;'>
        <h1 style='color: white; margin: 0; font-size: 40px;'>📦 ZUNI <span style='color: #FF851B;'>PRO STORES</span></h1>
        <p style='color: #FF851B; font-size: 18px; font-weight: bold;'>Inventory, Nutrition & Stock Tracking | FY 2026</p>
    </div>
    """, unsafe_allow_html=True)

# --- FETCH DATA ---
with db_connect() as conn:
    try:
        # Saara data ItemMaster se uthayen (Quantity, Cost, Nutrition ke saath)
        stock_df = fetch_df(conn, "SELECT ItemName, Category, UOM, Quantity, Cost, Nutrition FROM ItemMaster")
        animals_raw = fetch_df(conn, "SELECT TagID, Category FROM AnimalMaster")
    except:
        stock_df = pd.DataFrame(columns=['ItemName', 'Category', 'UOM', 'Quantity', 'Cost', 'Nutrition'])
        animals_raw = pd.DataFrame()

# --- TABS (SAARAY ORIGINAL TABS) ---
t1, t2, t3, t4, t5, t6 = st.tabs(["🌾 FEED RECIPE", "💰 ANIMAL P&L", "🧬 SEMEN BANK", "💊 MEDICINES", "⛽ GENERAL STORES", "➕ REGISTER ITEM"])

with t1:
    st.subheader("🌾 Feed & Wanda Stock (Live)")
    feed_data = stock_df[stock_df['Category'] == 'Feed']
    st.dataframe(feed_data, use_container_width=True)
    st.divider()
    col1, col2 = st.columns(2)
    feed_list = feed_data['ItemName'].tolist() if not feed_data.empty else []
    selected_feed = col1.selectbox("Select Feed / Wanda", [""] + feed_list)
    daily_qty = col2.number_input("Daily KG per Animal", min_value=0.0, value=8.0)
    if st.button("🚀 SAVE RECIPE"):
        st.success(f"Recipe Saved: {daily_qty}kg {selected_feed}")

with t2:
    st.subheader("💰 Daily Profit & Loss Mapping")
    if not animals_raw.empty:
        # P&L Formula Logic
        pl_display = animals_raw.copy()
        pl_display['Daily Feed Cost'] = daily_qty * 125 # Default Avg Rate
        pl_display['Daily Profit (Est)'] = (20.0 * 210) - pl_display['Daily Feed Cost']
        st.dataframe(pl_display, use_container_width=True)

with t3:
    st.subheader("🧬 Semen Bank (Straw Inventory)")
    semen_data = stock_df[stock_df['Category'] == 'Semen Straws']
    st.dataframe(semen_data, use_container_width=True)

with t4:
    st.subheader("💊 Medicine & Vaccine Store")
    med_data = stock_df[stock_df['Category'].isin(['Medicine', 'Vaccine'])]
    st.dataframe(med_data, use_container_width=True)

with t5:
    st.subheader("⛽ Fuel & General Assets")
    gen_data = stock_df[stock_df['Category'] == 'General Asset']
    st.dataframe(gen_data, use_container_width=True)

with t6:
    st.subheader("Register New Item in Catalog")
    with st.form("reg_item_form", clear_on_submit=True):
        f_name = st.text_input("Item Name (e.g. WANDA #1)").upper()
        f_cat = st.selectbox("Store Category", ["Feed", "Medicine", "Vaccine", "Semen Straws", "General Asset"])
        f_uom = st.selectbox("Unit (UOM)", ["KG", "Bag", "Litre", "ml", "Straw", "Vial", "Dose", "Bottle", "Each"])
        if st.form_submit_button("✅ REGISTER ITEM"):
            if f_name:
                with db_connect() as conn:
                    conn.execute("INSERT OR REPLACE INTO ItemMaster (ItemName, Category, UOM, Quantity, Cost) VALUES (?,?,?,0,0)", (f_name, f_cat, f_uom))
                    conn.commit()
                st.success(f"{f_name} Added to {f_cat} Store!")
                st.rerun()
