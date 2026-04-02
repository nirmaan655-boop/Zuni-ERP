import streamlit as st
import pandas as pd
from zuni_db import db_connect, fetch_df
from datetime import datetime

# --- 0. AUTO-REPAIR & SYNC ---
def init_proc_repair():
    with db_connect() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS AnimalMaster (
            TagID TEXT PRIMARY KEY, Breed TEXT, Category TEXT, 
            CurrentPen TEXT DEFAULT 'GENERAL', Weight REAL DEFAULT 0, 
            Status TEXT DEFAULT 'Active', PurchasePrice REAL, PurchaseDate TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS ItemMaster 
            (ItemName TEXT PRIMARY KEY, Category TEXT, UOM TEXT, Quantity REAL DEFAULT 0, Cost REAL DEFAULT 0, Nutrition TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS VendorLedger (
            ID INTEGER PRIMARY KEY AUTOINCREMENT, VendorName TEXT, 
            Date TEXT, Description TEXT, Credit REAL, Balance REAL)''')
        conn.commit()

init_proc_repair()

# --- 1. ZUNI BRANDING ---
st.markdown("""
    <div style='background: linear-gradient(90deg, #001F3F 0%, #003366 100%); padding: 25px; border-radius: 15px; border-bottom: 8px solid #FF851B; margin-bottom: 20px;'>
        <h1 style='color: white; margin: 0; font-size: 40px;'>🛒 ZUNI <span style='color: #FF851B;'>PROCUREMENT PRO</span></h1>
        <p style='color: #FF851B; font-size: 18px; font-weight: bold;'>Animal & Store Purchase | Live Total & Sync | FY 2026</p>
    </div>
    """, unsafe_allow_html=True)

# --- 2. FETCH MASTER DATA ---
with db_connect() as conn:
    vendors = fetch_df(conn, "SELECT VendorName FROM VendorMaster")['VendorName'].tolist()
    all_items = fetch_df(conn, "SELECT ItemName, Category, UOM FROM ItemMaster")

t1, t2, t3 = st.tabs(["🐄 PURCHASE ANIMAL", "📦 STORE PURCHASE", "📜 LEDGERS & HISTORY"])

# --- TAB 1: PURCHASE ANIMAL (FULL ORIGINAL FORM) ---
with t1:
    st.subheader("Add New Animal to Farm Inventory")
    with st.form("animal_purchase_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        v_sel = c1.selectbox("Select Vendor / Seller", [""] + vendors)
        a_tag = c2.text_input("Tag Number (Unique ID)").strip().upper()
        a_date = c3.date_input("Purchase Date", datetime.now())
        
        c4, c5, c6 = st.columns(3)
        a_breed = c4.selectbox("Select Breed", ["Sahiwal", "Cholistani", "HF", "Jersey", "Cross", "Australian"])
        a_cat = c5.selectbox("Category", ["Cow", "Bull", "Heifer (Pregnant)", "Heifer (Empty)", "Calf"])
        a_price = c6.number_input("Purchase Price (Rs.)", min_value=0.0)

        if st.form_submit_button("✅ REGISTER & PURCHASE ANIMAL"):
            if v_sel and a_tag:
                try:
                    with db_connect() as conn:
                        conn.execute("INSERT INTO AnimalMaster (TagID, Breed, Category, PurchasePrice, PurchaseDate, Status) VALUES (?,?,?,?,?,?)",
                                     (a_tag, a_breed, a_cat, a_price, a_date.strftime('%Y-%m-%d'), 'Active'))
                        desc = f"Purchased {a_cat} ({a_breed}) Tag: {a_tag}"
                        conn.execute("INSERT INTO VendorLedger (VendorName, Date, Description, Credit, Balance) VALUES (?,?,?,?,?)",
                                     (v_sel, a_date.strftime('%Y-%m-%d'), desc, a_price, a_price))
                        conn.commit()
                    st.success(f"MashaAllah! Animal {a_tag} added to Farm Inventory.")
                    st.rerun()
                except Exception as e: st.error(f"Error: {e}")
            else: st.warning("Vendor aur Tag ID zaroori hain!")

# --- TAB 2: STORE PURCHASE (WITH LIVE TOTAL) ---
with t2:
    st.subheader("📦 Purchase Items for Inventory Stores")
    
    # Selection Area
    col_v, col_d = st.columns(2)
    v_sel_i = col_v.selectbox("Select Vendor", [""] + vendors, key="v_store")
    p_date_i = col_d.date_input("Purchase Date", datetime.now(), key="d_store")
    
    store_sel = st.selectbox("Select Store Category", ["Feed", "Medicine", "Vaccine", "Semen Straws", "General Asset"])
    filtered_df = all_items[all_items['Category'] == store_sel]
    sel_item = st.selectbox(f"Select Item Name (from {store_sel})", [""] + filtered_df['ItemName'].tolist())
    
    uom_list = ["KG", "Bag", "Litre", "ml", "Straw", "Vial", "Dose", "Bottle", "Each"]
    sel_uom = st.selectbox("Confirm Unit (UOM)", uom_list)
    
    # Live Total Calculation Area
    c1, c2 = st.columns(2)
    qty = c1.number_input(f"Quantity Inward", min_value=0.0, step=0.1)
    rate = c2.number_input("Rate per Unit (Rs.)", min_value=0.0, step=1.0)
    
    total_bill = qty * rate
    
    st.markdown(f"""
        <div style='background-color: #001F3F; padding: 20px; border-radius: 12px; border-left: 10px solid #FF851B; margin-top: 15px;'>
            <h2 style='color: white; margin: 0; font-size: 25px;'>💰 Total Bill Amount: <span style='color: #FF851B;'>Rs. {total_bill:,.2f}</span></h2>
        </div>
    """, unsafe_allow_html=True)
    
    nutri = st.text_input("Nutrition Info (e.g. CP 18%)", key="nutri_field")
    
    # Save Action
    if st.button("🚀 COMPLETE STORE PURCHASE"):
        if v_sel_i and sel_item and qty > 0:
            with db_connect() as conn:
                # Formula: Stock = Current Stock + New Quantity
                conn.execute("""UPDATE ItemMaster 
                             SET Quantity = Quantity + ?, Cost = ?, Nutrition = ?, UOM = ? 
                             WHERE ItemName = ?""", (qty, rate, nutri, sel_uom, sel_item))
                
                desc = f"Bought {qty} {sel_uom} {sel_item} for {store_sel}"
                conn.execute("INSERT INTO VendorLedger (VendorName, Date, Description, Credit, Balance) VALUES (?,?,?,?,?)",
                             (v_sel_i, p_date_i.strftime('%Y-%m-%d'), desc, total_bill, total_bill))
                conn.commit()
            st.success(f"Stock Updated! Rs. {total_bill:,.2f} recorded in Vendor Ledger.")
            st.rerun()
        else:
            st.warning("Please fill Vendor, Item and Quantity!")

# --- TAB 3: LEDGERS ---
with t3:
    st.subheader("Recent Ledger Entries")
    with db_connect() as conn:
        df_logs = fetch_df(conn, "SELECT Date, VendorName, Description, Credit as Amount FROM VendorLedger ORDER BY ID DESC")
        if not df_logs.empty:
            st.dataframe(df_logs, use_container_width=True)
            st.divider()
            st.subheader("Outstanding Payables (Vendor Summary)")
            df_pay = fetch_df(conn, "SELECT VendorName, SUM(Credit) as Total_Payable FROM VendorLedger GROUP BY VendorName")
            st.dataframe(df_pay, use_container_width=True)
        else:
            st.info("No purchase history found.")
