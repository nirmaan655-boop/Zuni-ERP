import streamlit as st
import pandas as pd
from zuni_db import db_connect, fetch_df
from datetime import datetime, date

# --- 0. DATABASE INITIALIZATION (AUTO-FIX) ---
def init_sales_db():
    with db_connect() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS Sales (
            SaleID INTEGER PRIMARY KEY AUTOINCREMENT, Date TEXT, CustomerName TEXT, 
            Category TEXT, ItemName TEXT, Qty REAL, UOM TEXT, Rate REAL, Total REAL)""")
        try: conn.execute("ALTER TABLE Sales ADD COLUMN PaymentMode TEXT DEFAULT 'Cash'")
        except: pass
        conn.execute("""CREATE TABLE IF NOT EXISTS CustomerLedger (
            id INTEGER PRIMARY KEY AUTOINCREMENT, CustomerName TEXT, Date TEXT, 
            Description TEXT, Debit REAL, Credit REAL, Balance REAL)""")
        conn.commit()
init_sales_db()

# --- 1. BRANDING ---
st.markdown("""
    <div style='background: linear-gradient(90deg, #001F3F 0%, #003366 100%); padding: 25px; border-radius: 15px; border-bottom: 8px solid #FF851B; margin-bottom: 20px;'>
        <h1 style='color: white; margin: 0; font-size: 40px;'>💰 ZUNI <span style='color: #FF851B;'>SALES & RECEIVABLES</span></h1>
        <p style='color: #FF851B; font-size: 18px; font-weight: bold;'>Smart History, Audit & Live Totals | FY 2026</p>
    </div>
    """, unsafe_allow_html=True)

# --- 2. DATA FETCHING ---
with db_connect() as conn:
    try:
        vendor_list = fetch_df(conn, "SELECT VendorName FROM VendorMaster")['VendorName'].tolist()
        accounts = fetch_df(conn, "SELECT AccountName FROM ChartOfAccounts")['AccountName'].tolist()
        items_df = fetch_df(conn, "SELECT ItemName, Category, UOM FROM ItemMaster")
        tag_list = fetch_df(conn, "SELECT TagID FROM AnimalMaster WHERE Status='Active'")['TagID'].tolist()
    except:
        vendor_list, accounts, items_df, tag_list = [], ["Cash"], pd.DataFrame(), []

# --- 3. TABS ---
t1, t2, t3 = st.tabs(["🛒 NEW SALE ENTRY", "📖 CUSTOMER LEDGER", "📜 SALES HISTORY & AUDIT"])

# --- TAB 1: NEW SALE ENTRY ---
with t1:
    st.subheader("Register New Sale")
    with st.form("sales_entry_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        s_cust = c1.selectbox("Party Name", ["CASH CUSTOMER"] + vendor_list)
        s_cat = c2.selectbox("Category", ["Milk Sale", "Animal Sale", "Feed Sale", "Medicine Sale", "Other"])
        s_mode = c1.selectbox("Payment Mode", ["Cash/Bank Transfer", "On Credit (Ledger)"])
        s_acc = c2.selectbox("Account (If Cash)", accounts)
        
        uom_display = "Unit"
        if s_cat == "Animal Sale":
            s_item = st.selectbox("Animal Tag", tag_list)
            uom_display = "Head"
        elif s_cat == "Milk Sale":
            s_item = "Fresh Milk"
            uom_display = "Litre"
        else:
            s_item = st.selectbox("Store Item", items_df['ItemName'].tolist() if not items_df.empty else ["No Items"])
            if not items_df.empty and s_item in items_df['ItemName'].values:
                uom_display = items_df[items_df['ItemName'] == s_item]['UOM'].iloc[0]

        c3, c4 = st.columns(2)
        s_qty = c3.number_input("Quantity", min_value=0.1, step=1.0)
        s_rate = c4.number_input("Rate per Unit", min_value=0.0)
        
        # LIVE TOTAL SHOWING BEFORE SAVE
        s_total = s_qty * s_rate
        st.markdown(f"""
            <div style='background: #fdf2e9; padding: 15px; border-radius: 10px; border-left: 5px solid #FF851B; margin: 10px 0;'>
                <h3 style='margin:0; color:#001F3F;'>💰 Net Total: Rs. {s_total:,.2f}</h3>
            </div>
        """, unsafe_allow_html=True)

        if st.form_submit_button("🚀 COMPLETE & POST SALE"):
            with db_connect() as conn:
                conn.execute("INSERT INTO Sales (Date, CustomerName, Category, ItemName, Qty, UOM, Rate, Total, PaymentMode) VALUES (?,?,?,?,?,?,?,?,?)",
                             (str(date.today()), s_cust, s_cat, str(s_item), s_qty, uom_display, s_rate, s_total, s_mode))
                if s_mode == "Cash/Bank Transfer":
                    conn.execute("UPDATE ChartOfAccounts SET Balance = Balance + ? WHERE AccountName = ?", (s_total, s_acc))
                else:
                    conn.execute("INSERT INTO CustomerLedger (CustomerName, Date, Description, Debit, Credit, Balance) VALUES (?,?,?,?,?,?)",
                                 (s_cust, str(date.today()), f"Sale: {s_item} ({s_qty} {uom_display})", s_total, 0, s_total))
                if s_cat in ["Feed Sale", "Medicine Sale"]:
                    conn.execute("UPDATE ItemMaster SET Quantity = Quantity - ? WHERE ItemName = ?", (s_qty, s_item))
                if s_cat == "Animal Sale":
                    conn.execute("UPDATE AnimalMaster SET Status='Sold' WHERE TagID=?", (s_item,))
                conn.commit()
            st.success(f"Transaction Success: {s_item} sold to {s_cust}!")
            st.rerun()

# --- TAB 3: HISTORY & DELETE ---
with t3:
    st.subheader("📜 Sales Audit Trail")
    with db_connect() as conn:
        df_sales = fetch_df(conn, "SELECT rowid as ID, Date, CustomerName, ItemName, Qty, Total, PaymentMode FROM Sales ORDER BY rowid DESC LIMIT 50")
        if not df_sales.empty:
            st.dataframe(df_sales, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            st.subheader("🛠️ Reverse/Delete Transaction")
            c_del1, c_del2 = st.columns([1, 2])
            id_to_del = c_del1.number_input("Enter ID to Delete", step=1, min_value=0)
            if c_del2.button("🗑️ PERMANENT DELETE SALE"):
                with db_connect() as conn:
                    # Note: Yahan hum balance reversal bhi add kar sakte hain future mein
                    conn.execute("DELETE FROM Sales WHERE rowid = ?", (id_to_del,))
                    conn.commit()
                st.error(f"Sale ID {id_to_del} has been deleted.")
                st.rerun()
        else:
            st.info("No sales recorded yet.")

# --- TAB 2: LEDGER ---
with t2:
    target_cust = st.selectbox("View Party Ledger", [""] + vendor_list)
    if target_cust:
        with db_connect() as conn:
            led_df = fetch_df(conn, "SELECT Date, Description, Debit as Sale, Credit as Payment FROM CustomerLedger WHERE CustomerName = ?", (target_cust,))
            if not led_df.empty:
                led_df['Balance'] = (led_df['Sale'] - led_df['Payment']).cumsum()
                st.dataframe(led_df, use_container_width=True, hide_index=True)
                st.metric("Total Outstanding", f"Rs. {led_df['Balance'].iloc[-1]:,.0f}")
