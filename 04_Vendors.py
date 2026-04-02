import streamlit as st
import pandas as pd
from zuni_db import db_connect, fetch_df

# --- 0. AUTO-INITIALIZE TABLES (Safeguard) ---
def init_master_db():
    with db_connect() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS VendorMaster (
            VendorName TEXT PRIMARY KEY, ContactPerson TEXT, Phone TEXT, Address TEXT)""")
        # Employee Table with LeaveAllowed column
        conn.execute("""CREATE TABLE IF NOT EXISTS EmployeeMaster (
            Name TEXT, CNIC TEXT PRIMARY KEY, Phone TEXT, Designation TEXT, Salary REAL, LeaveAllowed INTEGER DEFAULT 2)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS ChartOfAccounts (
            AccountName TEXT PRIMARY KEY, AccountType TEXT, Balance REAL DEFAULT 0)""")
        
        # Column Fix: Agar purana table hai toh LeaveAllowed column add karna
        try: conn.execute("ALTER TABLE EmployeeMaster ADD COLUMN LeaveAllowed INTEGER DEFAULT 2")
        except: pass
        conn.commit()

init_master_db()

# --- 1. SESSION STATE FOR EDITING ---
if "edit_v" not in st.session_state: st.session_state.edit_v = None
if "edit_e" not in st.session_state: st.session_state.edit_e = None
if "edit_a" not in st.session_state: st.session_state.edit_a = None

# --- 2. BRANDING & CSS ---
st.markdown("""
    <div style='background: linear-gradient(90deg, #001F3F 0%, #003366 100%); padding: 25px; border-radius: 15px; border-bottom: 8px solid #FF851B; margin-bottom: 20px;'>
        <h1 style='color: white; margin: 0; font-size: 50px;'>👥 ZUNI <span style='color: #FF851B;'>MASTER SETUP</span></h1>
        <p style='color: #FF851B; font-size: 20px; font-weight: bold;'>Vendors, Employees & Financial Accounts | FY 2026</p>
    </div>
    <style>
    .stTabs [data-baseweb="tab"] { background-color: #001F3F; border-radius: 10px; color: white !important; padding: 10px 20px; margin-right: 5px; }
    .stTabs [aria-selected="true"] { background-color: #FF851B !important; font-weight: bold; }
    .stButton>button { width: 100%; border-radius: 10px; background-color: #001F3F; color: white !important; border: 2px solid #FF851B; font-weight: bold; height: 3em; }
    </style>
    """, unsafe_allow_html=True)

tab_v, tab_e, tab_a = st.tabs(["🤝 VENDORS", "👷 EMPLOYEES (PAYROLL)", "💰 ACCOUNTS"])

# --- TAB 1: VENDORS ---
with tab_v:
    st.subheader("🛠️ Manage Vendors")
    with st.form("v_form", clear_on_submit=True):
        ev = st.session_state.edit_v
        v1, v2 = st.columns(2)
        v_name = v1.text_input("Vendor Name", value=ev['VendorName'] if ev else "").strip().upper()
        v_person = v2.text_input("Contact Person", value=ev['ContactPerson'] if ev else "")
        v_phone = v1.text_input("Phone", value=ev['Phone'] if ev else "")
        v_address = v2.text_input("Address", value=ev['Address'] if ev else "")
        if st.form_submit_button("✅ SAVE VENDOR"):
            with db_connect() as conn:
                conn.execute("INSERT OR REPLACE INTO VendorMaster VALUES (?,?,?,?)", (v_name, v_person, v_phone, v_address))
                conn.commit()
            st.session_state.edit_v = None
            st.rerun()

    with db_connect() as conn:
        df_v = fetch_df(conn, "SELECT * FROM VendorMaster")
    if not df_v.empty:
        st.dataframe(df_v, use_container_width=True, hide_index=True)
        c1, c2, c3 = st.columns(3)
        sel_v = c1.selectbox("Select Vendor", df_v['VendorName'].tolist())
        if c2.button("📝 EDIT"): 
            st.session_state.edit_v = df_v[df_v['VendorName'] == sel_v].iloc[0]
            st.rerun()
        if c3.button("🗑️ DELETE"):
            with db_connect() as conn:
                conn.execute("DELETE FROM VendorMaster WHERE VendorName = ?", (sel_v,))
                conn.commit()
            st.rerun()

# --- TAB 2: EMPLOYEES (FIXED WITH LEAVE OPTION) ---
with tab_e:
    st.subheader("👷 Employee Directory & Leave Policy")
    with st.form("e_form", clear_on_submit=True):
        ee = st.session_state.edit_e
        e1, e2, e3 = st.columns(3)
        en = e1.text_input("Full Name", value=ee['Name'] if ee else "").upper()
        ec = e2.text_input("CNIC", value=ee['CNIC'] if ee else "")
        ep = e3.text_input("Phone", value=ee['Phone'] if ee else "")
        ds = e1.selectbox("Designation", ["Manager", "Supervisor", "Doctor", "Labor", "Guard"])
        sl = e2.number_input("Monthly Salary", value=float(ee['Salary']) if ee else 0.0)
        lv = e3.number_input("Leaves Allowed (Per Month)", min_value=0, max_value=30, value=int(ee['LeaveAllowed']) if ee else 2)
        
        if st.form_submit_button("📝 SAVE EMPLOYEE"):
            if en and ec:
                with db_connect() as conn:
                    conn.execute("INSERT OR REPLACE INTO EmployeeMaster (Name, CNIC, Phone, Designation, Salary, LeaveAllowed) VALUES (?,?,?,?,?,?)", (en, ec, ep, ds, sl, lv))
                    conn.commit()
                st.session_state.edit_e = None
                st.rerun()

    with db_connect() as conn:
        df_e = fetch_df(conn, "SELECT * FROM EmployeeMaster")
    if not df_e.empty:
        st.dataframe(df_e, use_container_width=True, hide_index=True)
        ce1, ce2, ce3 = st.columns(3)
        sel_e = ce1.selectbox("Select Employee", df_e['Name'].tolist())
        if ce2.button("📝 EDIT EMP"):
            st.session_state.edit_e = df_e[df_e['Name'] == sel_e].iloc[0]
            st.rerun()
        if ce3.button("🗑️ REMOVE"):
            with db_connect() as conn:
                conn.execute("DELETE FROM EmployeeMaster WHERE Name = ?", (sel_e,))
                conn.commit()
            st.rerun()

# --- TAB 3: ACCOUNTS ---
with tab_a:
    st.subheader("🏦 Chart of Accounts")
    with st.form("a_form", clear_on_submit=True):
        ea = st.session_state.edit_a
        a1, a2, a3 = st.columns(3)
        an = a1.text_input("Account Name", value=ea['AccountName'] if ea else "").upper()
        at = a2.selectbox("Type", ["Cash In Hand", "Bank Account", "Expense", "Fixed Asset"])
        ab = a3.number_input("Opening Balance", value=float(ea['Balance']) if ea else 0.0)
        if st.form_submit_button("🏦 SAVE ACCOUNT"):
            with db_connect() as conn:
                conn.execute("INSERT OR REPLACE INTO ChartOfAccounts VALUES (?,?,?)", (an, at, ab))
                conn.commit()
            st.session_state.edit_a = None
            st.rerun()

    with db_connect() as conn:
        df_a = fetch_df(conn, "SELECT * FROM ChartOfAccounts")
    if not df_a.empty:
        st.dataframe(df_a, use_container_width=True, hide_index=True)
        ca1, ca2, ca3 = st.columns(3)
        sel_a = ca1.selectbox("Select Account", df_a['AccountName'].tolist())
        if ca2.button("📝 EDIT ACC"):
            st.session_state.edit_a = df_a[df_a['AccountName'] == sel_a].iloc[0]
            st.rerun()
        if ca3.button("🗑️ DELETE ACCOUNT"):
            with db_connect() as conn:
                conn.execute("DELETE FROM ChartOfAccounts WHERE AccountName = ?", (sel_a,))
                conn.commit()
            st.rerun()
