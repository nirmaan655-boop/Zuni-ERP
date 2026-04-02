import streamlit as st
import pandas as pd
from zuni_db import db_connect, fetch_df
from io import BytesIO

# --- 1. BRANDING HEADER ---
st.markdown("""
    <div style='background: linear-gradient(90deg, #001F3F 0%, #003366 100%); padding: 25px; border-radius: 15px; border-bottom: 10px solid #FF851B; margin-bottom: 25px; text-align: center;'>
        <h1 style='color: white; margin: 0; font-size: 55px; font-weight: 900;'>ZUNI <span style='color: #FF851B;'>ERP</span></h1>
        <p style='color: #FF851B; font-size: 20px; font-weight: bold; margin: 0;'>Dairy Farm Control Center | FY 2026</p>
    </div>
    """, unsafe_allow_html=True)

# --- 2. FETCH LIVE DATA (FIXED MAPPING) ---
with db_connect() as conn:
    try:
        # Livestock Stats
        total_animals = fetch_df(conn, "SELECT COUNT(*) as c FROM AnimalMaster")['c'].iloc[0]
        cows = fetch_df(conn, "SELECT COUNT(*) as c FROM AnimalMaster WHERE Category='Cow'")['c'].iloc[0]
        calves = fetch_df(conn, "SELECT COUNT(*) as c FROM AnimalMaster WHERE Category='Calf'")['c'].iloc[0]
        vendors = fetch_df(conn, "SELECT COUNT(*) as c FROM VendorMaster")['c'].iloc[0]
        
        # FINANCIAL STATS (FIXED FOR CASH/BANK)
        f_df = fetch_df(conn, "SELECT AccountName, Balance FROM ChartOfAccounts")
        # Exact match for Cash and Bank
        cash_bal = f_df[f_df['AccountName'].str.contains('Cash', case=False, na=False)]['Balance'].sum()
        bank_bal = f_df[f_df['AccountName'].str.contains('Bank', case=False, na=False)]['Balance'].sum()
        
        # SALES & EXPENSES (From Transactions Table)
        trans_stats = fetch_df(conn, "SELECT SUM(Credit) as sales, SUM(Debit) as exp FROM Transactions")
        total_sales = trans_stats['sales'].iloc[0] if trans_stats['sales'].iloc[0] else 0
        total_expenses = trans_stats['exp'].iloc[0] if trans_stats['exp'].iloc[0] else 0
        
    except Exception as e:
        st.error(f"Data Error: {e}")
        total_animals = cows = calves = vendors = cash_bal = bank_bal = total_sales = total_expenses = 0

# --- 3. CUSTOM CSS ---
st.markdown("""
    <style>
    .stButton>button { 
        width: 100% !important; border-radius: 15px; height: 6em; 
        background-color: #001F3F !important; color: white !important; 
        font-weight: 900 !important; border: 3px solid #FF851B !important;
        font-size: 18px !important; box-shadow: 0px 5px 15px rgba(0,0,0,0.3);
    }
    .stButton>button:hover { background-color: #FF851B !important; color: #001F3F !important; transform: scale(1.03); }
    </style>
    """, unsafe_allow_html=True)

# --- 4. INTERACTIVE BOXES (DASHBOARD) ---
if 'active_report' not in st.session_state: st.session_state.active_report = "None"

st.subheader("🐄 LIVESTOCK SUMMARY")
col1, col2, col3, col4 = st.columns(4)
if col1.button(f"TOTAL ANIMALS\n{total_animals}"): st.session_state.active_report = "All Animals"
if col2.button(f"MILKING COWS\n{cows}"): st.session_state.active_report = "Cows"
if col3.button(f"CALVES\n{calves}"): st.session_state.active_report = "Calves"
if col4.button(f"VENDORS\n{vendors}"): st.session_state.active_report = "Vendors"

st.divider()
st.subheader("💰 FINANCIAL STATUS")
fcol1, fcol2, fcol3, fcol4 = st.columns(4)
if fcol1.button(f"CASH IN HAND\nRs. {cash_bal:,.0f}"): st.session_state.active_report = "Cash"
if fcol2.button(f"BANK BALANCE\nRs. {bank_bal:,.0f}"): st.session_state.active_report = "Bank"
if fcol3.button(f"TOTAL SALES\nRs. {total_sales:,.0f}"): st.session_state.active_report = "Sales"
if fcol4.button(f"TOTAL EXPENSES\nRs. {total_expenses:,.0f}"): st.session_state.active_report = "Expenses"

# --- 5. REPORT VIEW & EXCEL DOWNLOAD ---
st.divider()
if st.session_state.active_report != "None":
    st.subheader(f"📊 {st.session_state.active_report} Details")
    
    # Updated Query Mapping
    query_map = {
        "All Animals": "SELECT * FROM AnimalMaster",
        "Cows": "SELECT * FROM AnimalMaster WHERE Category='Cow'",
        "Calves": "SELECT * FROM AnimalMaster WHERE Category='Calf'",
        "Vendors": "SELECT * FROM VendorMaster",
        "Cash": "SELECT Date, PayeeName, Description, Debit, Credit FROM Transactions WHERE AccountName LIKE '%Cash%'",
        "Bank": "SELECT Date, PayeeName, Description, Debit, Credit FROM Transactions WHERE AccountName LIKE '%Bank%'",
        "Sales": "SELECT Date, PayeeName, Description, Credit as Amount FROM Transactions WHERE Credit > 0",
        "Expenses": "SELECT Date, PayeeName, Description, Debit as Amount FROM Transactions WHERE Debit > 0"
    }
    
    current_query = query_map.get(st.session_state.active_report)
    
    if current_query:
        with db_connect() as conn:
            report_df = fetch_df(conn, current_query)
        
        if not report_df.empty:
            # Show Table
            st.dataframe(report_df, use_container_width=True)
            
            # Excel Download Section
            towrite = BytesIO()
            report_df.to_excel(towrite, index=False, engine='xlsxwriter')
            towrite.seek(0)
            st.download_button(
                label=f"📥 DOWNLOAD {st.session_state.active_report.upper()} EXCEL",
                data=towrite,
                file_name=f"Zuni_{st.session_state.active_report.replace(' ', '_')}.xlsx",
                mime="application/vnd.ms-excel"
            )
        else:
            st.warning("No data found for this category.")
else:
    st.info("💡 Tip: Click on any box above to see details and download Excel reports.")
