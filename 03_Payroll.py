import streamlit as st
import pandas as pd
from zuni_db import db_connect, fetch_df
from datetime import date, datetime

# --- 0. DATABASE INITIALIZATION (Payroll Tables) ---
def init_payroll_db():
    with db_connect() as conn:
        # Attendance & Leave Table
        conn.execute("""CREATE TABLE IF NOT EXISTS StaffLeaves (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            Name TEXT, LeaveDate TEXT, Reason TEXT, Type TEXT)""")
        # Salary History
        conn.execute("""CREATE TABLE IF NOT EXISTS SalaryHistory (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            Name TEXT, Month TEXT, Basic REAL, Bonus REAL, Deduction REAL, NetPaid REAL)""")
        conn.commit()
init_payroll_db()

# --- 1. ZUNI BRANDING ---
st.markdown("""
    <div style='background: linear-gradient(90deg, #001F3F 0%, #003366 100%); padding: 25px; border-radius: 15px; border-bottom: 8px solid #FF851B; margin-bottom: 20px;'>
        <h1 style='color: white; margin: 0; font-size: 35px;'>💼 ZUNI <span style='color: #FF851B;'>PAYROLL PRO</span></h1>
        <p style='color: #FF851B; font-size: 16px; font-weight: bold; margin: 0;'>Automated Leaves & Salary Intelligence | FY 2026</p>
    </div>
    """, unsafe_allow_html=True)

# --- 2. FETCH STAFF DATA ---
with db_connect() as conn:
    try:
        staff_df = fetch_df(conn, "SELECT Name, Designation, Salary, LeaveAllowed FROM EmployeeMaster")
        staff_list = staff_df['Name'].tolist() if not staff_df.empty else []
    except:
        staff_df, staff_list = pd.DataFrame(), []

# --- 3. CUSTOM CSS ---
st.markdown("""
    <style>
    .stTabs [data-baseweb="tab"] { background-color: #001F3F; border-radius: 8px; color: white !important; padding: 10px 20px; margin-right: 5px; }
    .stTabs [aria-selected="true"] { background-color: #FF851B !important; font-weight: bold; }
    .stButton>button { border-radius: 12px; background-color: #FF851B !important; color: white !important; font-weight: bold; height: 3em; }
    </style>
    """, unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["💰 GENERATE SALARY", "📅 LEAVE MANAGEMENT", "📜 PAYROLL HISTORY"])

# --- TAB 1: GENERATE SALARY (WITH AUTO-LEAVE CALCULATION) ---
with tab1:
    st.subheader("Monthly Salary Calculation")
    if staff_list:
        with st.form("salary_form"):
            col1, col2 = st.columns(2)
            s_emp = col1.selectbox("Select Staff", staff_list)
            s_month = col2.selectbox("Select Month", ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
            
            # Fetch Employee Details
            emp_data = staff_df[staff_df['Name'] == s_emp].iloc[0]
            basic = float(emp_data['Salary'])
            allowed = int(emp_data['LeaveAllowed'])

            # 🛑 AUTO-CALCULATE LEAVES FROM DATABASE
            with db_connect() as conn:
                leave_count = fetch_df(conn, "SELECT COUNT(*) as total FROM StaffLeaves WHERE Name = ?", (s_emp,)).iloc[0]['total']
            
            st.info(f"📊 **Status:** Allowed: {allowed} | Taken: {leave_count}")
            
            # Deduction Logic (If leaves > allowed)
            extra_leaves = max(0, leave_count - allowed)
            per_day_sal = basic / 30
            auto_deduction = extra_leaves * per_day_sal

            s_bonus = col1.number_input("Bonus/Incentive", min_value=0.0)
            s_fine = col2.number_input("Other Deductions (Fine)", min_value=0.0, value=float(auto_deduction))
            
            net = basic + s_bonus - s_fine
            st.markdown(f"<h2 style='color: #FF851B;'>Net Payable: Rs. {net:,.0f}</h2>", unsafe_allow_html=True)
            
            if st.form_submit_button("🚀 POST SALARY TO LEDGER"):
                with db_connect() as conn:
                    conn.execute("INSERT INTO SalaryHistory (Name, Month, Basic, Bonus, Deduction, NetPaid) VALUES (?,?,?,?,?,?)", 
                                 (s_emp, s_month, basic, s_bonus, s_fine, net))
                    conn.commit()
                st.success(f"Salary for {s_emp} posted successfully!")
                st.rerun()
    else:
        st.error("⚠️ Master Setup mein Staff add karein.")

# --- TAB 2: LEAVE MANAGEMENT (FORM & HISTORY) ---
with tab2:
    st.subheader("📝 Leave Application Form")
    if staff_list:
        with st.form("leave_form", clear_on_submit=True):
            l_staff = st.selectbox("Staff Name", staff_list)
            l_date = st.date_input("Date of Leave", date.today())
            l_type = st.radio("Leave Type", ["Full Day", "Short Leave", "Emergency"], horizontal=True)
            l_reason = st.text_area("Reason for Leave")
            
            if st.form_submit_button("✅ APPROVE LEAVE"):
                with db_connect() as conn:
                    conn.execute("INSERT INTO StaffLeaves (Name, LeaveDate, Reason, Type) VALUES (?,?,?,?)", 
                                 (l_staff, str(l_date), l_reason, l_type))
                    conn.commit()
                st.success(f"Leave approved for {l_staff} on {l_date}")
        
        st.divider()
        st.subheader("📜 Recent Leave Records")
        with db_connect() as conn:
            history_df = fetch_df(conn, "SELECT rowid as ID, Name, LeaveDate, Type, Reason FROM StaffLeaves ORDER BY rowid DESC")
            st.dataframe(history_df, use_container_width=True, hide_index=True)
            
            # Delete Leave logic
            d1, d2 = st.columns([3,1])
            del_id = d1.number_input("Enter ID to Cancel Leave", step=1)
            if d2.button("🗑️ Cancel"):
                conn.execute("DELETE FROM StaffLeaves WHERE rowid = ?", (del_id,))
                conn.commit()
                st.rerun()

# --- TAB 3: PAYROLL HISTORY ---
with tab3:
    st.subheader("Past Salary Payments")
    with db_connect() as conn:
        sal_df = fetch_df(conn, "SELECT * FROM SalaryHistory ORDER BY id DESC")
        st.dataframe(sal_df, use_container_width=True, hide_index=True)
