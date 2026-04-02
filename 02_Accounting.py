import streamlit as st
import pandas as pd
from zuni_db import db_connect, fetch_df
from datetime import date

# --- 1. PAGE CONFIG & BRANDING ---
st.set_page_config(page_title="Zuni ERP | Financials", layout="wide")

st.markdown("""
    <style>
    .main-header { background-color: #d81b60; padding: 15px; border-radius: 5px; margin-bottom: 20px; color: white; display: flex; justify-content: space-between; align-items: center; }
    .stTabs [data-baseweb="tab"] { font-weight: bold; }
    </style>
    <div class="main-header">
        <h2 style='margin: 0;'>💰 ZUNI FINANCIAL CONTROL CENTER</h2>
        <div style='background: white; color: #d81b60; padding: 5px 15px; border-radius: 5px; font-weight: bold;'>FY 2026</div>
    </div>
    """, unsafe_allow_html=True)

# --- 2. SESSION STATE FOR FORMS ---
if 'pmt_rows' not in st.session_state:
    st.session_state.pmt_rows = [{"Account": "", "Amount": 0.0, "Narration": ""}]
if 'jv_rows' not in st.session_state:
    st.session_state.jv_rows = [{"Account": "", "Dr": 0.0, "Cr": 0.0, "Nar": ""}]

# --- 3. FETCH DATA ---
with db_connect() as conn:
    acc_df = fetch_df(conn, "SELECT AccountName, AccountType, Balance FROM ChartOfAccounts")
    try: vendors = fetch_df(conn, "SELECT VendorName as Name FROM VendorMaster")['Name'].tolist()
    except: vendors = []
    try: employees = fetch_df(conn, "SELECT Name FROM EmployeeMaster")['Name'].tolist()
    except: employees = []
    all_heads = sorted(list(set(acc_df['AccountName'].tolist() + vendors + employees + ["Milk Sale", "Feed Expense", "General Expense"])))

# --- 4. TABS SYSTEM ---
tab1, tab2, tab3, tab4 = st.tabs(["📝 VOUCHER ENTRY", "📖 PARTY LEDGER", "📊 FINANCIAL REPORTS", "📜 HISTORY & EDIT"])

# --- TAB 1: VOUCHER ENTRY (PMT & JV COMBINED) ---
with tab1:
    v_cat = st.radio("Select Voucher Type", ["💳 Payment / Receipt", "🔄 Journal Voucher (JV)"], horizontal=True)

    if v_cat == "💳 Payment / Receipt":
        v_mode = st.radio("Action", ["💸 Payment (Outward)", "💰 Receipt (Inward)"], horizontal=True)
        # --- HEADER SECTION (IMAGE DESIGN) ---
        with st.container(border=True):
            c1, c2, c3 = st.columns(3)
            inv_no = c1.text_input("Invoice No", placeholder="Enter Invoice No")
            cheq_no = c2.text_input("Cheq #", placeholder="Cheque #(If Any)")
            pay_methods = acc_df[acc_df['AccountName'].str.contains('Cash|Bank', case=False)]
            pay_options = [f"{r['AccountName']} | Bal: {r['Balance']:,.0f}" for _, r in pay_methods.iterrows()]
            sel_method_str = c3.selectbox("Payment From / Receive In", pay_options)
            method_name = sel_method_str.split(" | ")[0]
            t_date = st.date_input("Transaction Date", date.today(), key="p_dt")

        # --- GRID SECTION ---
        st.markdown("#### 🛒 Select Accounts")
        h1, h2, h3, h4, h5 = st.columns([3,2,2,2,3])
        h1.caption("Account Name"); h2.caption("Cur Balance"); h3.caption("Type"); h4.caption("Amount"); h5.caption("Narration")

        updated_pmt = []
        for i, row in enumerate(st.session_state.pmt_rows):
            r1, r2, r3, r4, r5 = st.columns([3,2,2,2,3])
            acc = r1.selectbox(f"p_acc_{i}", [""] + all_heads, key=f"p_acc_{i}", label_visibility="collapsed")
            info = acc_df[acc_df['AccountName'] == acc]
            r2.write(f"**{info['Balance'].iloc if not info.empty else 0:,.0f}**")
            r3.write(f"{info['AccountType'].iloc if not info.empty else '-'}")
            amt = r4.number_input(f"p_amt_{i}", value=row['Amount'], key=f"p_amt_{i}", label_visibility="collapsed")
            nar = r5.text_input(f"p_nar_{i}", value=row['Narration'], key=f"p_nar_{i}", label_visibility="collapsed")
            updated_pmt.append({"Account": acc, "Amount": amt, "Narration": nar})

        if st.button("➕ Add Account"):
            st.session_state.pmt_rows = updated_pmt + [{"Account": "", "Amount": 0.0, "Narration": ""}]
            st.rerun()

        total_p = sum(x['Amount'] for x in updated_pmt)
        if st.button("✅ Save Payment/Receipt", type="primary", use_container_width=True):
            if total_p > 0:
                with db_connect() as conn:
                    for r in updated_pmt:
                        if r['Account'] != "" and r['Amount'] > 0:
                            dr, cr = (r['Amount'], 0) if "Payment" in v_mode else (0, r['Amount'])
                            conn.execute("INSERT INTO Transactions (Date, AccountName, PayeeName, Description, Debit, Credit) VALUES (?,?,?,?,?,?)",
                                         (str(t_date), method_name, r['Account'], r['Narration'], dr, cr))
                            conn.execute("UPDATE ChartOfAccounts SET Balance = Balance + ? WHERE AccountName = ?", (cr - dr, method_name))
                    conn.commit()
                st.success("Voucher Saved!")
                st.session_state.pmt_rows = [{"Account": "", "Amount": 0.0, "Narration": ""}]
                st.rerun()

    else: # --- JOURNAL VOUCHER (JV) DESIGN ---
        st.markdown("### 🔄 Balanced Journal Voucher")
        with st.container(border=True):
            jc1, jc2 = st.columns(2)
            jv_date = jc1.date_input("JV Date", date.today(), key="jv_dt")
            jv_nar = jc2.text_input("Main Narration", placeholder="e.g. Adjustment Entry")

        st.markdown("---")
        h1, h2, h3, h4, h5, h6 = st.columns([3,1,1,2,2,3])
        h1.caption("Account"); h2.caption("Bal"); h3.caption("Type"); h4.caption("Debit"); h5.caption("Credit"); h6.caption("Narration")

        updated_jv = []
        for i, row in enumerate(st.session_state.jv_rows):
            r1, r2, r3, r4, r5, r6 = st.columns([3,1,1,2,2,3])
            acc = r1.selectbox(f"jv_acc_{i}", [""] + all_heads, key=f"jv_acc_{i}", label_visibility="collapsed")
            info = acc_df[acc_df['AccountName'] == acc]
            r2.write(f"{info['Balance'].iloc if not info.empty else 0:,.0f}")
            r3.write(f"{info['AccountType'].iloc if not info.empty else '-'}")
            dr = r4.number_input(f"jv_dr_{i}", value=row['Dr'], key=f"jv_dr_{i}", label_visibility="collapsed")
            cr = r5.number_input(f"jv_cr_{i}", value=row['Cr'], key=f"jv_cr_{i}", label_visibility="collapsed")
            nar = r6.text_input(f"jv_nar_{i}", value=row['Nar'], key=f"jv_nar_{i}", label_visibility="collapsed")
            updated_jv.append({"Account": acc, "Dr": dr, "Cr": cr, "Nar": nar})

        if st.button("➕ Add JV Row"):
            st.session_state.jv_rows = updated_jv + [{"Account": "", "Dr": 0.0, "Cr": 0.0, "Nar": ""}]
            st.rerun()

        tdr, tcr = sum(x['Dr'] for x in updated_jv), sum(x['Cr'] for x in updated_jv)
        diff = tdr - tcr
        st.markdown(f"**Dr: {tdr:,.0f} | Cr: {tcr:,.0f} | Diff: {diff:,.0f}**")

        if st.button("💾 Save JV Changes", type="primary", use_container_width=True):
            if tdr == tcr and tdr > 0:
                with db_connect() as conn:
                    for r in updated_jv:
                        if r['Account'] != "" and (r['Dr'] > 0 or r['Cr'] > 0):
                            conn.execute("INSERT INTO Transactions (Date, AccountName, Description, Debit, Credit) VALUES (?,?,?,?,?)",
                                         (str(jv_date), r['Account'], r['Nar'] or jv_nar, r['Dr'], r['Cr']))
                            conn.execute("UPDATE ChartOfAccounts SET Balance = Balance + ? - ? WHERE AccountName = ?", (r['Dr'], r['Cr'], r['Account']))
                    conn.commit()
                st.success("JV Posted!")
                st.session_state.jv_rows = [{"Account": "", "Dr": 0.0, "Cr": 0.0, "Nar": ""}]
                st.rerun()
            else: st.error("Debit & Credit must be equal!")

# --- TAB 2, 3, 4 (LEDGER, REPORTS, HISTORY) ---
with tab2:
    st.subheader("📖 Party Ledger")
    target = st.selectbox("Select Account", all_heads, key="led_s")
    if target:
        with db_connect() as conn:
            df = fetch_df(conn, "SELECT Date, Description, Debit, Credit FROM Transactions WHERE AccountName=? OR PayeeName=? ORDER BY Date ASC", (target, target))
            if not df.empty:
                df['Balance'] = (df['Credit'] - df['Debit']).cumsum()
                st.dataframe(df, use_container_width=True)

with tab3:
    st.subheader("📊 Financial Reports")
    rep = st.radio("Report", ["Trial Balance", "P&L"], horizontal=True)
    with db_connect() as conn:
        if rep == "Trial Balance": st.dataframe(fetch_df(conn, "SELECT AccountName, Balance FROM ChartOfAccounts"))
        else:
            pl = fetch_df(conn, "SELECT SUM(Credit) as Inc, SUM(Debit) as Exp FROM Transactions")
            st.metric("Net Profit", f"Rs. {(pl['Inc'].iloc - pl['Exp'].iloc or 0):,.0f}")

with tab4:
    st.subheader("📜 Recent Transactions (Edit/Delete)")
    with db_connect() as conn:
        h_df = fetch_df(conn, "SELECT rowid as ID, Date, AccountName as Method, PayeeName as Account, Description, Debit, Credit FROM Transactions ORDER BY rowid DESC LIMIT 15")
    if not h_df.empty:
        st.data_editor(h_df, use_container_width=True, hide_index=True, disabled=["ID"])
        del_id = st.number_input("ID to Delete", step=1, min_value=0)
        if st.button("🗑️ Delete"):
            with db_connect() as conn:
                conn.execute("DELETE FROM Transactions WHERE rowid=?", (del_id,))
                conn.commit()
            st.rerun()
