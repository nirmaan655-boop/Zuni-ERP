import streamlit as st
import pandas as pd
from zuni_db import db_connect, fetch_df
from datetime import datetime, date

# --- 0. DATABASE INITIALIZATION ---
def init_livestock_db():
    with db_connect() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS AnimalMaster (
            TagID TEXT PRIMARY KEY, Breed TEXT, Category TEXT, CurrentPen TEXT, 
            Weight REAL DEFAULT 0, Status TEXT DEFAULT 'Active', LactationNo INTEGER DEFAULT 0)""")
        conn.execute("CREATE TABLE IF NOT EXISTS MilkLogs (Date TEXT, TagID TEXT, Morning REAL, Noon REAL, Evening REAL, Total REAL)")
        conn.execute("CREATE TABLE IF NOT EXISTS TreatmentLogs (Date TEXT, TagID TEXT, Medicines TEXT, Dose TEXT, Symptoms TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS BreedingLogs (Date TEXT, TagID TEXT, Action TEXT, SemenName TEXT, DoseQty INTEGER, PD_Result TEXT, Vet TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS CalvingLogs (Date TEXT, DamID TEXT, CalfTag TEXT, Weight REAL, Sex TEXT, Status TEXT, LactNo INTEGER)")
        conn.execute("CREATE TABLE IF NOT EXISTS WeightLogs (Date TEXT, TagID TEXT, CurrentWeight REAL, PreviousWeight REAL, Gain REAL, DaysGap INTEGER, AvgDailyGain REAL)")
        try: conn.execute("ALTER TABLE WeightLogs ADD COLUMN DaysGap INTEGER DEFAULT 0")
        except: pass
        try: conn.execute("ALTER TABLE WeightLogs ADD COLUMN AvgDailyGain REAL DEFAULT 0")
        except: pass
        conn.commit()
init_livestock_db()

# --- 1. VIP BRANDING ---
st.markdown("""
    <div style='background: linear-gradient(90deg, #001F3F 0%, #003366 100%); padding: 25px; border-radius: 15px; border-bottom: 8px solid #FF851B; margin-bottom: 20px;'>
        <h1 style='color: white; margin: 0; font-size: 45px;'>🐄 ZUNI <span style='color: #FF851B;'>LIVESTOCK PRO</span></h1>
        <p style='color: #FF851B; font-size: 18px; font-weight: bold;'>9-Tabs Full Farm Management & Smart Breeding | FY 2026</p>
    </div>
    """, unsafe_allow_html=True)

# --- 2. FRESH DATA FETCHING ---
with db_connect() as conn:
    animal_data = fetch_df(conn, "SELECT * FROM AnimalMaster")
    tag_list = animal_data['TagID'].tolist() if not animal_data.empty else []
    pen_list = ["PEN-A", "PEN-B", "CALF-PEN", "DRY-PEN", "QUARANTINE"]
    
    # 👨‍⚕️ FETCH STAFF FOR VET OPTIONS
    try:
        staff_list = fetch_df(conn, "SELECT Name FROM EmployeeMaster WHERE Designation IN ('Doctor', 'Manager', 'Supervisor')")['Name'].tolist()
    except:
        staff_list = []
    
    try:
        semen_inventory = fetch_df(conn, "SELECT ItemName, Quantity FROM ItemMaster WHERE Category = 'Semen Straws'")
        semen_options = [f"{row['ItemName']} (Stock: {row['Quantity']})" for _, row in semen_inventory.iterrows()] if not semen_inventory.empty else ["No Semen Straws"]
        med_list = ["None"] + fetch_df(conn, "SELECT ItemName FROM ItemMaster WHERE Category IN ('Medicine', 'Vaccine')")['ItemName'].tolist()
    except: semen_options, med_list = ["Error"], ["None"]

# --- HELPER: HISTORY ---
def show_history(table_name):
    st.markdown(f"---")
    st.markdown(f"### 📋 Recent {table_name} History")
    with db_connect() as conn:
        try:
            df_hist = fetch_df(conn, f"SELECT rowid as ID, * FROM {table_name} ORDER BY rowid DESC LIMIT 10")
            if not df_hist.empty:
                st.dataframe(df_hist, use_container_width=True, hide_index=True)
                c1, c2 = st.columns()
                del_id = c1.number_input(f"ID to delete", step=1, key=f"del_val_{table_name}")
                if c2.button(f"🗑️ Delete Entry", key=f"btn_{table_name}"):
                    conn.execute(f"DELETE FROM {table_name} WHERE rowid = ?", (del_id,))
                    conn.commit(); st.rerun()
        except: st.info("Waiting for records...")

# --- 3. TABS ---
t_search, t_milk, t_treat, t_breed, t_vac, t_calv, t_weight, t_move, t_reg = st.tabs([
    "🔍 360°", "🥛 MILK", "🏥 TREAT", "🧬 BREED", "💉 BULK VAC", "🍼 CALVING", "⚖️ WEIGHT", "🏠 MOVE", "📝 REG"
])

# (Search, Milk, Treat remain same as previous version)
with t_search:
    search_id = st.selectbox("Search Tag ID", [""] + tag_list)
    if search_id:
        row = animal_data[animal_data['TagID'] == search_id].iloc
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Breed", row['Breed']); c2.metric("Weight", f"{row['Weight']} kg")
        c3.metric("Lactation", f"L-{row['LactationNo']}"); c4.metric("Pen", row['CurrentPen'])
    st.divider(); st.dataframe(animal_data, use_container_width=True)

with t_milk:
    with st.form("milk_form", clear_on_submit=True):
        m_tag = st.selectbox("Tag", tag_list); m_date = st.date_input("Date", date.today())
        s1, s2, s3 = st.columns(3); m, n, e = s1.number_input("Morning"), s2.number_input("Noon"), s3.number_input("Evening")
        if st.form_submit_button("✅ SAVE YIELD"):
            with db_connect() as conn:
                conn.execute("INSERT INTO MilkLogs (Date, TagID, Morning, Noon, Evening, Total) VALUES (?,?,?,?,?,?)", (str(m_date), m_tag, m, n, e, m+n+e))
                conn.commit(); st.rerun()
    show_history("MilkLogs")

with t_treat:
    with st.form("treat_form", clear_on_submit=True):
        tr_tag = st.selectbox("Patient", tag_list)
        c1, c2 = st.columns(2); m1, m2 = c1.selectbox("Inj 1", med_list), c2.selectbox("Inj 2", med_list)
        m3, m4 = c1.selectbox("Inj 3", med_list), c2.selectbox("Inj 4", med_list)
        tr_dis = st.text_area("Remarks")
        if st.form_submit_button("💉 LOG TREATMENT"):
            all_meds = ", ".join([m for m in [m1, m2, m3, m4] if m != "None"])
            with db_connect() as conn:
                conn.execute("INSERT INTO TreatmentLogs (Date, TagID, Medicines, Dose, Symptoms) VALUES (?,?,?,?,?)", (str(date.today()), tr_tag, all_meds, "Standard", tr_dis))
                conn.commit(); st.rerun()
    show_history("TreatmentLogs")

# --- TAB 3: BREEDING (FIXED DRY OFF, NATURAL, PD & AUTO-VET) ---
with t_breed:
    st.subheader("🧬 Smart Breeding & AI")
    b_tag = st.selectbox("Select Cow Tag", [""] + tag_list, key="b_cow_tag")
    b_action = st.selectbox("Action", ["Insemination (AI)", "PD Check (+/-)", "Natural Service", "Dry Off"])
    
    with st.form("breeding_form_dynamic"):
        sem_name, sem_qty, pd_res, bull_tag = "N/A", 0, "N/A", "N/A"
        
        if b_action == "Insemination (AI)":
            c_ai1, c_ai2 = st.columns(2)
            sel_sem = c_ai1.selectbox("Select Semen Straw", semen_options)
            sem_qty = c_ai2.number_input("Straws Used", min_value=1, step=1)
            sem_name = sel_sem.split(" (")[0]
            
        elif b_action == "PD Check (+/-)":
            pd_res = st.radio("PD Result", ["Pregnant (+)", "Empty (-)", "Re-Check"], horizontal=True)
            
        elif b_action == "Natural Service":
            bull_tag = st.text_input("Enter Breeding Bull Tag ID")
            sem_name = f"Bull: {bull_tag}"
            
        elif b_action == "Dry Off":
            st.warning("⚠️ Action: Janwar ko 'Dry' mark kiya jaye ga aur DRY-PEN mein shift kiya jaye ga.")
            sem_name = "Dry Status"

        # 👨‍⚕️ VET SELECTION (AUTO + MANUAL)
        c_v1, c_v2 = st.columns(2)
        v_sel = c_v1.selectbox("Select Registered Staff", ["Manual Entry"] + staff_list)
        if v_sel == "Manual Entry":
            v_name = c_v2.text_input("Enter Vet Name Manually")
        else:
            v_name = v_sel

        if st.form_submit_button("🧬 SAVE BREEDING RECORD"):
            if b_tag:
                with db_connect() as conn:
                    conn.execute("INSERT INTO BreedingLogs (Date, TagID, Action, SemenName, DoseQty, PD_Result, Vet) VALUES (?,?,?,?,?,?,?)", 
                                 (str(date.today()), b_tag, b_action, sem_name, sem_qty, pd_res, v_name))
                    if b_action == "Insemination (AI)" and "No Semen" not in sel_sem:
                        conn.execute("UPDATE ItemMaster SET Quantity = Quantity - ? WHERE ItemName = ?", (sem_qty, sem_name))
                    if b_action == "Dry Off":
                        conn.execute("UPDATE AnimalMaster SET Status = 'Dry', CurrentPen = 'DRY-PEN' WHERE TagID = ?", (b_tag,))
                    conn.commit()
                st.success(f"MashaAllah! {b_action} recorded for {b_tag}!")
                st.rerun()
    show_history("BreedingLogs")

# (Tabs 4-9 remain same as before)
with t_vac:
    with st.form("vac_form"):
        v_pen = st.selectbox("Pen Area", pen_list); v_med = st.selectbox("Vaccine", med_list); v_dose = st.number_input("Dose/Head", 0.0)
        if st.form_submit_button("🚀 APPLY"): st.success("Started!"); st.rerun()

with t_calv:
    with st.form("calving_form"):
        dam = st.selectbox("Dam ID", tag_list); c_tag = st.text_input("Calf Tag")
        c1, c2, c3, c4 = st.columns(4); c_stat = c1.selectbox("Status", ["Normal", "Assisted", "Dystocia", "Operation"])
        c_lact = c2.number_input("Lact No", 1); c_sex = c3.selectbox("Sex", ["Female", "Male"]); c_wght = c4.number_input("Birth Weight", 0.0)
        if st.form_submit_button("🍼 SAVE"):
            with db_connect() as conn:
                conn.execute("INSERT INTO CalvingLogs (Date, DamID, CalfTag, Weight, Sex, Status, LactNo) VALUES (?,?,?,?,?,?,?)", (str(date.today()), dam, c_tag, c_wght, c_sex, c_stat, c_lact))
                conn.execute("UPDATE AnimalMaster SET LactationNo = ? WHERE TagID = ?", (c_lact, dam))
                conn.commit(); st.rerun()
    show_history("CalvingLogs")

with t_weight:
    st.subheader("⚖️ ADG Tracker")
    w_tag = st.selectbox("Select Animal", [""] + tag_list, key="w_sel")
    last_w, last_date_str = 0.0, "No Record"
    if w_tag:
        with db_connect() as conn:
            res = fetch_df(conn, "SELECT CurrentWeight, Date FROM WeightLogs WHERE TagID = ? ORDER BY rowid DESC LIMIT 1", (w_tag,))
            if not res.empty: last_w, last_date_str = res['CurrentWeight'].iloc, res['Date'].iloc
            else: last_w = animal_data[animal_data['TagID'] == w_tag]['Weight'].iloc

    with st.form("w_form_fix"):
        c1, c2 = st.columns(2); c1.number_input("Last Weight", value=float(last_w), disabled=True)
        w_date, curr_w = c2.date_input("Date", date.today()), st.number_input("New Weight", 0.0)
        if st.form_submit_button("⚖️ SAVE WEIGHT"):
            gain = curr_w - last_w
            days_gap, adg = 0, 0.0
            if last_date_str != "No Record":
                d1 = datetime.strptime(last_date_str, '%Y-%m-%d').date()
                days_gap = (w_date - d1).days
                adg = gain / days_gap if days_gap > 0 else 0.0
            with db_connect() as conn:
                conn.execute("INSERT INTO WeightLogs (Date, TagID, CurrentWeight, PreviousWeight, Gain, DaysGap, AvgDailyGain) VALUES (?,?,?,?,?,?,?)", (str(w_date), w_tag, curr_w, last_w, gain, int(days_gap), round(float(adg), 3)))
                conn.execute("UPDATE AnimalMaster SET Weight = ? WHERE TagID = ?", (curr_w, w_tag))
                conn.commit(); st.rerun()
    show_history("WeightLogs")

with t_move:
    with st.form("move_form"):
        mv_tag = st.selectbox("Animal", tag_list); mv_to = st.selectbox("New Pen", pen_list)
        if st.form_submit_button("🚛 TRANSFER"):
            with db_connect() as conn:
                conn.execute("UPDATE AnimalMaster SET CurrentPen = ? WHERE TagID = ?", (mv_to, mv_tag))
                conn.commit(); st.rerun()

with t_reg:
    with st.form("reg_form"):
        r_tag = st.text_input("Tag Number"); r_breed = st.selectbox("Breed", ["HF", "Jersey", "Sahiwal", "Cross"])
        r_cat = st.selectbox("Category", ["Cow", "Heifer", "Bull", "Calf"])
        r_lact = st.number_input("Lact No", 0); r_pen = st.selectbox("Pen", pen_list); r_wght = st.number_input("Weight", 0.0)
        if st.form_submit_button("✅ REGISTER"):
            with db_connect() as conn:
                conn.execute("INSERT INTO AnimalMaster (TagID, Breed, Category, CurrentPen, Weight, LactationNo) VALUES (?,?,?,?,?,?)", (r_tag.upper().strip(), r_breed, r_cat, r_pen, r_wght, r_lact))
                conn.commit(); st.rerun()
