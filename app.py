"""
DRG Calculator — Streamlit UI
Run: streamlit run app.py
"""

import streamlit as st
import requests
import json

import os
API = os.getenv("API_URL", "https://drg-calculator.onrender.com/api")

st.set_page_config(page_title="DRG Calculator", page_icon="⚕", layout="wide")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap');
    .drg-hero {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
        border-radius: 14px; padding: 32px; color: #fff; margin-bottom: 24px;
    }
    .drg-number { font-size: 56px; font-weight: 700; letter-spacing: -2px; line-height: 1; }
    .drg-title { font-size: 16px; color: #cbd5e1; margin-top: 10px; font-weight: 500; }
    .stat-box {
        background: rgba(255,255,255,0.06); border-radius: 10px; padding: 14px 20px;
        text-align: center; border: 1px solid rgba(255,255,255,0.08);
    }
    .stat-label { font-size: 11px; color: #94a3b8; font-family: 'IBM Plex Mono', monospace; }
    .stat-value { font-size: 22px; font-weight: 700; color: #fff; }
    .chip {
        background: rgba(255,255,255,0.07); border-radius: 8px; padding: 8px 16px;
        display: inline-flex; align-items: center; gap: 8px;
        border: 1px solid rgba(255,255,255,0.08); margin-right: 8px;
    }
    .chip-label { font-size: 11px; color: #94a3b8; font-family: 'IBM Plex Mono', monospace; }
    .chip-value { font-size: 13px; font-weight: 600; color: #e2e8f0; }
    .hac-box { border-radius: 10px; padding: 16px 20px; margin-bottom: 8px; }
    .hac-good { background: #f0fdf4; border: 1px solid #bbf7d0; }
    .hac-bad { background: #fef2f2; border: 1px solid #fecaca; }
</style>
""", unsafe_allow_html=True)

DISCHARGE_OPTIONS = {
    "01": "Discharged Home",
    "02": "Short-term Hospital",
    "03": "SNF",
    "04": "Custodial Care",
    "05": "Cancer/Children's Hospital",
    "06": "Home Health",
    "07": "Left AMA",
    "20": "Expired",
    "30": "Still Patient",
    "50": "Hospice-Home",
    "51": "Hospice-Facility",
    "61": "Swing Bed",
    "62": "Rehab Facility",
    "63": "LTCH",
    "65": "Psych Hospital",
    "70": "Other Institution",
}

POA_OPTIONS = ["Y", "N", "U", "W", "1"]


def discharge_label(code):
    return DISCHARGE_OPTIONS.get(code, code)


def handle_error(res):
    """Handle API errors with smart suggestion display."""
    err = res.json().get("detail", "Calculation failed")
    if isinstance(err, dict):
        st.error(err.get("message", "Error"))
        suggestions = err.get("suggestions", [])
        if suggestions:
            st.markdown("**Did you mean:**")
            for s in suggestions:
                st.markdown(f"- `{s['code']}` — {s['desc']}")
    else:
        st.error(f"Error: {err}")


def render_results(data):
    """Render DRG results — shared by both tabs."""
    r = data["result"]
    inp = data["input"]

    # --- DRG Result Hero ---
    stat_html = ""
    for label, val in [("Weight", f"{r['relative_weight']:.4f}" if r['relative_weight'] else "—"),
                       ("GMLOS", f"{r['gmlos']} days" if r['gmlos'] else "—"),
                       ("AMLOS", f"{r['amlos']} days" if r['amlos'] else "—")]:
        stat_html += f'<div class="stat-box"><div class="stat-label">{label}</div><div class="stat-value">{val}</div></div>'

    sev_color = "#dc2626" if r.get("severity_level") == "MCC" else "#f59e0b" if r.get("severity_level") == "CC" else "#22c55e"

    st.markdown(f"""
    <div class="drg-hero">
        <div style="display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:24px;">
            <div>
                <div class="stat-label">MS-DRG</div>
                <div class="drg-number">{r['msdrg']}</div>
                <div class="drg-title">{r['msdrg_title']}</div>
            </div>
            <div style="display:flex; gap:16px; flex-wrap:wrap;">
                {stat_html}
            </div>
        </div>
        <div style="display:flex; gap:12px; margin-top:24px; flex-wrap:wrap;">
            <div class="chip"><span class="chip-label">MDC</span><span class="chip-value">{r.get('mdc_code', '—')} · {r.get('mdc_title', 'Unknown')}</span></div>
            <div class="chip"><span class="chip-label">Type</span><span class="chip-value">{r.get('drg_type', '—')}</span></div>
            <div class="chip"><span class="chip-label">Severity</span><span class="chip-value" style="color:{sev_color}">{r.get('severity_level', '—')}</span></div>
            <div class="chip"><span class="chip-label">Return Code</span><span class="chip-value">{r.get('return_code', '—')}</span></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # --- Patient & Encounter (only if from DB) ---
    if "patient" in data and data["patient"]:
        pat = data["patient"]
        clm = data.get("claim", {})
        col_pat, col_enc = st.columns(2)
        with col_pat:
            st.subheader("Patient")
            st.markdown(f"""
            | Field | Value |
            |-------|-------|
            | **Name** | {pat.get('first_name', '')} {pat.get('last_name', '')} |
            | **MRN** | `{pat.get('mrn', '—')}` |
            | **DOB** | {pat.get('date_of_birth', '—')} |
            | **Sex** | {"Male" if pat.get('sex') == "M" else "Female"} |
            """)
        with col_enc:
            st.subheader("Encounter")
            st.markdown(f"""
            | Field | Value |
            |-------|-------|
            | **Encounter** | `{clm.get('encounter_num', '—')}` |
            | **Admit** | {clm.get('admit_date', '—')} |
            | **Discharge** | {clm.get('discharge_date', '—')} |
            | **Age** | {clm.get('age', inp.get('age', '—'))} |
            | **Status** | {discharge_label(str(clm.get('discharge_status', inp.get('discharge_status', '—'))))} |
            """)
    else:
        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Demographics")
            st.markdown(f"""
            | Field | Value |
            |-------|-------|
            | **Age** | {inp.get('age', '—')} |
            | **Sex** | {"Male" if inp.get('sex') == "M" else "Female"} |
            | **Discharge Status** | {discharge_label(str(inp.get('discharge_status', '—')))} |
            """)

    # --- Diagnoses ---
    st.subheader("Diagnoses")
    dx_rows = []
    pdx = inp.get("principal_dx", {})
    dx_rows.append({
        "Role": "🔵 Principal",
        "Code": pdx.get("code_formatted", pdx.get("code", "—")),
        "Description": pdx.get("short_desc", ""),
        "POA": "Y",
        "CC/MCC": "—",
    })
    for dx in inp.get("secondary_dx", []):
        dx_rows.append({
            "Role": "Secondary",
            "Code": dx.get("code_formatted", dx.get("code", "—")),
            "Description": dx.get("short_desc", ""),
            "POA": dx.get("poa", "Y"),
            "CC/MCC": dx.get("cc_mcc_status", "Non-CC"),
        })
    st.dataframe(dx_rows, use_container_width=True, hide_index=True)

    # --- Procedures ---
    procs = inp.get("procedures", [])
    if procs:
        st.subheader("Procedures")
        px_rows = [{"Code": px.get("code", ""), "Description": px.get("short_desc", "")} for px in procs]
        st.dataframe(px_rows, use_container_width=True, hide_index=True)

    # --- Qualifying CC/MCC ---
    if r.get("qualifying_cc_mcc"):
        st.subheader("Qualifying CC/MCC Codes")
        cc_cols = st.columns(min(len(r["qualifying_cc_mcc"]), 6))
        for i, q in enumerate(r["qualifying_cc_mcc"]):
            with cc_cols[i % len(cc_cols)]:
                color = "#dc2626" if q["status"] == "MCC" else "#f59e0b"
                st.markdown(f"""
                <div style="padding:12px 16px; border-radius:8px; background:{color}12; border:1px solid {color}30; text-align:center;">
                    <div style="font-family:'IBM Plex Mono',monospace; font-weight:600; font-size:16px;">{q['code']}</div>
                    <div style="font-size:12px; font-weight:700; color:{color}; margin-top:4px;">{q['status']}</div>
                </div>
                """, unsafe_allow_html=True)

    # --- HAC Impact ---
    st.subheader("HAC Impact")
    hac_status = r.get("hac_status", "NOT_APPLICABLE")
    if hac_status in ("NOT_APPLICABLE", "HAC_CRITERIA_NOT_MET", "HAC_NOT_APPLICABLE_EXEMPT", "HAC_NOT_APPLICABLE_EXCLUSION"):
        st.markdown(f"""
        <div class="hac-box hac-good">
            <div style="font-weight:600; color:#166534;">✅ No HAC Impact</div>
            <div style="font-size:13px; color:#15803d; margin-top:4px;">Status: {hac_status.replace('_', ' ').title()}</div>
        </div>
        """, unsafe_allow_html=True)
    elif hac_status == "HAC_CRITERIA_MET":
        st.markdown(f"""
        <div class="hac-box hac-bad">
            <div style="font-weight:600; color:#991b1b;">⚠️ HAC Criteria Met — DRG May Be Affected</div>
            <div style="font-size:13px; color:#b91c1c; margin-top:4px;">
                Hospital-Acquired Condition detected. The DRG assignment may have been adjusted
                due to conditions not present on admission (POA=N).
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info(f"HAC Status: {hac_status}")

    # --- MCE Validation ---
    st.subheader("MCE Validation")
    mce_errors = r.get("mce_errors", [])
    return_code = r.get("return_code", "OK")
    if return_code == "OK" and not mce_errors:
        st.markdown("""
        <div class="hac-box hac-good">
            <div style="font-weight:600; color:#166534;">✅ No Errors</div>
            <div style="font-size:13px; color:#15803d; margin-top:4px;">All codes validated successfully. Grouper return code: OK</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        if return_code != "OK":
            st.error(f"Grouper Return Code: **{return_code}**")
        for err in mce_errors:
            if isinstance(err, dict):
                st.error(f"Code: {err.get('code', '?')} — {err.get('error', err.get('message', 'Unknown error'))}")
            else:
                st.error(str(err))

    # --- Related DRGs ---
    if r.get("related_drgs"):
        st.subheader("Related DRGs (Same Base Group)")
        rel_rows = [{
            "DRG": d["msdrg"],
            "Title": d["msdrg_title"],
            "Severity": d.get("severity_level", "—"),
            "Weight": d.get("relative_weight", 0),
            "GMLOS": d.get("gmlos", 0),
            "Assigned": "✅" if d["msdrg"] == r["msdrg"] else "",
        } for d in r["related_drgs"]]
        st.dataframe(rel_rows, use_container_width=True, hide_index=True)

    # --- Raw JSON ---
    with st.expander("Show Raw JSON Response"):
        st.json(data)


# ============================================================
# HEADER
# ============================================================
st.markdown("# ⚕ DRG Calculator")
st.markdown("*MS-DRG Grouper · CMS V43.1 · Production Accurate*") # · By Kiran Datta Atmuri

with st.expander("ℹ️ How to Use", expanded=True):
    st.markdown("""
    **Option 1 — Patient Lookup:**  
    Enter a Patient ID (1–21) in the first tab. The system auto-fetches diagnoses, procedures & demographics from the database and calculates the DRG.
    
    **Option 2 — Manual Entry:**  
    Use the second tab to enter ICD-10-CM/PCS codes manually. Enter the principal diagnosis, secondary diagnoses with POA indicators, procedure codes, age, sex, and discharge status.
    """)
st.divider()

# ============================================================
# TABS
# ============================================================
tab1, tab2 = st.tabs(["📋 Patient Lookup", "✏️ Manual Entry"])

# ============================================================
# TAB 1: PATIENT LOOKUP
# ============================================================
with tab1:
    col_input, col_btn = st.columns([2, 3])
    with col_input:
        patient_id = st.text_input("Patient ID", value="", placeholder="Enter patient ID", key="tab1_pid")
    with col_btn:
        st.write("")
        st.write("")
        calc_patient = st.button("🔬 Calculate DRG", type="primary", use_container_width=True, key="tab1_btn")

    if calc_patient and patient_id:
        with st.spinner("Calculating DRG using CMS V43.1 Grouper..."):
            try:
                res = requests.post(f"{API}/patient/{patient_id}/calculate-drg?claim_index=0")
                if res.status_code != 200:
                    handle_error(res)
                    st.stop()
                render_results(res.json())
            except requests.ConnectionError:
                st.error("Cannot connect to backend. Make sure `python3 main.py` is running on port 8000.")
    elif calc_patient:
        st.warning("Please enter a Patient ID.")

# ============================================================
# TAB 2: MANUAL ENTRY
# ============================================================
with tab2:
    st.markdown("Enter ICD-10 codes manually to calculate DRG without a patient record.")

    # --- Demographics ---
    st.subheader("Patient Info")
    dem_col1, dem_col2, dem_col3 = st.columns(3)
    with dem_col1:
        age = st.text_input("Age *", value="", placeholder="e.g. 65", key="tab2_age")
    with dem_col2:
        sex = st.selectbox("Sex *", ["Male", "Female"], key="tab2_sex")
    with dem_col3:
        ds_options = [f"{k} - {v}" for k, v in DISCHARGE_OPTIONS.items()]
        discharge = st.selectbox("Discharge Status *", ds_options, key="tab2_discharge")

    st.divider()

    # --- Principal Diagnosis ---
    st.subheader("Principal Diagnosis *")
    principal_dx = st.text_input(
        "ICD-10-CM Code",
        value="",
        placeholder="e.g. A41.9 or A419",
        key="tab2_pdx",
    )

    st.divider()

    # --- Secondary Diagnoses ---
    st.subheader("Secondary Diagnoses")
    st.markdown("Enter up to 24 secondary diagnosis codes (one per row).")

    if "sdx_count" not in st.session_state:
        st.session_state.sdx_count = 3

    sdx_codes = []
    sdx_poas = []
    for i in range(st.session_state.sdx_count):
        sc1, sc2 = st.columns([4, 1])
        with sc1:
            code = st.text_input(f"Dx {i+1}", value="", placeholder="ICD-10-CM code", key=f"sdx_{i}", label_visibility="collapsed")
        with sc2:
            poa = st.selectbox(f"POA {i+1}", POA_OPTIONS, key=f"poa_{i}", label_visibility="collapsed")
        sdx_codes.append(code)
        sdx_poas.append(poa)

    col_add, col_remove = st.columns(2)
    with col_add:
        if st.button("+ Add Diagnosis", key="add_sdx"):
            if st.session_state.sdx_count < 24:
                st.session_state.sdx_count += 1
                st.rerun()
    with col_remove:
        if st.button("- Remove Last", key="remove_sdx"):
            if st.session_state.sdx_count > 1:
                st.session_state.sdx_count -= 1
                st.rerun()

    st.divider()

    # --- Procedures ---
    st.subheader("Procedures (ICD-10-PCS)")
    st.markdown("Enter up to 25 procedure codes (one per row).")

    if "px_count" not in st.session_state:
        st.session_state.px_count = 2

    px_codes = []
    for i in range(st.session_state.px_count):
        code = st.text_input(f"Px {i+1}", value="", placeholder="ICD-10-PCS code", key=f"px_{i}", label_visibility="collapsed")
        px_codes.append(code)

    col_add_px, col_remove_px = st.columns(2)
    with col_add_px:
        if st.button("+ Add Procedure", key="add_px"):
            if st.session_state.px_count < 25:
                st.session_state.px_count += 1
                st.rerun()
    with col_remove_px:
        if st.button("- Remove Last", key="remove_px"):
            if st.session_state.px_count > 1:
                st.session_state.px_count -= 1
                st.rerun()

    st.divider()

    # --- Calculate ---
    calc_manual = st.button("🔬 Calculate DRG", type="primary", use_container_width=True, key="tab2_btn")

    if calc_manual:
        if not principal_dx:
            st.error("Principal Diagnosis is required.")
            st.stop()
        if not age:
            st.error("Age is required.")
            st.stop()

        secondary_dx = []
        for code, poa in zip(sdx_codes, sdx_poas):
            code = code.strip().upper().replace(".", "")
            if code:
                secondary_dx.append({"code": code, "poa": poa})

        procedures = []
        for code in px_codes:
            code = code.strip().upper()
            if code:
                procedures.append(code)

        discharge_code = discharge.split(" - ")[0]

        payload = {
            "principal_dx": principal_dx.strip().upper().replace(".", ""),
            "secondary_dx": secondary_dx,
            "procedures": procedures,
            "age": int(age),
            "sex": "M" if sex == "Male" else "F",
            "discharge_status": discharge_code,
            "grouper_version": "V43.1",
        }

        with st.spinner("Calculating DRG using CMS V43.1 Grouper..."):
            try:
                res = requests.post(f"{API}/calculate-drg", json=payload)
                if res.status_code != 200:
                    handle_error(res)
                    st.stop()
                render_results(res.json())
            except requests.ConnectionError:
                st.error("Cannot connect to backend. Make sure `python3 main.py` is running on port 8000.")