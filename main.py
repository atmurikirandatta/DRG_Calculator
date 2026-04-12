"""
DRG Calculator — FastAPI Backend
Single-file backend for MS-DRG calculation and CMS reference data.

Run: uvicorn main:app --reload --port 8000
"""

import sqlite3
import os
from datetime import datetime, date
from typing import Optional
from contextlib import contextmanager, asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# CMS MS-DRG V43.1 Grouper — Direct JPype bridge to official CMS Java JARs
JARS_PATH = "./jars/*"

try:
    import jpype
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[JARS_PATH])

    from jpype import JClass
    GfcPoa = JClass('com.mmm.his.cer.foundation.model.GfcPoa')
    MsdrgRuntimeOption = JClass('gov.agency.msdrg.model.v2.MsdrgRuntimeOption')
    RuntimeOptions = JClass('gov.agency.msdrg.model.v2.RuntimeOptions')
    MsdrgOption = JClass('gov.agency.msdrg.model.v2.MsdrgOption')
    MsdrgHospitalStatusOptionFlag = JClass('gov.agency.msdrg.model.v2.enumeration.MsdrgHospitalStatusOptionFlag')
    MsdrgAffectDrgOptionFlag = JClass('gov.agency.msdrg.model.v2.enumeration.MsdrgAffectDrgOptionFlag')
    MarkingLogicTieBreaker = JClass('gov.agency.msdrg.model.v2.enumeration.MarkingLogicTieBreaker')
    MsdrgDischargeStatus = JClass('gov.agency.msdrg.model.v2.enumeration.MsdrgDischargeStatus')
    MsdrgSex = JClass('gov.agency.msdrg.model.v2.enumeration.MsdrgSex')
    MsdrgInputCls = JClass('gov.agency.msdrg.model.v2.transfer.input.MsdrgInput')
    MsdrgInputDxCode = JClass('gov.agency.msdrg.model.v2.transfer.input.MsdrgInputDxCode')
    MsdrgInputPrCode = JClass('gov.agency.msdrg.model.v2.transfer.input.MsdrgInputPrCode')
    MsdrgClaim = JClass('gov.agency.msdrg.model.v2.transfer.MsdrgClaim')
    MsdrgComponent = JClass('gov.agency.msdrg.v431.MsdrgComponent')
    ArrayList = JClass('java.util.ArrayList')

    # Initialize runtime options (once)
    _rt_options = RuntimeOptions()
    _rt_options.setPoaReportingExempt(MsdrgHospitalStatusOptionFlag.NON_EXEMPT)
    _rt_options.setComputeAffectDrg(MsdrgAffectDrgOptionFlag.COMPUTE)
    _rt_options.setMarkingLogicTieBreaker(MarkingLogicTieBreaker.CLINICAL_SIGNIFICANCE)
    _msdrg_runtime = MsdrgRuntimeOption()
    _msdrg_runtime.put(MsdrgOption.RUNTIME_OPTION_FLAGS, _rt_options)
    _msdrg_component = MsdrgComponent(_msdrg_runtime)

    GROUPER_AVAILABLE = True
    print("CMS MS-DRG V43.1 Grouper initialized successfully")

except Exception as e:
    GROUPER_AVAILABLE = False
    _msdrg_component = None
    print(f"WARNING: CMS Grouper failed to initialize: {e}")

# Discharge status mapping
DISCHARGE_MAP = {
    "01": "HOME_SELFCARE_ROUTINE",
    "02": "SHORT_TERM_HOSPITAL",
    "03": "SNF",
    "04": "CUST_SUPP_CARE",
    "05": "CANC_CHILD_HOSP",
    "06": "HOME_HEALTH_SERVICE",
    "07": "LEFT_AGAINST_MEDICAL_ADVICE",
    "20": "DIED",
    "30": "STILL_A_PATIENT",
    "50": "HOSPICE_HOME",
    "51": "HOSPICE_MEDICAL_FACILITY",
    "61": "SWING_BED",
    "62": "REHAB_FACILITY_REHAB_UNIT",
    "63": "LONG_TERM_CARE_HOSPITAL",
    "65": "PSYCH_HOSP_UNIT",
    "70": "OTH_INSTITUTION",
}

# ============================================================
# APP CONFIGURATION
# ============================================================

@asynccontextmanager
async def lifespan(app):
    """Verify databases exist on startup."""
    if not os.path.exists(CMS_DB_PATH):
        print(f"WARNING: CMS reference database not found at {CMS_DB_PATH}")
        print("Run create_cms_reference_db.py first.")
    if not os.path.exists(CALC_DB_PATH):
        print(f"WARNING: Calculator database not found at {CALC_DB_PATH}")
        print("Run create_drg_db.py and seed_synthetic_data.py first.")
    yield

app = FastAPI(
    title="DRG Calculator API",
    description="Calculate MS-DRG assignments from ICD-10 codes using CMS data",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database paths
CMS_DB_PATH = "./database/cms_reference.db"
CALC_DB_PATH = "./database/drg_calculator.db"


# ============================================================
# DATABASE HELPERS
# ============================================================

@contextmanager
def get_cms_db():
    """Connect to CMS reference database."""
    conn = sqlite3.connect(CMS_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_calc_db():
    """Connect to DRG calculator database."""
    conn = sqlite3.connect(CALC_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=OFF;")
    try:
        yield conn
    finally:
        conn.close()


def row_to_dict(row):
    """Convert sqlite3.Row to dict."""
    if row is None:
        return None
    return dict(row)


def rows_to_list(rows):
    """Convert list of sqlite3.Row to list of dicts."""
    return [dict(r) for r in rows]


# ============================================================
# REQUEST / RESPONSE MODELS
# ============================================================

class SecondaryDiagnosis(BaseModel):
    code: str
    poa: str = Field(default="Y", pattern="^(Y|N|U|W|1)$")


class DRGRequest(BaseModel):
    principal_dx: str
    secondary_dx: list[SecondaryDiagnosis] = []
    procedures: list[str] = []
    age: int = Field(ge=0, le=124)
    sex: str = Field(pattern="^(M|F)$")
    discharge_status: str = "01"
    grouper_version: str = "V43.1"


class ClaimCreateRequest(BaseModel):
    patient_id: int
    encounter_num: str
    admit_date: str
    discharge_date: str
    discharge_status: str = "01"
    patient_age: int
    patient_sex: str = Field(pattern="^(M|F)$")
    principal_dx: str
    principal_dx_poa: str = "Y"
    secondary_dx: list[SecondaryDiagnosis] = []
    procedures: list[dict] = []  # [{"code": "0SR9019", "date": "2026-03-10"}]


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/api/health")
def health_check():
    """Check API and database status."""
    cms_ok = os.path.exists(CMS_DB_PATH)
    calc_ok = os.path.exists(CALC_DB_PATH)

    cms_stats = {}
    if cms_ok:
        with get_cms_db() as conn:
            cms_stats = {
                "icd10cm_codes": conn.execute("SELECT COUNT(*) c FROM icd10cm_codes").fetchone()["c"],
                "icd10pcs_codes": conn.execute("SELECT COUNT(*) c FROM icd10pcs_codes").fetchone()["c"],
                "drg_definitions": conn.execute("SELECT COUNT(*) c FROM drg_definitions").fetchone()["c"],
                "cc_list": conn.execute("SELECT COUNT(*) c FROM cc_list").fetchone()["c"],
                "mcc_list": conn.execute("SELECT COUNT(*) c FROM mcc_list").fetchone()["c"],
            }

    calc_stats = {}
    if calc_ok:
        with get_calc_db() as conn:
            calc_stats = {
                "patients": conn.execute("SELECT COUNT(*) c FROM patients").fetchone()["c"],
                "claims": conn.execute("SELECT COUNT(*) c FROM claims").fetchone()["c"],
            }

    return {
        "status": "healthy" if cms_ok and calc_ok else "degraded",
        "cms_database": {"connected": cms_ok, "tables": cms_stats},
        "calculator_database": {"connected": calc_ok, "tables": calc_stats},
    }


# ============================================================
# 1. SEARCH ENDPOINTS — Query CMS reference data
# ============================================================

@app.get("/api/search/diagnosis")
def search_diagnosis(
    q: str = Query(..., min_length=1, description="Search by code or description"),
    limit: int = Query(20, ge=1, le=100),
):
    """Search ICD-10-CM diagnosis codes by code or description."""
    with get_cms_db() as conn:
        query = "%{}%".format(q.upper())
        rows = conn.execute("""
            SELECT code, code_formatted, short_desc, long_desc, is_billable, chapter
            FROM icd10cm_codes
            WHERE (UPPER(code) LIKE ? OR UPPER(short_desc) LIKE ? OR UPPER(long_desc) LIKE ?)
            AND is_billable = 1
            ORDER BY
                CASE WHEN UPPER(code) = ? THEN 0
                     WHEN UPPER(code) LIKE ? THEN 1
                     ELSE 2 END,
                code
            LIMIT ?
        """, (query, query, query, q.upper(), q.upper() + "%", limit)).fetchall()

        results = rows_to_list(rows)

        # Add CC/MCC status for each code
        for r in results:
            r["cc_mcc_status"] = get_cc_mcc_status(conn, r["code"])

        return {"query": q, "count": len(results), "results": results}


@app.get("/api/search/procedure")
def search_procedure(
    q: str = Query(..., min_length=1, description="Search by code or description"),
    limit: int = Query(20, ge=1, le=100),
):
    """Search ICD-10-PCS procedure codes by code or description."""
    with get_cms_db() as conn:
        query = "%{}%".format(q.upper())
        rows = conn.execute("""
            SELECT code, short_desc, long_desc, is_billable
            FROM icd10pcs_codes
            WHERE (UPPER(code) LIKE ? OR UPPER(short_desc) LIKE ? OR UPPER(long_desc) LIKE ?)
            AND is_billable = 1
            ORDER BY
                CASE WHEN UPPER(code) = ? THEN 0
                     WHEN UPPER(code) LIKE ? THEN 1
                     ELSE 2 END,
                code
            LIMIT ?
        """, (query, query, query, q.upper(), q.upper() + "%", limit)).fetchall()

        return {"query": q, "count": len(rows), "results": rows_to_list(rows)}


@app.get("/api/search/drg")
def search_drg(
    q: str = Query(..., min_length=1, description="Search by DRG number or title"),
    limit: int = Query(20, ge=1, le=100),
):
    """Search MS-DRG definitions by number or title."""
    with get_cms_db() as conn:
        query = "%{}%".format(q.upper())
        rows = conn.execute("""
            SELECT d.msdrg, d.msdrg_title, d.mdc_code, d.drg_type,
                   d.severity_level, d.base_drg_group,
                   m.mdc_title,
                   w.relative_weight, w.gmlos, w.amlos
            FROM drg_definitions d
            LEFT JOIN mdc m ON d.mdc_code = m.mdc_code
            LEFT JOIN drg_weights w ON d.msdrg = w.msdrg
            WHERE UPPER(d.msdrg) LIKE ? OR UPPER(d.msdrg_title) LIKE ?
            ORDER BY CAST(d.msdrg AS INTEGER)
            LIMIT ?
        """, (query, query, limit)).fetchall()

        return {"query": q, "count": len(rows), "results": rows_to_list(rows)}


# ============================================================
# 2. LOOKUP ENDPOINTS — Get details for specific codes
# ============================================================

@app.get("/api/diagnosis/{code}")
def get_diagnosis(code: str):
    """Get full details for a specific ICD-10-CM code."""
    with get_cms_db() as conn:
        row = conn.execute("""
            SELECT code, code_formatted, short_desc, long_desc, is_billable, chapter
            FROM icd10cm_codes WHERE code = ?
        """, (code.upper().replace(".", ""),)).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Diagnosis code {code} not found")

        result = row_to_dict(row)
        result["cc_mcc_status"] = get_cc_mcc_status(conn, result["code"])

        # Get MDC mapping
        mdc = conn.execute("""
            SELECT dm.mdc_code, m.mdc_title
            FROM dx_mdc_mapping dm
            JOIN mdc m ON dm.mdc_code = m.mdc_code
            WHERE dm.dx_code = ?
        """, (result["code"],)).fetchone()
        result["mdc"] = row_to_dict(mdc) if mdc else None

        return result


@app.get("/api/procedure/{code}")
def get_procedure(code: str):
    """Get full details for a specific ICD-10-PCS code."""
    with get_cms_db() as conn:
        row = conn.execute("""
            SELECT code, short_desc, long_desc, is_billable
            FROM icd10pcs_codes WHERE code = ?
        """, (code.upper(),)).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Procedure code {code} not found")

        return row_to_dict(row)


@app.get("/api/drg/{msdrg}")
def get_drg(msdrg: str):
    """Get full details for a specific MS-DRG."""
    with get_cms_db() as conn:
        row = conn.execute("""
            SELECT d.msdrg, d.msdrg_title, d.mdc_code, d.drg_type,
                   d.severity_level, d.base_drg_group,
                   m.mdc_title, m.body_system,
                   w.relative_weight, w.gmlos, w.amlos, w.sso_threshold
            FROM drg_definitions d
            LEFT JOIN mdc m ON d.mdc_code = m.mdc_code
            LEFT JOIN drg_weights w ON d.msdrg = w.msdrg
            WHERE d.msdrg = ?
        """, (msdrg,)).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"MS-DRG {msdrg} not found")

        result = row_to_dict(row)

        # Get related DRGs in same base group
        related = conn.execute("""
            SELECT d.msdrg, d.msdrg_title, d.severity_level,
                   w.relative_weight, w.gmlos, w.amlos
            FROM drg_definitions d
            LEFT JOIN drg_weights w ON d.msdrg = w.msdrg
            WHERE d.base_drg_group = ?
            ORDER BY CAST(d.msdrg AS INTEGER)
        """, (result["base_drg_group"],)).fetchall()
        result["related_drgs"] = rows_to_list(related)

        return result


@app.get("/api/mdc")
def list_mdc():
    """List all Major Diagnostic Categories."""
    with get_cms_db() as conn:
        rows = conn.execute("""
            SELECT mdc_code, mdc_title, body_system FROM mdc ORDER BY mdc_code
        """).fetchall()
        return {"count": len(rows), "results": rows_to_list(rows)}


@app.get("/api/mdc/{mdc_code}")
def get_mdc(mdc_code: str):
    """Get MDC details with its DRGs."""
    with get_cms_db() as conn:
        mdc = conn.execute("""
            SELECT mdc_code, mdc_title, body_system FROM mdc WHERE mdc_code = ?
        """, (mdc_code,)).fetchone()

        if not mdc:
            raise HTTPException(status_code=404, detail=f"MDC {mdc_code} not found")

        drgs = conn.execute("""
            SELECT d.msdrg, d.msdrg_title, d.drg_type, d.severity_level,
                   w.relative_weight, w.gmlos, w.amlos
            FROM drg_definitions d
            LEFT JOIN drg_weights w ON d.msdrg = w.msdrg
            WHERE d.mdc_code = ?
            ORDER BY CAST(d.msdrg AS INTEGER)
        """, (mdc_code,)).fetchall()

        result = row_to_dict(mdc)
        result["drgs"] = rows_to_list(drgs)
        return result


# ============================================================
# 3. DRG CALCULATION ENDPOINT
# ============================================================

@app.post("/api/calculate-drg")
def calculate_drg(request: DRGRequest):
    """
    Calculate MS-DRG from diagnosis/procedure codes and demographics.
    Uses drgpy (Python MS-DRG grouper) for calculation.
    """
    with get_cms_db() as conn:

        # --- Step 1: Validate principal diagnosis ---
        pdx = conn.execute(
            "SELECT code, code_formatted, short_desc, is_billable FROM icd10cm_codes WHERE code = ?",
            (request.principal_dx.upper().replace(".", ""),)
        ).fetchone()

        if not pdx:
            pdx_code = request.principal_dx.upper().replace(".", "")
            pdx = {"code": pdx_code, "code_formatted": request.principal_dx, "short_desc": pdx_code}
        else:
            pdx = row_to_dict(pdx)

        # Check if non-billable header code — suggest alternatives
        if pdx.get("is_billable") == 0:
            suggestions = conn.execute("""
                SELECT code_formatted, short_desc FROM icd10cm_codes
                WHERE code LIKE ? AND is_billable = 1
                ORDER BY code LIMIT 10
            """, (pdx["code"] + "%",)).fetchall()
            raise HTTPException(
                status_code=400,
                detail={
                    "message": f"{pdx.get('code_formatted', pdx['code'])} is a header code (not billable). Use a specific billable code instead.",
                    "suggestions": [{"code": s["code_formatted"], "desc": s["short_desc"]} for s in suggestions],
                }
            )

        # --- Step 2: Validate secondary diagnoses ---
        secondary_details = []
        for dx in request.secondary_dx:
            row = conn.execute(
                "SELECT code, code_formatted, short_desc FROM icd10cm_codes WHERE code = ?",
                (dx.code.upper().replace(".", ""),)
            ).fetchone()
            if not row:
                detail = {"code": dx.code.upper().replace(".", ""), "code_formatted": dx.code, "short_desc": dx.code}
            else:
                detail = row_to_dict(row)
            detail["poa"] = dx.poa
            detail["cc_mcc_status"] = get_cc_mcc_status(conn, detail["code"])
            secondary_details.append(detail)

        # --- Step 3: Validate procedures ---
        procedure_details = []
        for px_code in request.procedures:
            row = conn.execute(
                "SELECT code, short_desc FROM icd10pcs_codes WHERE code = ?",
                (px_code.upper(),)
            ).fetchone()
            if not row:
                procedure_details.append({"code": px_code.upper(), "short_desc": px_code})
            else:
                procedure_details.append(row_to_dict(row))

        # --- Step 4: Calculate DRG ---
        # DEMO: Uses database lookup logic
        # PRODUCTION: Replace with Myelin grouper call
        drg_result = calculate_drg_from_codes(
            conn=conn,
            principal_dx=pdx["code"],
            secondary_dx=secondary_details,
            procedures=procedure_details,
            age=request.age,
            sex=request.sex,
            discharge_status=request.discharge_status,
        )

        # --- Step 5: Build response ---
        return {
            "grouper_version": request.grouper_version,
            "input": {
                "principal_dx": pdx,
                "secondary_dx": secondary_details,
                "procedures": procedure_details,
                "age": request.age,
                "sex": request.sex,
                "discharge_status": request.discharge_status,
            },
            "result": drg_result,
            "calculated_at": datetime.now().isoformat(),
        }


def get_cc_mcc_status(conn, dx_code: str) -> str:
    """Check if a diagnosis code is CC, MCC, or Non-CC."""
    is_mcc = conn.execute(
        "SELECT 1 FROM mcc_list WHERE dx_code = ?", (dx_code,)
    ).fetchone()
    if is_mcc:
        return "MCC"

    is_cc = conn.execute(
        "SELECT 1 FROM cc_list WHERE dx_code = ?", (dx_code,)
    ).fetchone()
    if is_cc:
        return "CC"

    return "Non-CC"


def calculate_drg_from_codes(conn, principal_dx, secondary_dx, procedures, age, sex, discharge_status):
    """
    Production DRG calculation using official CMS MS-DRG V43.1 Java Grouper via JPype.
    """
    if not GROUPER_AVAILABLE:
        raise HTTPException(status_code=503, detail="CMS Grouper not initialized. Check JARs path.")

    # POA mapping
    poa_map = {"Y": GfcPoa.Y, "N": GfcPoa.N, "U": GfcPoa.U, "W": GfcPoa.W, "1": GfcPoa.ONE}

    # Build secondary diagnoses
    sdx_list = ArrayList()
    for dx in secondary_dx:
        code = dx["code"] if isinstance(dx, dict) else dx.code
        poa_str = dx.get("poa", "Y") if isinstance(dx, dict) else dx.poa
        sdx_list.add(MsdrgInputDxCode(code, poa_map.get(poa_str, GfcPoa.Y)))

    # Build procedures
    px_list = ArrayList()
    for px in procedures:
        code = px["code"] if isinstance(px, dict) else px
        px_list.add(MsdrgInputPrCode(code))

    # Map sex
    sex_enum = MsdrgSex.FEMALE if sex == "F" else MsdrgSex.MALE

    # Map discharge status
    ds_name = DISCHARGE_MAP.get(discharge_status, "HOME_SELFCARE_ROUTINE")
    ds_enum = MsdrgDischargeStatus.valueOf(ds_name)

    # Build input
    inp = MsdrgInputCls.builder() \
        .withPrincipalDiagnosisCode(MsdrgInputDxCode(principal_dx, GfcPoa.Y)) \
        .withSecondaryDiagnosisCodes(sdx_list) \
        .withProcedureCodes(px_list) \
        .withAgeInYears(age) \
        .withSex(sex_enum) \
        .withDischargeStatus(ds_enum) \
        .build()

    # Process
    try:
        claim = MsdrgClaim(inp)
        _msdrg_component.process(claim)
        output = claim.getOutput().get()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Grouper error: {str(e)}")

    # Extract results
    drg_val = output.getFinalDrg()
    mdc_val = output.getFinalMdc()
    msdrg = str(int(drg_val.getValue())).zfill(3)
    msdrg_title = str(drg_val.getDescription())
    mdc_code = str(int(mdc_val.getValue())).zfill(2)
    mdc_title = str(mdc_val.getDescription())
    drg_type = str(output.getFinalMedSugType())
    severity = str(output.getFinalSeverity())
    hac_status = str(output.getHacStatus())
    return_code = str(output.getFinalGrc())

    # Enrich with weights from CMS reference DB
    drg_row = conn.execute("""
        SELECT d.base_drg_group, w.relative_weight, w.gmlos, w.amlos
        FROM drg_definitions d
        LEFT JOIN drg_weights w ON d.msdrg = w.msdrg
        WHERE d.msdrg = ?
    """, (msdrg,)).fetchone()

    relative_weight = drg_row["relative_weight"] if drg_row else 0.0
    gmlos = drg_row["gmlos"] if drg_row else 0.0
    amlos = drg_row["amlos"] if drg_row else 0.0

    # Per-diagnosis severity from grouper output
    qualifying_codes = []
    sdx_outputs = output.getSdxOutput()
    for i in range(sdx_outputs.size()):
        sdx_out = sdx_outputs.get(i)
        sev_flag = str(sdx_out.getFinalSeverityUsage())
        dx_code = str(sdx_out.getInputDxCode().getValue())
        if sev_flag in ("MCC", "CC"):
            qualifying_codes.append({"code": dx_code, "status": sev_flag})

    # Related DRGs
    related = []
    if drg_row and drg_row["base_drg_group"]:
        related = conn.execute("""
            SELECT d.msdrg, d.msdrg_title, d.severity_level,
                   w.relative_weight, w.gmlos, w.amlos
            FROM drg_definitions d
            LEFT JOIN drg_weights w ON d.msdrg = w.msdrg
            WHERE d.base_drg_group = ?
            ORDER BY CAST(d.msdrg AS INTEGER)
        """, (drg_row["base_drg_group"],)).fetchall()

    return {
        "msdrg": msdrg,
        "msdrg_title": msdrg_title,
        "mdc_code": mdc_code,
        "mdc_title": mdc_title,
        "drg_type": drg_type,
        "severity_level": severity,
        "relative_weight": relative_weight or 0.0,
        "gmlos": gmlos or 0.0,
        "amlos": amlos or 0.0,
        "qualifying_cc_mcc": qualifying_codes,
        "diagnosis_details": secondary_dx,
        "procedure_details": procedures,
        "related_drgs": rows_to_list(related) if related else [],
        "hac_status": hac_status,
        "return_code": return_code,
        "mce_errors": [],
    }


# ============================================================
# 4. PATIENT-BASED DRG CALCULATION (single patient ID input)
# ============================================================

@app.get("/api/patient/{patient_id}/details")
def get_patient_full_details(patient_id: int):
    """
    Enter patient ID → get ALL data: demographics, claims, diagnoses,
    procedures, CC/MCC status — everything pre-populated.
    """
    with get_calc_db() as calc_conn:
        # Get patient
        patient = calc_conn.execute(
            "SELECT * FROM patients WHERE patient_id = ?", (patient_id,)
        ).fetchone()
        if not patient:
            raise HTTPException(status_code=404, detail="Patient not found")

        # Get all claims for this patient
        claims = calc_conn.execute("""
            SELECT * FROM claims WHERE patient_id = ? ORDER BY discharge_date DESC
        """, (patient_id,)).fetchall()

        enriched_claims = []
        for claim in claims:
            claim_dict = row_to_dict(claim)

            # Get diagnoses
            diagnoses = calc_conn.execute("""
                SELECT * FROM claim_diagnoses WHERE claim_id = ? ORDER BY dx_sequence
            """, (claim_dict["claim_id"],)).fetchall()

            # Get procedures
            procedures = calc_conn.execute("""
                SELECT * FROM claim_procedures WHERE claim_id = ? ORDER BY px_sequence
            """, (claim_dict["claim_id"],)).fetchall()

            # Get existing grouper result
            grouper_result = calc_conn.execute("""
                SELECT * FROM grouper_results WHERE claim_id = ?
            """, (claim_dict["claim_id"],)).fetchone()

            # Enrich with CMS descriptions
            with get_cms_db() as cms_conn:
                enriched_dx = []
                for dx in diagnoses:
                    dx_dict = row_to_dict(dx)
                    cms_row = cms_conn.execute(
                        "SELECT code_formatted, short_desc, long_desc FROM icd10cm_codes WHERE code = ?",
                        (dx_dict["dx_code"],)
                    ).fetchone()
                    if cms_row:
                        dx_dict.update(row_to_dict(cms_row))
                    dx_dict["cc_mcc_status"] = get_cc_mcc_status(cms_conn, dx_dict["dx_code"])

                    # MDC mapping
                    mdc = cms_conn.execute("""
                        SELECT dm.mdc_code, m.mdc_title
                        FROM dx_mdc_mapping dm JOIN mdc m ON dm.mdc_code = m.mdc_code
                        WHERE dm.dx_code = ?
                    """, (dx_dict["dx_code"],)).fetchone()
                    dx_dict["mdc"] = row_to_dict(mdc) if mdc else None
                    enriched_dx.append(dx_dict)

                enriched_px = []
                for px in procedures:
                    px_dict = row_to_dict(px)
                    cms_row = cms_conn.execute(
                        "SELECT short_desc, long_desc FROM icd10pcs_codes WHERE code = ?",
                        (px_dict["px_code"],)
                    ).fetchone()
                    if cms_row:
                        px_dict.update(row_to_dict(cms_row))
                    enriched_px.append(px_dict)

            claim_dict["diagnoses"] = enriched_dx
            claim_dict["procedures"] = enriched_px
            claim_dict["grouper_result"] = row_to_dict(grouper_result) if grouper_result else None
            enriched_claims.append(claim_dict)

        result = row_to_dict(patient)
        result["claims"] = enriched_claims
        return result


@app.post("/api/patient/{patient_id}/calculate-drg")
def calculate_patient_drg(patient_id: int, claim_index: int = Query(0, description="Which claim to group (0 = latest)")):
    """
    Enter patient ID → backend fetches all data → calculates DRG → returns full result.

    Flow:
    1. Fetch patient demographics from drg_calculator.db
    2. Fetch their claim (diagnoses + procedures) from drg_calculator.db
    3. Enrich codes with descriptions from cms_reference.db
    4. Pass to grouper (demo logic / Myelin)
    5. Store result in grouper_results
    6. Return everything
    """
    with get_calc_db() as calc_conn:
        # --- Fetch patient ---
        patient = calc_conn.execute(
            "SELECT * FROM patients WHERE patient_id = ?", (patient_id,)
        ).fetchone()
        if not patient:
            raise HTTPException(status_code=404, detail="Patient not found")

        # --- Fetch claims ---
        claims = calc_conn.execute("""
            SELECT * FROM claims WHERE patient_id = ? ORDER BY discharge_date DESC
        """, (patient_id,)).fetchall()

        if not claims:
            raise HTTPException(status_code=404, detail="No claims found for this patient")

        if claim_index >= len(claims):
            raise HTTPException(status_code=400, detail=f"Patient has {len(claims)} claim(s). claim_index must be 0-{len(claims)-1}")

        claim = claims[claim_index]
        claim_id = claim["claim_id"]

        # --- Fetch diagnoses ---
        diagnoses = calc_conn.execute("""
            SELECT dx_code, dx_sequence, poa_indicator
            FROM claim_diagnoses WHERE claim_id = ? ORDER BY dx_sequence
        """, (claim_id,)).fetchall()

        # --- Fetch procedures ---
        procedures = calc_conn.execute("""
            SELECT px_code, px_sequence, px_date
            FROM claim_procedures WHERE claim_id = ? ORDER BY px_sequence
        """, (claim_id,)).fetchall()

        # --- Build grouper input ---
        principal_dx = None
        secondary_dx = []
        for dx in diagnoses:
            if dx["dx_sequence"] == 1:
                principal_dx = dx["dx_code"]
            else:
                secondary_dx.append(SecondaryDiagnosis(code=dx["dx_code"], poa=dx["poa_indicator"]))

        if not principal_dx:
            raise HTTPException(status_code=400, detail="No principal diagnosis on this claim")

        drg_request = DRGRequest(
            principal_dx=principal_dx,
            secondary_dx=secondary_dx,
            procedures=[px["px_code"] for px in procedures],
            age=claim["patient_age"],
            sex=claim["patient_sex"],
            discharge_status=claim["discharge_status"],
        )

        # --- Calculate DRG ---
        drg_response = calculate_drg(drg_request)
        drg_result = drg_response["result"]

        # --- Store result ---
        calc_conn.execute("DELETE FROM grouper_results WHERE claim_id = ?", (claim_id,))
        calc_conn.execute("""
            INSERT INTO grouper_results
            (claim_id, msdrg, mdc_code, drg_type, severity_level,
             relative_weight, gmlos, amlos, grouper_version, grouped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            claim_id, drg_result["msdrg"], drg_result["mdc_code"],
            drg_result["drg_type"], drg_result["severity_level"],
            drg_result["relative_weight"], drg_result["gmlos"],
            drg_result["amlos"], drg_request.grouper_version,
        ))

        calc_conn.execute(
            "UPDATE claims SET status='grouped', updated_at=datetime('now') WHERE claim_id=?",
            (claim_id,)
        )

        calc_conn.execute("""
            INSERT INTO audit_log (claim_id, action, new_msdrg, notes, created_at)
            VALUES (?, 'grouped', ?, 'Patient-based calculation', datetime('now'))
        """, (claim_id, drg_result["msdrg"]))

        calc_conn.commit()

        # --- Return full response ---
        return {
            "patient": row_to_dict(patient),
            "claim": {
                "claim_id": claim_id,
                "encounter_num": claim["encounter_num"],
                "admit_date": claim["admit_date"],
                "discharge_date": claim["discharge_date"],
                "discharge_status": claim["discharge_status"],
                "age": claim["patient_age"],
                "sex": claim["patient_sex"],
                "status": "grouped",
            },
            "input": drg_response["input"],
            "result": drg_result,
            "calculated_at": drg_response["calculated_at"],
        }


# ============================================================
# 5. CLAIMS / PATIENT ENDPOINTS
# ============================================================

@app.get("/api/patients")
def list_patients(limit: int = Query(50, ge=1, le=200)):
    """List all patients."""
    with get_calc_db() as conn:
        rows = conn.execute("""
            SELECT p.*,
                   (SELECT COUNT(*) FROM claims c WHERE c.patient_id = p.patient_id) as claim_count
            FROM patients p
            ORDER BY p.patient_id
            LIMIT ?
        """, (limit,)).fetchall()
        return {"count": len(rows), "results": rows_to_list(rows)}


@app.get("/api/patients/{patient_id}")
def get_patient(patient_id: int):
    """Get patient details with their claims."""
    with get_calc_db() as conn:
        patient = conn.execute(
            "SELECT * FROM patients WHERE patient_id = ?", (patient_id,)
        ).fetchone()

        if not patient:
            raise HTTPException(status_code=404, detail="Patient not found")

        claims = conn.execute("""
            SELECT * FROM claims WHERE patient_id = ? ORDER BY admit_date DESC
        """, (patient_id,)).fetchall()

        result = row_to_dict(patient)
        result["claims"] = rows_to_list(claims)
        return result


@app.get("/api/claims")
def list_claims(
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
):
    """List all claims with patient info."""
    with get_calc_db() as conn:
        query = """
            SELECT c.*, p.first_name, p.last_name, p.mrn,
                   (SELECT dx_code FROM claim_diagnoses
                    WHERE claim_id = c.claim_id AND dx_sequence = 1) as principal_dx,
                   (SELECT COUNT(*) FROM claim_diagnoses
                    WHERE claim_id = c.claim_id AND dx_sequence > 1) as secondary_dx_count,
                   (SELECT COUNT(*) FROM claim_procedures
                    WHERE claim_id = c.claim_id) as procedure_count,
                   gr.msdrg, gr.mdc_code, gr.severity_level, gr.relative_weight
            FROM claims c
            JOIN patients p ON c.patient_id = p.patient_id
            LEFT JOIN grouper_results gr ON c.claim_id = gr.claim_id
        """
        params = []

        if status:
            query += " WHERE c.status = ?"
            params.append(status)

        query += " ORDER BY c.discharge_date DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        return {"count": len(rows), "results": rows_to_list(rows)}


@app.get("/api/claims/{claim_id}")
def get_claim(claim_id: int):
    """Get full claim details including diagnoses, procedures, and grouper result."""
    with get_calc_db() as conn:
        claim = conn.execute("""
            SELECT c.*, p.first_name, p.last_name, p.mrn, p.date_of_birth
            FROM claims c
            JOIN patients p ON c.patient_id = p.patient_id
            WHERE c.claim_id = ?
        """, (claim_id,)).fetchone()

        if not claim:
            raise HTTPException(status_code=404, detail="Claim not found")

        diagnoses = conn.execute("""
            SELECT * FROM claim_diagnoses WHERE claim_id = ? ORDER BY dx_sequence
        """, (claim_id,)).fetchall()

        procedures = conn.execute("""
            SELECT * FROM claim_procedures WHERE claim_id = ? ORDER BY px_sequence
        """, (claim_id,)).fetchall()

        grouper_result = conn.execute("""
            SELECT * FROM grouper_results WHERE claim_id = ?
        """, (claim_id,)).fetchone()

        # Enrich diagnoses with descriptions from CMS DB
        enriched_dx = []
        with get_cms_db() as cms_conn:
            for dx in diagnoses:
                dx_dict = row_to_dict(dx)
                cms_row = cms_conn.execute(
                    "SELECT code_formatted, short_desc, long_desc FROM icd10cm_codes WHERE code = ?",
                    (dx_dict["dx_code"],)
                ).fetchone()
                if cms_row:
                    dx_dict.update(row_to_dict(cms_row))
                dx_dict["cc_mcc_status"] = get_cc_mcc_status(cms_conn, dx_dict["dx_code"])
                enriched_dx.append(dx_dict)

            enriched_px = []
            for px in procedures:
                px_dict = row_to_dict(px)
                cms_row = cms_conn.execute(
                    "SELECT short_desc, long_desc FROM icd10pcs_codes WHERE code = ?",
                    (px_dict["px_code"],)
                ).fetchone()
                if cms_row:
                    px_dict.update(row_to_dict(cms_row))
                enriched_px.append(px_dict)

        result = row_to_dict(claim)
        result["diagnoses"] = enriched_dx
        result["procedures"] = enriched_px
        result["grouper_result"] = row_to_dict(grouper_result) if grouper_result else None

        return result


# ============================================================
# 5. CALCULATE DRG FOR EXISTING CLAIM
# ============================================================

@app.post("/api/claims/{claim_id}/calculate")
def calculate_claim_drg(claim_id: int):
    """Calculate DRG for an existing claim using its stored codes."""
    with get_calc_db() as calc_conn:
        claim = calc_conn.execute(
            "SELECT * FROM claims WHERE claim_id = ?", (claim_id,)
        ).fetchone()

        if not claim:
            raise HTTPException(status_code=404, detail="Claim not found")

        diagnoses = calc_conn.execute("""
            SELECT dx_code, dx_sequence, poa_indicator
            FROM claim_diagnoses WHERE claim_id = ? ORDER BY dx_sequence
        """, (claim_id,)).fetchall()

        procedures = calc_conn.execute("""
            SELECT px_code FROM claim_procedures WHERE claim_id = ? ORDER BY px_sequence
        """, (claim_id,)).fetchall()

        # Build DRG request
        principal_dx = None
        secondary_dx = []
        for dx in diagnoses:
            if dx["dx_sequence"] == 1:
                principal_dx = dx["dx_code"]
            else:
                secondary_dx.append(SecondaryDiagnosis(code=dx["dx_code"], poa=dx["poa_indicator"]))

        if not principal_dx:
            raise HTTPException(status_code=400, detail="No principal diagnosis found for this claim")

        drg_request = DRGRequest(
            principal_dx=principal_dx,
            secondary_dx=secondary_dx,
            procedures=[px["px_code"] for px in procedures],
            age=claim["patient_age"],
            sex=claim["patient_sex"],
            discharge_status=claim["discharge_status"],
        )

        # Calculate
        drg_response = calculate_drg(drg_request)
        drg_result = drg_response["result"]

        # Store result
        calc_conn.execute("DELETE FROM grouper_results WHERE claim_id = ?", (claim_id,))
        calc_conn.execute("""
            INSERT INTO grouper_results
            (claim_id, msdrg, mdc_code, drg_type, severity_level,
             relative_weight, gmlos, amlos, grouper_version, grouped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            claim_id,
            drg_result["msdrg"],
            drg_result["mdc_code"],
            drg_result["drg_type"],
            drg_result["severity_level"],
            drg_result["relative_weight"],
            drg_result["gmlos"],
            drg_result["amlos"],
            drg_request.grouper_version,
        ))

        # Update claim status
        calc_conn.execute(
            "UPDATE claims SET status = 'grouped', updated_at = datetime('now') WHERE claim_id = ?",
            (claim_id,)
        )

        # Log audit
        calc_conn.execute("""
            INSERT INTO audit_log (claim_id, action, new_msdrg, notes, created_at)
            VALUES (?, 'grouped', ?, 'DRG calculated via API', datetime('now'))
        """, (claim_id, drg_result["msdrg"]))

        calc_conn.commit()

        return {
            "claim_id": claim_id,
            "status": "grouped",
            **drg_response,
        }


# ============================================================
# 6. BATCH CALCULATE — Group all pending claims
# ============================================================

@app.post("/api/claims/batch-calculate")
def batch_calculate():
    """Calculate DRG for all pending claims."""
    with get_calc_db() as calc_conn:
        pending = calc_conn.execute(
            "SELECT claim_id FROM claims WHERE status = 'pending'"
        ).fetchall()

        results = []
        errors = []

        for row in pending:
            try:
                result = calculate_claim_drg(row["claim_id"])
                results.append({
                    "claim_id": row["claim_id"],
                    "msdrg": result["result"]["msdrg"],
                    "status": "success",
                })
            except Exception as e:
                errors.append({
                    "claim_id": row["claim_id"],
                    "error": str(e),
                    "status": "failed",
                })

        return {
            "total": len(pending),
            "success": len(results),
            "failed": len(errors),
            "results": results,
            "errors": errors,
        }


# ============================================================
# 7. DASHBOARD / STATS
# ============================================================

@app.get("/api/dashboard")
def dashboard():
    """Get summary statistics for the dashboard."""
    with get_calc_db() as conn:
        total_claims = conn.execute("SELECT COUNT(*) c FROM claims").fetchone()["c"]
        grouped_claims = conn.execute("SELECT COUNT(*) c FROM claims WHERE status='grouped'").fetchone()["c"]
        pending_claims = conn.execute("SELECT COUNT(*) c FROM claims WHERE status='pending'").fetchone()["c"]
        total_patients = conn.execute("SELECT COUNT(*) c FROM patients").fetchone()["c"]

        # Top DRGs
        top_drgs = conn.execute("""
            SELECT msdrg, COUNT(*) as count
            FROM grouper_results
            GROUP BY msdrg
            ORDER BY count DESC
            LIMIT 10
        """).fetchall()

        # Recent claims
        recent = conn.execute("""
            SELECT c.claim_id, c.encounter_num, p.first_name || ' ' || p.last_name as patient_name,
                   c.discharge_date, c.status, gr.msdrg, gr.relative_weight
            FROM claims c
            JOIN patients p ON c.patient_id = p.patient_id
            LEFT JOIN grouper_results gr ON c.claim_id = gr.claim_id
            ORDER BY c.discharge_date DESC
            LIMIT 10
        """).fetchall()

        return {
            "summary": {
                "total_claims": total_claims,
                "grouped_claims": grouped_claims,
                "pending_claims": pending_claims,
                "total_patients": total_patients,
            },
            "top_drgs": rows_to_list(top_drgs),
            "recent_claims": rows_to_list(recent),
        }


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)