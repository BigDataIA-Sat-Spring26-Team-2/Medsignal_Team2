"""
streamlit_app/pages/6_evidence_explorer.py — Literature Evidence Explorer

Rendering rule: every st.markdown() block must be fully self-contained.
No div opened in one st.markdown() can be closed in another.
"""

import requests
import streamlit as st
from datetime import datetime
import os
from pathlib import Path
from dotenv import load_dotenv

st.set_page_config(
    page_title="MedSignal — Evidence Explorer",
    page_icon ="⚕",
    layout    ="wide",
    initial_sidebar_state="collapsed",
)

load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")
API_BASE = os.getenv("MEDSIGNAL_API_BASE", "http://localhost:8000").strip().strip('"').strip("'").rstrip("/")


# ── CSS — copied verbatim from 3_hitl_queue.py ────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&family=Inter:wght@300;400;500;600&display=swap');

:root {
    --bg-base       : #080C14;
    --bg-surface    : #0E1421;
    --bg-elevated   : #141C2E;
    --bg-hover      : #1A2238;
    --border        : rgba(255,255,255,0.06);
    --border-strong : rgba(255,255,255,0.10);
    --text-primary  : #EEF2FF;
    --text-secondary: #9BAEC8;
    --text-muted    : #5E7498;
    --text-dim      : #4A5D7A;
    --p1            : #F72A2A;
    --p1-dim        : rgba(247,42,42,0.12);
    --p1-border     : rgba(247,42,42,0.30);
    --p2            : #F97316;
    --p2-dim        : rgba(249,115,22,0.12);
    --p2-border     : rgba(249,115,22,0.30);
    --p3            : #EAB308;
    --p3-dim        : rgba(234,179,8,0.12);
    --p3-border     : rgba(234,179,8,0.30);
    --p4            : #22C55E;
    --p4-dim        : rgba(34,197,94,0.10);
    --p4-border     : rgba(34,197,94,0.25);
    --accent        : #3B82F6;
    --accent-dim    : rgba(59,130,246,0.15);
    --amber         : #F59E0B;
    --amber-dim     : rgba(245,158,11,0.12);
    --amber-border  : rgba(245,158,11,0.30);
    --font-display  : 'Syne', sans-serif;
    --font-mono     : 'JetBrains Mono', monospace;
    --font-body     : 'Inter', sans-serif;
}

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

html, body, .stApp {
    background: var(--bg-base) !important;
    color: var(--text-primary) !important;
    font-family: var(--font-body) !important;
    -webkit-font-smoothing: antialiased;
}

#MainMenu, footer, header,
[data-testid="stToolbar"],
[data-testid="stDecoration"],
[data-testid="stStatusWidget"] { display: none !important; }
[data-testid="stSidebar"] { display: none !important; }
.block-container { padding: 0 !important; max-width: 100% !important; }

/* ── Centre Streamlit's own container to match ms-wrap ──────────────────────── */
section[data-testid="stMain"] > div {
    padding-left: calc((100vw - 1100px) / 2) !important;
    padding-right: calc((100vw - 1100px) / 2) !important;
    max-width: 100% !important;
}

/* ── Topbar ─────────────────────────────────────────────────────────────────── */
.ms-topbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    height: 60px;
    padding: 0 48px;
    background: var(--bg-surface);
    border-bottom: 1px solid var(--border);
    position: sticky;
    top: 0;
    z-index: 200;
}
.ms-brand {
    font-family: var(--font-display);
    font-size: 20px;
    font-weight: 700;
    letter-spacing: -0.3px;
    color: var(--text-primary);
}
.ms-brand span { color: var(--accent); }
.ms-nav { display: flex; gap: 4px; }
.ms-navlink {
    font-family: var(--font-body);
    font-size: 14px;
    font-weight: 500;
    color: var(--text-secondary);
    text-decoration: none;
    padding: 7px 16px;
    border-radius: 7px;
    transition: background 0.12s, color 0.12s;
}
.ms-navlink:hover { background: var(--bg-elevated); color: var(--text-primary); }
.ms-navlink.active { background: var(--bg-elevated); color: var(--text-primary); }
.ms-live {
    display: flex;
    align-items: center;
    gap: 8px;
    font-family: var(--font-mono);
    font-size: 13px;
    color: var(--text-muted);
}
.ms-live-dot {
    width: 6px; height: 6px; border-radius: 50%;
    background: var(--p4);
    animation: blink 2.5s ease-in-out infinite;
}
@keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.25} }

/* ── Page ───────────────────────────────────────────────────────────────────── */
.ms-wrap {
    max-width: 1100px;
    margin: 0 auto;
    padding: 48px 0 80px;
}

/* ── Page header — centred ──────────────────────────────────────────────────── */
.ms-page-header {
    text-align: center;
    margin-bottom: 44px;
}
.ms-page-title {
    font-family: var(--font-display);
    font-size: 44px;
    font-weight: 700;
    color: var(--text-primary);
    letter-spacing: -1.2px;
    line-height: 1;
    margin-bottom: 12px;
}
.ms-page-desc {
    font-size: 16px;
    color: var(--text-secondary);
    line-height: 1.6;
    max-width: 560px;
    margin: 0 auto;
}

/* ── Card sections ──────────────────────────────────────────────────────────── */
.ms-card-top {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-bottom: none;
    border-radius: 12px 12px 0 0;
    padding: 28px 32px 24px;
    position: relative;
    overflow: hidden;
}
.ms-card-top::before {
    content:'';
    position:absolute;
    left:0;top:0;bottom:0;
    width:4px;
}
.ms-card-top.tier-p1::before{background:var(--p1)}
.ms-card-top.tier-p2::before{background:var(--p2)}
.ms-card-top.tier-p3::before{background:var(--p3)}
.ms-card-top.tier-p4::before{background:var(--p4)}

.ms-card-mid {
    background: var(--bg-surface);
    border-left: 1px solid var(--border);
    border-right: 1px solid var(--border);
    padding: 0 0 16px;
}
.ms-card-bottom {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-top: 1px solid var(--border);
    border-radius: 0 0 12px 12px;
    padding: 14px 0 18px;
    margin-bottom: 20px;
}

/* Card header */
.ms-card-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    margin-bottom: 24px;
}
.ms-drug {
    font-family: var(--font-display);
    font-size: 24px;
    font-weight: 700;
    color: var(--text-primary);
    letter-spacing: -0.4px;
    text-transform: capitalize;
    margin-bottom: 6px;
}
.ms-pt {
    font-size: 15px;
    color: var(--text-secondary);
    font-weight: 400;
}
.ms-card-badges { display:flex; align-items:center; gap:10px; }
.ms-priority {
    font-family: var(--font-mono);
    font-size: 13px;
    font-weight: 500;
    letter-spacing: 0.8px;
    padding: 6px 14px;
    border-radius: 6px;
    border: 1px solid;
}
.ms-priority.p1{color:var(--p1);background:var(--p1-dim);border-color:var(--p1-border)}
.ms-priority.p2{color:var(--p2);background:var(--p2-dim);border-color:var(--p2-border)}
.ms-priority.p3{color:var(--p3);background:var(--p3-dim);border-color:var(--p3-border)}
.ms-priority.p4{color:var(--p4);background:var(--p4-dim);border-color:var(--p4-border)}
.ms-action-badge {
    font-family: var(--font-mono);
    font-size: 12px;
    font-weight: 500;
    letter-spacing: 0.8px;
    text-transform: uppercase;
    color: var(--text-secondary);
    background: var(--bg-elevated);
    border: 1px solid var(--border-strong);
    padding: 6px 14px;
    border-radius: 6px;
}

/* Retriever badges */
.ms-retriever-hnsw {
    font-family: var(--font-mono);
    font-size: 12px;
    font-weight: 500;
    letter-spacing: 0.8px;
    padding: 4px 10px;
    border-radius: 5px;
    color: var(--accent);
    background: var(--accent-dim);
    border: 1px solid rgba(59,130,246,0.30);
}
.ms-retriever-bm25 {
    font-family: var(--font-mono);
    font-size: 12px;
    font-weight: 500;
    letter-spacing: 0.8px;
    padding: 4px 10px;
    border-radius: 5px;
    color: var(--amber);
    background: var(--amber-dim);
    border: 1px solid var(--amber-border);
}

/* Metrics — unified panel */
.ms-metrics {
    display: flex;
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    border-radius: 8px;
    overflow: hidden;
    margin-bottom: 20px;
}
.ms-metric {
    flex: 1;
    padding: 16px 12px;
    text-align: center;
    border-right: 1px solid var(--border);
}
.ms-metric:last-child { border-right: none; }
.ms-metric-label {
    font-family: var(--font-mono);
    font-size: 11px;
    font-weight: 500;
    letter-spacing: 1.2px;
    text-transform: uppercase;
    color: var(--text-muted);
    margin-bottom: 10px;
    white-space: nowrap;
}
.ms-metric-value {
    font-family: var(--font-mono);
    font-size: 24px;
    font-weight: 500;
    color: var(--text-primary);
}
.ms-metric-value.hl { color: var(--accent); }

/* Score bars */
.ms-score-item {
    display: flex;
    align-items: center;
    gap: 12px;
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 12px 16px;
}
.ms-score-label {
    font-family: var(--font-mono);
    font-size: 12px;
    font-weight: 500;
    letter-spacing: 0.8px;
    text-transform: uppercase;
    color: var(--text-muted);
    width: 84px;
    flex-shrink: 0;
}
.ms-score-track {
    flex: 1;
    height: 4px;
    background: var(--bg-hover);
    border-radius: 2px;
    overflow: hidden;
}
.ms-score-fill { height: 100%; border-radius: 2px; }
.ms-score-val {
    font-family: var(--font-mono);
    font-size: 15px;
    font-weight: 500;
    color: var(--text-secondary);
    width: 48px;
    text-align: right;
    flex-shrink: 0;
}

/* Abstract cards */
.ms-abstract-card {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px 24px;
    margin-bottom: 14px;
}
.ms-abstract-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 14px;
}
.ms-rank {
    font-family: var(--font-mono);
    font-size: 22px;
    font-weight: 500;
    color: var(--text-dim);
    width: 32px;
    flex-shrink: 0;
}
.ms-abstract-meta {
    display: flex;
    align-items: center;
    gap: 10px;
    flex: 1;
}
.ms-pmid-link {
    font-family: var(--font-mono);
    font-size: 13px;
    color: var(--accent);
    text-decoration: none;
}
.ms-pmid-link:hover { text-decoration: underline; }
.ms-abstract-text {
    font-size: 14px;
    color: var(--text-secondary);
    line-height: 1.7;
    margin-bottom: 12px;
}
.ms-abstract-footer {
    display: flex;
    align-items: center;
    justify-content: space-between;
}
.ms-drug-tag {
    font-family: var(--font-mono);
    font-size: 11px;
    color: var(--text-dim);
    letter-spacing: 0.6px;
}

/* Section header */
.ms-section-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 20px;
    margin-top: 36px;
}
.ms-section-title {
    font-family: var(--font-display);
    font-size: 20px;
    font-weight: 600;
    color: var(--text-primary);
    letter-spacing: -0.3px;
}
.ms-count-badge {
    font-family: var(--font-mono);
    font-size: 12px;
    font-weight: 500;
    color: var(--text-muted);
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    padding: 3px 10px;
    border-radius: 5px;
}

/* Summary line */
.ms-summary-line {
    font-family: var(--font-mono);
    font-size: 13px;
    color: var(--text-muted);
    margin-top: 8px;
    padding: 12px 16px;
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    border-radius: 7px;
}

/* ── Empty / error ──────────────────────────────────────────────────────────── */
.ms-empty { text-align:center; padding:80px 40px; }
.ms-empty-title {
    font-family:var(--font-display);
    font-size:26px; color:var(--text-secondary);
    margin-bottom:12px; letter-spacing:-0.3px;
}
.ms-empty-desc { font-size:15px; color:var(--text-muted); line-height:1.6; }
.ms-error {
    background:rgba(220,38,38,0.08);
    border:1px solid rgba(220,38,38,0.20);
    border-radius:10px; padding:18px 24px;
    font-family:var(--font-mono);
    font-size:13px; color:#F87171; margin-bottom:24px;
}

/* ── Selectbox ──────────────────────────────────────────────────────────────── */
.stSelectbox > div > div {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-strong) !important;
    border-radius: 7px !important;
    color: var(--text-primary) !important;
    font-family: var(--font-mono) !important;
    font-size: 13px !important;
}
.stSelectbox label {
    font-family: var(--font-mono) !important;
    font-size: 10px !important;
    font-weight: 500 !important;
    letter-spacing: 1.5px !important;
    text-transform: uppercase !important;
    color: var(--text-muted) !important;
}

/* ── Buttons ─────────────────────────────────────────────────────────────────── */
[data-testid="stButton"] button {
    font-family: var(--font-mono) !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    letter-spacing: 0.8px !important;
    text-transform: uppercase !important;
    padding: 10px 24px !important;
    width: 100% !important;
    border-radius: 7px !important;
    border: 1px solid rgba(59,130,246,0.40) !important;
    background: rgba(59,130,246,0.10) !important;
    color: #93C5FD !important;
    transition: all 0.12s !important;
}
[data-testid="stButton"] button:hover {
    background: rgba(59,130,246,0.20) !important;
    border-color: rgba(59,130,246,0.60) !important;
    color: #fff !important;
}

::-webkit-scrollbar { width:5px; height:5px; }
::-webkit-scrollbar-track { background:var(--bg-base); }
::-webkit-scrollbar-thumb { background:var(--bg-hover); border-radius:3px; }
::-webkit-scrollbar-thumb:hover { background:var(--text-muted); }
</style>
""", unsafe_allow_html=True)


# ── Helpers ────────────────────────────────────────────────────────────────────

def fetch_queue():
    try:
        r = requests.get(f"{API_BASE}/hitl/queue", timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return None
    except Exception as e:
        return []

def fetch_evidence(drug_key: str, pt: str):
    try:
        r = requests.get(
            f"{API_BASE}/signals/{drug_key}/{pt}/evidence",
            timeout=30,
        )
        if r.status_code == 404:
            return "not_found", r.json().get("detail", "Signal not found")
        r.raise_for_status()
        return "ok", r.json()
    except requests.exceptions.ConnectionError:
        return "conn_error", None
    except Exception as e:
        return "error", str(e)

def fprr(v):
    try: return f"{float(v):.2f}"
    except: return "—"

def fsc(v):
    try: return f"{float(v):.3f}"
    except: return "—"

def pc(p):
    return (p or "p4").lower()

def sbar_color(score, kind):
    if kind == "stat":
        return "#F72A2A" if score >= 0.7 else "#F97316" if score >= 0.5 else "#3B82F6"
    return "#22C55E" if score >= 0.5 else "#EAB308" if score >= 0.3 else "#4A5568"


# ── Session state ──────────────────────────────────────────────────────────────

for k, v in [
    ("ev_result", None),
    ("ev_drug", None),
    ("ev_pt", None),
    ("ev_expanded", {}),
]:
    if k not in st.session_state:
        st.session_state[k] = v


# ── Topbar ─────────────────────────────────────────────────────────────────────

st.markdown(f"""
<div class="ms-topbar">
    <div class="ms-brand">Med<span>Signal</span></div>
    <nav class="ms-nav">
        <a class="ms-navlink" href="/signal_feed">Signal Feed</a>
        <a class="ms-navlink" href="/signal_detail">Signal Detail</a>
        <a class="ms-navlink" href="/hitl_queue">Review Queue</a>
        <a class="ms-navlink" href="/evaluation">Evaluation</a>
        <a class="ms-navlink" href="/metrics">Metrics</a>
        <a class="ms-navlink active" href="/evidence_explorer">Evidence</a>
    </nav>
    <div class="ms-live">
        <div class="ms-live-dot"></div>
        {datetime.utcnow().strftime("%d %b %Y")}
    </div>
</div>
""", unsafe_allow_html=True)


# ── Page header ────────────────────────────────────────────────────────────────

st.markdown("""
<div class="ms-wrap">
    <div class="ms-page-header">
        <div class="ms-page-title">Evidence Explorer</div>
        <div class="ms-page-desc">
            ChromaDB retrieval results for any flagged drug-reaction pair.
            Includes on-demand investigated signals.
        </div>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Load queue ─────────────────────────────────────────────────────────────────

queue = fetch_queue()

if queue is None:
    st.markdown("""
    <div class="ms-error">
        Cannot reach API at localhost:8000 —
        run: poetry run uvicorn main:app --reload --port 8000
    </div>
    """, unsafe_allow_html=True)
    st.stop()

if not queue:
    st.markdown("""
    <div class="ms-empty">
        <div class="ms-empty-title">No investigated signals found</div>
        <div class="ms-empty-desc">
            Run the agent pipeline or click Investigate on the Signal Detail page
            to process a drug-reaction pair through the pipeline first.
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ── Build drug → reactions map ─────────────────────────────────────────────────

drug_to_pts: dict[str, list[str]] = {}
signal_map: dict[tuple, dict] = {}

for sig in queue:
    dk = sig.get("drug_key", "")
    pt_val = sig.get("pt", "")
    if not dk or not pt_val:
        continue
    drug_to_pts.setdefault(dk, [])
    if pt_val not in drug_to_pts[dk]:
        drug_to_pts[dk].append(pt_val)
    signal_map[(dk, pt_val)] = sig

sorted_drugs = sorted(drug_to_pts.keys(), key=lambda x: x.lower())


# ── Section 1: Cascading dropdowns ────────────────────────────────────────────

st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

col_drug, col_pt = st.columns([0.55, 0.45])

with col_drug:
    drug_options = [d.capitalize() for d in sorted_drugs]
    drug_display = st.selectbox(
        "DRUG",
        options=drug_options,
        key="ev_drug_select",
    )
    selected_drug = sorted_drugs[drug_options.index(drug_display)] if drug_display else None

with col_pt:
    if selected_drug:
        pt_options = sorted(drug_to_pts.get(selected_drug, []))
    else:
        pt_options = []
    selected_pt = st.selectbox(
        "ADVERSE REACTION",
        options=pt_options,
        key="ev_pt_select",
    )

st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

show_btn = st.button("Show Evidence", key="ev_show_btn", use_container_width=True)

if show_btn and selected_drug and selected_pt:
    with st.spinner("Querying ChromaDB — this may take 3-5 seconds..."):
        status, data = fetch_evidence(selected_drug, selected_pt)

    if status == "conn_error":
        st.markdown("""
        <div class="ms-error">
            Cannot reach API at localhost:8000 —
            run: poetry run uvicorn main:app --reload --port 8000
        </div>
        """, unsafe_allow_html=True)
        st.stop()
    elif status == "not_found":
        st.markdown(f"""
        <div class="ms-error">
            Signal not found in signals_flagged: {selected_drug} × {selected_pt}.<br>
            If this signal was recently investigated on-demand, try refreshing. Otherwise,
            click Investigate on the Signal Detail page first.
        </div>
        """, unsafe_allow_html=True)
        st.stop()
    elif status == "error":
        st.markdown(f"""
        <div class="ms-error">
            Evidence fetch failed: {data}
        </div>
        """, unsafe_allow_html=True)
        st.stop()
    else:
        st.session_state["ev_result"] = data
        st.session_state["ev_drug"]   = selected_drug
        st.session_state["ev_pt"]     = selected_pt
        st.session_state["ev_expanded"] = {}


# ── Sections 2 & 3: Signal context + abstracts ────────────────────────────────

result  = st.session_state.get("ev_result")
ev_drug = st.session_state.get("ev_drug")
ev_pt   = st.session_state.get("ev_pt")

if result and ev_drug and ev_pt:
    sig = signal_map.get((ev_drug, ev_pt), {})

    priority   = (sig.get("priority") or "P4").upper()
    stat_score = float(sig.get("stat_score") or 0)
    lit_score  = float(sig.get("lit_score")  or 0)
    prr        = sig.get("prr")
    case_count = sig.get("case_count") or sig.get("drug_reaction_count") or 0
    death      = int(sig.get("death_count") or 0)
    rec_action = sig.get("recommended_action") or "—"
    pclass     = pc(priority)
    sc         = sbar_color(stat_score, "stat")
    lc         = sbar_color(lit_score, "lit")

    # ── Section 2: Signal context card ────────────────────────────────────────
    st.markdown(f"""
<div class="ms-section-header">
    <div class="ms-section-title">Signal Context</div>
</div>
<div class="ms-card-top tier-{pclass}">
    <div class="ms-card-header">
        <div>
            <div class="ms-drug">{ev_drug}</div>
            <div class="ms-pt">{ev_pt}</div>
        </div>
        <div class="ms-card-badges">
            <div class="ms-priority {pclass}">{priority}</div>
            <div class="ms-action-badge">{rec_action}</div>
        </div>
    </div>
    <div class="ms-metrics">
        <div class="ms-metric">
            <div class="ms-metric-label">PRR</div>
            <div class="ms-metric-value hl">{fprr(prr)}</div>
        </div>
        <div class="ms-metric">
            <div class="ms-metric-label">Cases</div>
            <div class="ms-metric-value">{int(case_count):,}</div>
        </div>
        <div class="ms-metric">
            <div class="ms-metric-label">Deaths</div>
            <div class="ms-metric-value">{death}</div>
        </div>
        <div class="ms-metric">
            <div class="ms-metric-label">Stat Score</div>
            <div class="ms-metric-value">{fsc(stat_score)}</div>
        </div>
        <div class="ms-metric">
            <div class="ms-metric-label">Lit Score</div>
            <div class="ms-metric-value">{fsc(lit_score)}</div>
        </div>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">
        <div class="ms-score-item">
            <div class="ms-score-label">Stat Score</div>
            <div class="ms-score-track">
                <div class="ms-score-fill" style="width:{stat_score*100:.1f}%;background:{sc};"></div>
            </div>
            <div class="ms-score-val">{fsc(stat_score)}</div>
        </div>
        <div class="ms-score-item">
            <div class="ms-score-label">Lit Score</div>
            <div class="ms-score-track">
                <div class="ms-score-fill" style="width:{lit_score*100:.1f}%;background:{lc};"></div>
            </div>
            <div class="ms-score-val">{fsc(lit_score)}</div>
        </div>
    </div>
</div>
<div style="background:var(--bg-surface);border:1px solid var(--border);
     border-top:none;border-radius:0 0 12px 12px;height:8px;margin-bottom:24px;"></div>
""", unsafe_allow_html=True)

    # ── Section 3: Abstract results ───────────────────────────────────────────
    abstracts = result.get("abstracts", [])
    summary   = result.get("summary", {})
    hnsw_cnt  = summary.get("hnsw_count", 0)
    bm25_cnt  = summary.get("bm25_count", 0)
    avg_sim   = summary.get("avg_similarity", 0.0)

    st.markdown(f"""
<div class="ms-section-header">
    <div class="ms-section-title">Retrieved Abstracts</div>
    <div class="ms-count-badge">{len(abstracts)}</div>
</div>
""", unsafe_allow_html=True)

    if not abstracts:
        st.markdown("""
<div class="ms-empty">
    <div class="ms-empty-title">No abstracts found</div>
    <div class="ms-empty-desc">
        No abstracts found in ChromaDB for this drug above the similarity threshold.<br>
        If this is a non-golden drug, click Investigate on the Signal Detail page
        first to load its PubMed abstracts.
    </div>
</div>
""", unsafe_allow_html=True)
    else:
        for i, abstract in enumerate(abstracts):
            pmid        = abstract.get("pmid", "unknown")
            text        = abstract.get("text", "")
            similarity  = float(abstract.get("similarity", 0))
            retriever   = abstract.get("retriever", "hnsw")
            drug_name   = abstract.get("drug_name", ev_drug)
            rank        = i + 1
            card_key    = f"ev_abs_{rank}"
            is_expanded = st.session_state["ev_expanded"].get(card_key, False)

            preview     = text[:350] + ("…" if len(text) > 350 else "")
            disp_text   = text if is_expanded else preview

            ret_class   = "ms-retriever-hnsw" if retriever == "hnsw" else "ms-retriever-bm25"
            ret_label   = "HNSW" if retriever == "hnsw" else "BM25"
            sim_color   = "#3B82F6" if retriever == "hnsw" else "#F59E0B"
            pubmed_url  = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

            # Abstract card — self-contained
            st.markdown(f"""
<div class="ms-abstract-card">
    <div class="ms-abstract-header">
        <div class="ms-rank">#{rank}</div>
        <div class="ms-abstract-meta">
            <span class="{ret_class}">{ret_label}</span>
            <a class="ms-pmid-link" href="{pubmed_url}" target="_blank">PMID {pmid}</a>
        </div>
        <div class="ms-score-item" style="min-width:220px;padding:8px 12px;">
            <div class="ms-score-label">Similarity</div>
            <div class="ms-score-track">
                <div class="ms-score-fill" style="width:{similarity*100:.1f}%;background:{sim_color};"></div>
            </div>
            <div class="ms-score-val">{similarity:.3f}</div>
        </div>
    </div>
    <div class="ms-abstract-text">{disp_text}</div>
    <div class="ms-abstract-footer">
        <div></div>
        <div class="ms-drug-tag">{drug_name}</div>
    </div>
</div>
""", unsafe_allow_html=True)

            # Read more toggle — own block, widget only
            if len(text) > 350:
                if st.button(
                    "Collapse" if is_expanded else "Read more",
                    key=f"toggle_{card_key}",
                ):
                    st.session_state["ev_expanded"][card_key] = not is_expanded
                    st.rerun()

        # Summary line — self-contained
        st.markdown(f"""
<div class="ms-summary-line">
    {hnsw_cnt} HNSW &nbsp;·&nbsp; {bm25_cnt} BM25 &nbsp;·&nbsp; avg similarity {avg_sim:.3f}
</div>
""", unsafe_allow_html=True)
