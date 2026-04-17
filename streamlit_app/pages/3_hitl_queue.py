"""
streamlit_app/pages/3_hitl_queue.py — HITL Review Queue

Key rendering fix:
    Streamlit closes unclosed HTML divs when it encounters native widgets.
    Cards are split into three self-contained HTML blocks:
        ms-card-top    — header, metrics, scores, outcomes, brief
        ms-card-mid    — wraps Streamlit widgets (textarea + buttons)
        ms-card-bottom — timestamp
    All three share visual continuity via matching border/bg and
    border-radius only on the outer corners.
"""

import requests
import streamlit as st
from datetime import datetime

st.set_page_config(
    page_title="MedSignal — Review Queue",
    page_icon ="⚕",
    layout    ="wide",
    initial_sidebar_state="collapsed",
)

API_BASE = "http://localhost:8000"

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
    --text-secondary: #7B8DB0;
    --text-muted    : #3D4F6E;
    --text-dim      : #2A3850;
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

.ms-topbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    height: 56px;
    padding: 0 36px;
    background: var(--bg-surface);
    border-bottom: 1px solid var(--border);
    position: sticky;
    top: 0;
    z-index: 200;
}
.ms-brand {
    font-family: var(--font-display);
    font-size: 17px;
    font-weight: 700;
    letter-spacing: -0.3px;
    color: var(--text-primary);
}
.ms-brand span { color: var(--accent); }
.ms-nav { display: flex; gap: 2px; }
.ms-navlink {
    font-family: var(--font-body);
    font-size: 12px;
    font-weight: 500;
    color: var(--text-secondary);
    text-decoration: none;
    padding: 6px 14px;
    border-radius: 6px;
    transition: background 0.12s, color 0.12s;
}
.ms-navlink:hover { background: var(--bg-elevated); color: var(--text-primary); }
.ms-navlink.active { background: var(--bg-elevated); color: var(--text-primary); }
.ms-topbar-right { display: flex; align-items: center; }
.ms-live {
    display: flex;
    align-items: center;
    gap: 7px;
    font-family: var(--font-mono);
    font-size: 11px;
    color: var(--text-muted);
}
.ms-live-dot {
    width: 5px; height: 5px; border-radius: 50%;
    background: var(--p4);
    animation: blink 2.5s ease-in-out infinite;
}
@keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.25} }

.ms-wrap { padding: 36px 40px 60px; max-width: 1440px; margin: 0 auto; }

.ms-page-title {
    font-family: var(--font-display);
    font-size: 30px;
    font-weight: 700;
    color: var(--text-primary);
    letter-spacing: -0.8px;
    line-height: 1;
    margin-bottom: 8px;
}
.ms-page-desc {
    font-size: 13px;
    color: var(--text-secondary);
    line-height: 1.5;
    max-width: 520px;
    margin-bottom: 32px;
}

.ms-summary {
    display: grid;
    grid-template-columns: repeat(5,1fr);
    gap: 10px;
    margin-bottom: 28px;
}
.ms-stat {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px 18px;
}
.ms-stat-label {
    font-family: var(--font-mono);
    font-size: 9px;
    font-weight: 500;
    letter-spacing: 1.8px;
    text-transform: uppercase;
    color: var(--text-muted);
    margin-bottom: 10px;
}
.ms-stat-value {
    font-family: var(--font-mono);
    font-size: 30px;
    font-weight: 500;
    line-height: 1;
}
.v-total{color:var(--text-primary)}
.v-p1{color:var(--p1)}
.v-p2{color:var(--p2)}
.v-p3{color:var(--p3)}
.v-p4{color:var(--p4)}

/* Card is three sections sharing the same bg/border */
.ms-card-top {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-bottom: none;
    border-radius: 10px 10px 0 0;
    padding: 22px 24px 16px 24px;
    position: relative;
    overflow: hidden;
}
.ms-card-top::before {
    content:'';
    position:absolute;
    left:0;top:0;bottom:0;
    width:3px;
}
.ms-card-top.tier-p1::before{background:var(--p1)}
.ms-card-top.tier-p2::before{background:var(--p2)}
.ms-card-top.tier-p3::before{background:var(--p3)}
.ms-card-top.tier-p4::before{background:var(--p4)}

.ms-card-mid {
    background: var(--bg-surface);
    border-left: 1px solid var(--border);
    border-right: 1px solid var(--border);
    padding: 0 24px 12px;
}
.ms-card-bottom {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-top: 1px solid var(--border);
    border-radius: 0 0 10px 10px;
    padding: 12px 24px 16px;
}

.ms-card-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    margin-bottom: 16px;
}
.ms-drug {
    font-family: var(--font-display);
    font-size: 18px;
    font-weight: 700;
    color: var(--text-primary);
    letter-spacing: -0.3px;
    text-transform: capitalize;
}
.ms-pt {
    font-size: 13px;
    color: var(--text-secondary);
    margin-top: 3px;
}
.ms-card-badges { display:flex; align-items:center; gap:8px; margin-left:20px; }
.ms-priority {
    font-family: var(--font-mono);
    font-size: 11px;
    font-weight: 500;
    letter-spacing: 1px;
    padding: 4px 10px;
    border-radius: 5px;
    border: 1px solid;
}
.ms-priority.p1{color:var(--p1);background:var(--p1-dim);border-color:var(--p1-border)}
.ms-priority.p2{color:var(--p2);background:var(--p2-dim);border-color:var(--p2-border)}
.ms-priority.p3{color:var(--p3);background:var(--p3-dim);border-color:var(--p3-border)}
.ms-priority.p4{color:var(--p4);background:var(--p4-dim);border-color:var(--p4-border)}
.ms-action-badge {
    font-family: var(--font-mono);
    font-size: 9px;
    font-weight: 500;
    letter-spacing: 1px;
    text-transform: uppercase;
    color: var(--text-muted);
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    padding: 4px 10px;
    border-radius: 5px;
}

.ms-metrics { display:flex; gap:28px; margin-bottom:14px; }
.ms-metric { display:flex; flex-direction:column; gap:3px; }
.ms-metric-label {
    font-family:var(--font-mono);
    font-size:9px; font-weight:500;
    letter-spacing:1.5px;
    text-transform:uppercase;
    color:var(--text-muted);
}
.ms-metric-value {
    font-family:var(--font-mono);
    font-size:15px; font-weight:500;
    color:var(--text-primary);
}
.ms-metric-value.hl { color:var(--accent); }

.ms-scores { display:flex; gap:16px; margin-bottom:16px; align-items:center; }
.ms-score-item { display:flex; align-items:center; gap:10px; flex:1; }
.ms-score-label {
    font-family:var(--font-mono);
    font-size:9px; font-weight:500;
    letter-spacing:1.2px; text-transform:uppercase;
    color:var(--text-muted); width:54px; flex-shrink:0;
}
.ms-score-track {
    flex:1; height:3px;
    background:var(--bg-elevated);
    border-radius:2px; overflow:hidden;
}
.ms-score-fill { height:100%; border-radius:2px; }
.ms-score-val {
    font-family:var(--font-mono);
    font-size:11px; color:var(--text-secondary);
    width:34px; text-align:right; flex-shrink:0;
}

.ms-outcomes { display:flex; gap:8px; margin-bottom:16px; }
.ms-flag {
    font-family:var(--font-mono);
    font-size:9px; font-weight:500;
    letter-spacing:1px; text-transform:uppercase;
    padding:3px 8px; border-radius:4px;
    border:1px solid var(--border);
    color:var(--text-muted); background:var(--bg-elevated);
}
.ms-flag.on { color:var(--p1); border-color:var(--p1-border); background:var(--p1-dim); }

.ms-brief {
    font-size:13px; color:var(--text-secondary);
    line-height:1.65; margin-bottom:16px;
    padding:14px 16px;
    background:var(--bg-elevated);
    border-radius:6px;
    border-left:2px solid var(--border-strong);
}

.ms-divider { height:1px; background:var(--border); margin-bottom:0; }

.ms-timestamp {
    font-family:var(--font-mono);
    font-size:10px; color:var(--text-dim);
}

/* Buttons — always visible text */
[data-testid="stButton"] button {
    font-family: var(--font-mono) !important;
    font-size: 11px !important;
    font-weight: 500 !important;
    letter-spacing: 1px !important;
    text-transform: uppercase !important;
    padding: 10px 16px !important;
    width: 100% !important;
    border-radius: 6px !important;
    border: 1px solid rgba(255,255,255,0.12) !important;
    background: var(--bg-elevated) !important;
    color: var(--text-primary) !important;
    transition: all 0.12s !important;
}
[data-testid="stButton"] button:hover {
    background: var(--bg-hover) !important;
    border-color: rgba(255,255,255,0.22) !important;
    color: #fff !important;
}
/* Column 2 = Approve */
div[data-testid="column"]:nth-child(2) [data-testid="stButton"] button {
    border-color: rgba(34,197,94,0.40) !important;
    color: #4ADE80 !important;
    background: rgba(34,197,94,0.10) !important;
}
div[data-testid="column"]:nth-child(2) [data-testid="stButton"] button:hover {
    background: rgba(34,197,94,0.20) !important;
}
/* Column 3 = Reject */
div[data-testid="column"]:nth-child(3) [data-testid="stButton"] button {
    border-color: rgba(247,42,42,0.40) !important;
    color: #F87171 !important;
    background: rgba(247,42,42,0.10) !important;
}
div[data-testid="column"]:nth-child(3) [data-testid="stButton"] button:hover {
    background: rgba(247,42,42,0.20) !important;
}
/* Column 4 = Escalate */
div[data-testid="column"]:nth-child(4) [data-testid="stButton"] button {
    border-color: rgba(234,179,8,0.40) !important;
    color: #FACC15 !important;
    background: rgba(234,179,8,0.10) !important;
}
div[data-testid="column"]:nth-child(4) [data-testid="stButton"] button:hover {
    background: rgba(234,179,8,0.20) !important;
}

.stTextArea textarea {
    background: var(--bg-elevated) !important;
    border: 1px solid var(--border-strong) !important;
    border-radius: 6px !important;
    color: var(--text-primary) !important;
    font-family: var(--font-mono) !important;
    font-size: 12px !important;
    resize: none !important;
}
.stTextArea textarea:focus {
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 1px var(--accent-dim) !important;
}
.stTextArea label {
    font-family: var(--font-mono) !important;
    font-size: 10px !important;
    font-weight: 500 !important;
    letter-spacing: 1.5px !important;
    text-transform: uppercase !important;
    color: var(--text-muted) !important;
}

.stSelectbox > div > div {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-strong) !important;
    border-radius: 6px !important;
    color: var(--text-primary) !important;
    font-family: var(--font-mono) !important;
    font-size: 12px !important;
}

.streamlit-expanderHeader {
    background: var(--bg-elevated) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
    font-family: var(--font-mono) !important;
    font-size: 11px !important;
    color: var(--text-secondary) !important;
}

.ms-empty { text-align:center; padding:80px 40px; }
.ms-empty-title {
    font-family:var(--font-display);
    font-size:22px; color:var(--text-secondary);
    margin-bottom:10px; letter-spacing:-0.3px;
}
.ms-empty-desc { font-size:13px; color:var(--text-muted); line-height:1.6; }
.ms-error {
    background:rgba(220,38,38,0.08);
    border:1px solid rgba(220,38,38,0.20);
    border-radius:8px; padding:16px 20px;
    font-family:var(--font-mono);
    font-size:12px; color:#F87171; margin-bottom:20px;
}

::-webkit-scrollbar { width:5px; height:5px; }
::-webkit-scrollbar-track { background:var(--bg-base); }
::-webkit-scrollbar-thumb { background:var(--bg-hover); border-radius:3px; }
::-webkit-scrollbar-thumb:hover { background:var(--text-muted); }
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_queue():
    try:
        r = requests.get(f"{API_BASE}/hitl/queue", timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return None
    except Exception as e:
        st.session_state["api_error"] = str(e)
        return []


def post_decision(drug_key, pt, brief_id, decision, note):
    try:
        r = requests.post(
            f"{API_BASE}/hitl/decisions",
            json={"drug_key": drug_key, "pt": pt, "brief_id": brief_id,
                  "decision": decision, "reviewer_note": note or None},
            timeout=15,
        )
        return r.status_code == 200
    except Exception:
        return False


def sbar_color(score, kind):
    if kind == "stat":
        return "#F72A2A" if score >= 0.7 else "#F97316" if score >= 0.5 else "#3B82F6"
    return "#22C55E" if score >= 0.5 else "#EAB308" if score >= 0.3 else "#4A5568"

def fsc(v):
    try: return f"{float(v):.3f}"
    except: return "—"

def fprr(v):
    try: return f"{float(v):.2f}"
    except: return "—"

def fts(ts):
    if not ts: return "—"
    try:
        dt = datetime.fromisoformat(str(ts).replace(" ", "T"))
        return dt.strftime("%d %b %Y  %H:%M UTC")
    except: return str(ts)

def pc(p):
    return (p or "p4").lower()

def ct(sigs, tier):
    return sum(1 for s in sigs if (s.get("priority") or "").upper() == tier)


# ── Session state ─────────────────────────────────────────────────────────────

for k, v in [("submitted",{}),("expanded",{}),
              ("filter_tier","All"),("api_error",None)]:
    if k not in st.session_state:
        st.session_state[k] = v


# ── Topbar ────────────────────────────────────────────────────────────────────

st.markdown(f"""
<div class="ms-topbar">
    <div class="ms-brand">Med<span>Signal</span></div>
    <nav class="ms-nav">
        <a class="ms-navlink" href="/signal_feed">Signal Feed</a>
        <a class="ms-navlink" href="/signal_detail">Signal Detail</a>
        <a class="ms-navlink active" href="/hitl_queue">Review Queue</a>
        <a class="ms-navlink" href="/evaluation">Evaluation</a>
    </nav>
    <div class="ms-topbar-right">
        <div class="ms-live">
            <div class="ms-live-dot"></div>
            {datetime.utcnow().strftime("%d %b %Y")}
        </div>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Data ──────────────────────────────────────────────────────────────────────

queue = fetch_queue()

if queue is None:
    st.markdown("""
    <div class="ms-wrap">
        <div class="ms-error">
            Cannot reach API at localhost:8000 —
            run: poetry run uvicorn main:app --reload --port 8000
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

pending = [s for s in queue
           if f"{s['drug_key']}|{s['pt']}" not in st.session_state["submitted"]]

total = len(pending)
n_p1  = ct(pending, "P1")
n_p2  = ct(pending, "P2")
n_p3  = ct(pending, "P3")
n_p4  = ct(pending, "P4")


# ── Header + summary — one clean self-contained block ────────────────────────

st.markdown(f"""
<div class="ms-wrap">
<div class="ms-page-title">Review Queue</div>
<div class="ms-page-desc">
    Signals awaiting pharmacovigilance review, sorted by priority tier
    then statistical score. Every decision is immutably logged with a UTC timestamp.
</div>
<div class="ms-summary">
    <div class="ms-stat">
        <div class="ms-stat-label">Pending</div>
        <div class="ms-stat-value v-total">{total}</div>
    </div>
    <div class="ms-stat">
        <div class="ms-stat-label">P1 — Critical</div>
        <div class="ms-stat-value v-p1">{n_p1}</div>
    </div>
    <div class="ms-stat">
        <div class="ms-stat-label">P2 — Elevated</div>
        <div class="ms-stat-value v-p2">{n_p2}</div>
    </div>
    <div class="ms-stat">
        <div class="ms-stat-label">P3 — Moderate</div>
        <div class="ms-stat-value v-p3">{n_p3}</div>
    </div>
    <div class="ms-stat">
        <div class="ms-stat-label">P4 — Monitor</div>
        <div class="ms-stat-value v-p4">{n_p4}</div>
    </div>
</div>
</div>
""", unsafe_allow_html=True)


# ── Filter — Streamlit widget, outside any open div ──────────────────────────

fc, _ = st.columns([2, 8])
with fc:
    tf = st.selectbox(
        "Filter",
        ["All","P1","P2","P3","P4"],
        index=["All","P1","P2","P3","P4"].index(st.session_state["filter_tier"]),
        label_visibility="collapsed",
        key="tier_select",
    )
    st.session_state["filter_tier"] = tf

if tf != "All":
    pending = [s for s in pending
               if (s.get("priority") or "").upper() == tf]

if st.session_state["api_error"]:
    st.markdown(
        f'<div class="ms-error" style="margin:0 0 16px;">'
        f'API error — {st.session_state["api_error"]}</div>',
        unsafe_allow_html=True,
    )

if not pending:
    st.markdown("""
    <div class="ms-empty">
        <div class="ms-empty-title">Queue is clear</div>
        <div class="ms-empty-desc">
            All signals in this tier have been reviewed.<br>
            Run the agent pipeline to generate new safety briefs.
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ── Cards ─────────────────────────────────────────────────────────────────────

st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

for signal in pending:
    drug_key   = signal.get("drug_key", "")
    pt_val     = signal.get("pt", "")
    priority   = (signal.get("priority") or "P4").upper()
    stat_score = float(signal.get("stat_score") or 0)
    lit_score  = float(signal.get("lit_score")  or 0)
    prr        = signal.get("prr")
    case_count = signal.get("case_count") or signal.get("drug_reaction_count") or 0
    death      = int(signal.get("death_count") or 0)
    hosp       = int(signal.get("hosp_count")  or 0)
    lt         = int(signal.get("lt_count")    or 0)
    brief_text = signal.get("brief_text") or ""
    rec_action = signal.get("recommended_action") or "—"
    brief_id   = signal.get("brief_id")
    generated  = signal.get("generated_at")

    card_key    = f"{drug_key}|{pt_val}"
    is_expanded = st.session_state["expanded"].get(card_key, False)
    pclass      = pc(priority)
    sc          = sbar_color(stat_score, "stat")
    lc          = sbar_color(lit_score,  "lit")

    if brief_text:
        preview = brief_text[:300] + ("…" if len(brief_text) > 300 else "")
        disp    = brief_text if is_expanded else preview
        bhtml   = f'<div class="ms-brief">{disp}</div>'
    else:
        bhtml   = ""

    # ── TOP — fully closed HTML, no widgets inside ────────────────────────
    st.markdown(f"""
<div class="ms-card-top tier-{pclass}">
    <div class="ms-card-header">
        <div>
            <div class="ms-drug">{drug_key}</div>
            <div class="ms-pt">{pt_val}</div>
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
            <div class="ms-metric-label">Hosp.</div>
            <div class="ms-metric-value">{hosp}</div>
        </div>
        <div class="ms-metric">
            <div class="ms-metric-label">Life-Threat.</div>
            <div class="ms-metric-value">{lt}</div>
        </div>
    </div>
    <div class="ms-scores">
        <div class="ms-score-item">
            <div class="ms-score-label">StatScore</div>
            <div class="ms-score-track">
                <div class="ms-score-fill"
                     style="width:{stat_score*100:.1f}%;background:{sc};"></div>
            </div>
            <div class="ms-score-val">{fsc(stat_score)}</div>
        </div>
        <div class="ms-score-item">
            <div class="ms-score-label">LitScore</div>
            <div class="ms-score-track">
                <div class="ms-score-fill"
                     style="width:{lit_score*100:.1f}%;background:{lc};"></div>
            </div>
            <div class="ms-score-val">{fsc(lit_score)}</div>
        </div>
    </div>
    <div class="ms-outcomes">
        <div class="ms-flag {'on' if death else ''}">Death</div>
        <div class="ms-flag {'on' if lt else ''}">Life-threatening</div>
        <div class="ms-flag {'on' if hosp else ''}">Hospitalisation</div>
    </div>
    {bhtml}
</div>
""", unsafe_allow_html=True)

    # ── Expand toggle — widget after closed div ───────────────────────────
    if brief_text and len(brief_text) > 300:
        st.markdown('<div class="ms-card-mid">', unsafe_allow_html=True)
        if st.button(
            "Collapse" if is_expanded else "Read full brief",
            key=f"toggle_{card_key}",
        ):
            st.session_state["expanded"][card_key] = not is_expanded
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    # ── MID — reviewer note + buttons ────────────────────────────────────
    st.markdown('<div class="ms-card-mid">', unsafe_allow_html=True)

    nc, ac, rc, ec = st.columns([4, 1.2, 1.2, 1.2])

    with nc:
        note = st.text_area(
            "REVIEWER NOTE",
            placeholder="Optional clinical justification...",
            height=68,
            key=f"note_{card_key}",
            label_visibility="visible",
        )
    with ac:
        if st.button("Approve", key=f"approve_{card_key}",
                     use_container_width=True):
            if post_decision(drug_key, pt_val, brief_id, "APPROVE", note):
                st.session_state["submitted"][card_key] = "APPROVE"
                st.rerun()
    with rc:
        if st.button("Reject", key=f"reject_{card_key}",
                     use_container_width=True):
            if post_decision(drug_key, pt_val, brief_id, "REJECT", note):
                st.session_state["submitted"][card_key] = "REJECT"
                st.rerun()
    with ec:
        if st.button("Escalate", key=f"escalate_{card_key}",
                     use_container_width=True):
            if post_decision(drug_key, pt_val, brief_id, "ESCALATE", note):
                st.session_state["submitted"][card_key] = "ESCALATE"
                st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)

    # ── BOTTOM — timestamp ────────────────────────────────────────────────
    st.markdown(f"""
<div class="ms-card-bottom">
    <div class="ms-timestamp">Generated {fts(generated)}</div>
</div>
<div style="height:12px;"></div>
""", unsafe_allow_html=True)


# ── Decided this session ──────────────────────────────────────────────────────

if st.session_state["submitted"]:
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    with st.expander(
        f"Decided this session  ({len(st.session_state['submitted'])})",
        expanded=False,
    ):
        for key, dec in st.session_state["submitted"].items():
            drug, pt_v = key.split("|", 1)
            color = {"APPROVE":"#4ADE80","REJECT":"#F87171",
                     "ESCALATE":"#FACC15"}.get(dec,"#7B8DB0")
            st.markdown(
                f'<div style="font-family:var(--font-mono);font-size:12px;'
                f'padding:8px 0;border-bottom:1px solid var(--border);'
                f'display:flex;justify-content:space-between;">'
                f'<span style="color:var(--text-secondary);">'
                f'{drug} &times; {pt_v}</span>'
                f'<span style="color:{color};font-weight:500;">{dec}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )