"""
streamlit_app/pages/1_signal_feed.py — Signal Feed

Rendering rule: every st.markdown() block must be fully self-contained.
No div opened in one st.markdown() can be closed in another.

Alignment fix: Streamlit native widget rows (st.columns) are wrapped
in a padding shim so they align with the ms-wrap container (56px sides).
"""

import requests
import streamlit as st
from datetime import datetime

st.set_page_config(
    page_title="MedSignal — Signal Feed",
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
    --font-display  : 'Syne', sans-serif;
    --font-mono     : 'JetBrains Mono', monospace;
    --font-body     : 'Inter', sans-serif;
    --wrap-pad      : 56px;   /* must match ms-wrap padding sides */
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
[data-testid="stSidebar"]      { display: none !important; }
.block-container { padding: 0 !important; max-width: 100% !important; }

/* ── Topbar ─────────────────────────────────────────────────────────────── */
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
.ms-navlink:hover  { background: var(--bg-elevated); color: var(--text-primary); }
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

/* ── Page ───────────────────────────────────────────────────────────────── */
.ms-wrap {
    max-width: 1100px;
    margin: 0 auto;
    padding: 48px var(--wrap-pad) 80px;
}


/* ── Streamlit widget styling ───────────────────────────────────────────── */
.stTextInput label, .stSelectbox label {
    font-family: var(--font-mono) !important;
    font-size: 10px !important;
    font-weight: 500 !important;
    letter-spacing: 1.5px !important;
    text-transform: uppercase !important;
    color: var(--text-muted) !important;
}
.stTextInput input {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-strong) !important;
    border-radius: 7px !important;
    color: var(--text-primary) !important;
    font-family: var(--font-mono) !important;
    font-size: 13px !important;
}
.stTextInput input:focus {
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 1px var(--accent-dim) !important;
}
.stSelectbox > div > div {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-strong) !important;
    border-radius: 7px !important;
    color: var(--text-primary) !important;
    font-family: var(--font-mono) !important;
    font-size: 13px !important;
}

/* ── Page header ────────────────────────────────────────────────────────── */
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

/* ── Summary strip ──────────────────────────────────────────────────────── */
.ms-summary {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 12px;
    margin-bottom: 36px;
}
.ms-stat {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px 16px;
    text-align: center;
}
.ms-stat-label {
    font-family: var(--font-mono);
    font-size: 11px;
    font-weight: 500;
    letter-spacing: 1.4px;
    text-transform: uppercase;
    color: var(--text-muted);
    margin-bottom: 12px;
}
.ms-stat-value {
    font-family: var(--font-mono);
    font-size: 38px;
    font-weight: 500;
    line-height: 1;
}
.v-total { color: var(--text-primary) }
.v-p1    { color: var(--p1) }
.v-p2    { color: var(--p2) }
.v-p3    { color: var(--p3) }
.v-p4    { color: var(--p4) }

/* ── Count label ────────────────────────────────────────────────────────── */
.ms-count-label {
    font-family: var(--font-mono);
    font-size: 11px;
    letter-spacing: 1.2px;
    text-transform: uppercase;
    color: var(--text-muted);
    padding: 12px 0 20px;
}

/* ── Card sections ──────────────────────────────────────────────────────── */
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
    content: '';
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 4px;
}
.ms-card-top.tier-p1::before { background: var(--p1) }
.ms-card-top.tier-p2::before { background: var(--p2) }
.ms-card-top.tier-p3::before { background: var(--p3) }
.ms-card-top.tier-p4::before { background: var(--p4) }

.ms-card-bottom {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-top: 1px solid var(--border);
    border-radius: 0 0 12px 12px;
    padding: 14px 32px 18px;
    margin-bottom: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
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
.ms-card-badges  { display: flex; align-items: center; gap: 10px; }
.ms-priority {
    font-family: var(--font-mono);
    font-size: 13px;
    font-weight: 500;
    letter-spacing: 0.8px;
    padding: 6px 14px;
    border-radius: 6px;
    border: 1px solid;
}
.ms-priority.p1 { color:var(--p1); background:var(--p1-dim); border-color:var(--p1-border) }
.ms-priority.p2 { color:var(--p2); background:var(--p2-dim); border-color:var(--p2-border) }
.ms-priority.p3 { color:var(--p3); background:var(--p3-dim); border-color:var(--p3-border) }
.ms-priority.p4 { color:var(--p4); background:var(--p4-dim); border-color:var(--p4-border) }

/* Metrics row */
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
    padding: 14px 16px;
    border-right: 1px solid var(--border);
    text-align: center;
}
.ms-metric:last-child { border-right: none; }
.ms-metric-label {
    font-family: var(--font-mono);
    font-size: 10px;
    font-weight: 500;
    letter-spacing: 1.2px;
    text-transform: uppercase;
    color: var(--text-muted);
    margin-bottom: 8px;
}
.ms-metric-value {
    font-family: var(--font-mono);
    font-size: 22px;
    font-weight: 500;
    color: var(--text-primary);
}
.ms-metric-value.hl { color: var(--accent); }

/* Score bars */
.ms-scores {
    display: flex;
    gap: 20px;
    margin-bottom: 20px;
}
.ms-score-item {
    flex: 1;
    display: flex;
    align-items: center;
    gap: 12px;
}
.ms-score-label {
    font-family: var(--font-mono);
    font-size: 10px;
    letter-spacing: 1px;
    text-transform: uppercase;
    color: var(--text-muted);
    white-space: nowrap;
    width: 72px;
    flex-shrink: 0;
}
.ms-score-track {
    flex: 1;
    height: 4px;
    background: var(--bg-elevated);
    border-radius: 2px;
    overflow: hidden;
}
.ms-score-fill { height: 100%; border-radius: 2px; }
.ms-score-val {
    font-family: var(--font-mono);
    font-size: 12px;
    color: var(--text-secondary);
    width: 36px;
    text-align: right;
    flex-shrink: 0;
}

/* Outcome flags */
.ms-outcomes { display: flex; gap: 8px; flex-wrap: wrap; }
.ms-flag {
    font-family: var(--font-mono);
    font-size: 11px;
    font-weight: 500;
    letter-spacing: 0.8px;
    text-transform: uppercase;
    padding: 5px 12px;
    border-radius: 5px;
    border: 1px solid var(--border);
    color: var(--text-dim);
    background: transparent;
}
.ms-flag.on {
    color: var(--p2);
    background: var(--p2-dim);
    border-color: var(--p2-border);
}

/* Timestamp */
.ms-timestamp {
    font-family: var(--font-mono);
    font-size: 11px;
    color: var(--text-dim);
}

/* View detail link */
.ms-view-detail {
    font-family: var(--font-mono);
    font-size: 12px;
    font-weight: 500;
    letter-spacing: 0.5px;
    color: var(--text-secondary);
    text-decoration: none;
    padding: 6px 14px;
    border: 1px solid var(--border-strong);
    border-radius: 6px;
    transition: background 0.12s, color 0.12s;
}
.ms-view-detail:hover {
    background: var(--bg-elevated);
    color: var(--text-primary);
}

/* Empty / error */
.ms-empty { text-align: center; padding: 80px 40px; }
.ms-empty-title {
    font-family: var(--font-display);
    font-size: 26px; color: var(--text-secondary);
    margin-bottom: 12px; letter-spacing: -0.3px;
}
.ms-empty-desc { font-size: 15px; color: var(--text-muted); line-height: 1.6; }
.ms-error {
    background: rgba(220,38,38,0.08);
    border: 1px solid rgba(220,38,38,0.20);
    border-radius: 10px; padding: 18px 24px;
    font-family: var(--font-mono);
    font-size: 13px; color: #F87171; margin-bottom: 24px;
}

::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: var(--bg-base); }
::-webkit-scrollbar-thumb { background: var(--bg-hover); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_signals():
    try:
        r = requests.get(f"{API_BASE}/signals", timeout=60)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return None
    except Exception as e:
        st.session_state["api_error"] = str(e)
        return []

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
    # NULL priority defaults to P4 (no safety brief generated yet),
    # matching the pc() display logic in cards.
    return sum(1 for s in sigs
               if ((s.get("priority") or "P4").upper() == tier))


# ── Session state ─────────────────────────────────────────────────────────────

for k, v in [("api_error", None), ("search_q", ""),
              ("filter_pri", "All"), ("sort_by", "Priority")]:
    if k not in st.session_state:
        st.session_state[k] = v


# ── Topbar ────────────────────────────────────────────────────────────────────
# Topbar is full-width — intentionally outside ms-wrap, padding set in CSS.

st.markdown(f"""
<div class="ms-topbar">
    <div class="ms-brand">Med<span>Signal</span></div>
    <nav class="ms-nav">
        <a class="ms-navlink active" href="/signal_feed">Signal Feed</a>
        <a class="ms-navlink" href="/signal_detail">Signal Detail</a>
        <a class="ms-navlink" href="/hitl_queue">Review Queue</a>
        <a class="ms-navlink" href="/evaluation">Evaluation</a>
    </nav>
    <div class="ms-live">
        <div class="ms-live-dot"></div>
        {datetime.utcnow().strftime("%d %b %Y")}
    </div>
</div>
""", unsafe_allow_html=True)


# ── Data ──────────────────────────────────────────────────────────────────────

signals = fetch_signals()

if signals is None:
    st.markdown("""
    <div class="ms-wrap">
        <div class="ms-error">
            Cannot reach API at localhost:8000 —
            run: poetry run uvicorn main:app --reload --port 8000
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

n_p1 = ct(signals, "P1")
n_p2 = ct(signals, "P2")
n_p3 = ct(signals, "P3")
n_p4 = ct(signals, "P4")


# ── Header + summary ──────────────────────────────────────────────────────────

st.markdown(f"""
<div class="ms-wrap">
    <div class="ms-page-header">
        <div class="ms-page-title">Signal Feed</div>
        <div class="ms-page-desc">
            All flagged drug-reaction signals ranked by priority tier.
            PRR, StatScore, LitScore and outcome severity visible at a glance.
        </div>
    </div>
    <div class="ms-summary">
        <div class="ms-stat">
            <div class="ms-stat-label">Total Signals</div>
            <div class="ms-stat-value v-total">{len(signals)}</div>
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


# ── Filter row ────────────────────────────────────────────────────────────────
# Streamlit native widgets (required for rerun on change).
# CSS below constrains the horizontal block to ms-wrap width + padding.

search_q   = st.session_state.get("search_q",   "")
filter_pri = st.session_state.get("filter_pri",  "All")
sort_by    = st.session_state.get("sort_by",     "Priority")

# ── Filter row — padding enforced via surrounding ms-wrap divs ───────────────
# Opening half of ms-wrap: sets left offset. Streamlit columns render next.
# Closing half follows. This is the only reliable way to inset st.columns.
st.markdown("""
<style>
/* Constrain ONLY the filter row horizontal block, not cards */
div[data-testid="stHorizontalBlock"]:has(div[data-testid="stTextInputRootElement"]) {
    max-width: 988px !important;      /* 1100 - 2×56 */
    margin-left: auto !important;
    margin-right: auto !important;
}
</style>
""", unsafe_allow_html=True)

col_s, col_p, col_o = st.columns([4, 2, 2])
with col_s:
    new_search = st.text_input("SEARCH", value=search_q,
                               placeholder="Drug name or reaction...",
                               key="search_input", label_visibility="visible")
    st.session_state["search_q"] = new_search
    search_q = new_search

with col_p:
    pri_opts = ["All", "P1", "P2", "P3", "P4"]
    new_pri = st.selectbox("PRIORITY", pri_opts,
                           index=pri_opts.index(filter_pri),
                           key="priority_select", label_visibility="visible")
    st.session_state["filter_pri"] = new_pri
    filter_pri = new_pri

with col_o:
    sort_opts = ["Priority", "PRR", "Cases", "Deaths"]
    new_sort = st.selectbox("SORT BY", sort_opts,
                            index=sort_opts.index(sort_by),
                            key="sort_select", label_visibility="visible")
    st.session_state["sort_by"] = new_sort
    sort_by = new_sort


# ── Filter + sort ─────────────────────────────────────────────────────────────

filtered = signals[:]

if search_q:
    q = search_q.lower()
    filtered = [s for s in filtered
                if q in (s.get("drug_key") or "").lower()
                or q in (s.get("pt") or "").lower()]

if filter_pri != "All":
    filtered = [s for s in filtered
                if (s.get("priority") or "P4").upper() == filter_pri]

sort_key_map = {
    "Priority": lambda s: {"P1":0,"P2":1,"P3":2,"P4":3}.get(
        (s.get("priority") or "P4").upper(), 3),  # NULL → P4 → 3
    "PRR"     : lambda s: -float(s.get("prr") or 0),
    "Cases"   : lambda s: -int(s.get("drug_reaction_count") or 0),
    "Deaths"  : lambda s: -int(s.get("death_count") or 0),
}
if sort_by in sort_key_map:
    filtered.sort(key=sort_key_map[sort_by])

st.markdown(
    f'<div class="ms-wrap" style="padding-top:0;padding-bottom:0;">'
    f'<div class="ms-count-label">Showing {len(filtered)} of {len(signals)} signals</div>'
    f'</div>',
    unsafe_allow_html=True,
)

if st.session_state.get("api_error"):
    st.markdown(
        f'<div class="ms-wrap" style="padding-top:0;">'
        f'<div class="ms-error">API error — {st.session_state["api_error"]}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

if not filtered:
    st.markdown("""
    <div class="ms-empty">
        <div class="ms-empty-title">No signals match</div>
        <div class="ms-empty-desc">Try adjusting the search or priority filter.</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ── Cards ─────────────────────────────────────────────────────────────────────

for signal in filtered:
    drug_key   = signal.get("drug_key", "")
    pt_val     = signal.get("pt", "")
    priority   = (signal.get("priority") or "P4").upper()
    stat_score = float(signal.get("stat_score") or 0)
    lit_score  = float(signal.get("lit_score")  or 0)
    prr        = signal.get("prr")
    case_count = signal.get("drug_reaction_count") or 0
    death      = int(signal.get("death_count") or 0)
    hosp       = int(signal.get("hosp_count")  or 0)
    lt         = int(signal.get("lt_count")    or 0)
    computed   = signal.get("computed_at")

    pclass = pc(priority)
    sc     = sbar_color(stat_score, "stat")
    lc     = sbar_color(lit_score,  "lit")

    death_badge = f'<span style="color:var(--p1);font-weight:600;">{death}</span>' if death else str(death)
    hosp_badge  = f'<span style="color:var(--p2);font-weight:600;">{hosp}</span>'  if hosp  else str(hosp)
    lt_badge    = f'<span style="color:var(--p3);font-weight:600;">{lt}</span>'    if lt    else str(lt)

    st.markdown(f"""
<div class="ms-wrap" style="padding-top:0;padding-bottom:0;">
<div class="ms-card-top tier-{pclass}">
    <div class="ms-card-header">
        <div>
            <div class="ms-drug">{drug_key}</div>
            <div class="ms-pt">{pt_val}</div>
        </div>
        <div class="ms-card-badges">
            <div class="ms-priority {pclass}">{priority}</div>
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
            <div class="ms-metric-value">{death_badge}</div>
        </div>
        <div class="ms-metric">
            <div class="ms-metric-label">Hosp.</div>
            <div class="ms-metric-value">{hosp_badge}</div>
        </div>
        <div class="ms-metric">
            <div class="ms-metric-label">Life-Threat.</div>
            <div class="ms-metric-value">{lt_badge}</div>
        </div>
    </div>
    <div class="ms-scores">
        <div class="ms-score-item">
            <div class="ms-score-label">Stat Score</div>
            <div class="ms-score-track">
                <div class="ms-score-fill"
                     style="width:{stat_score*100:.1f}%;background:{sc};"></div>
            </div>
            <div class="ms-score-val">{fsc(stat_score)}</div>
        </div>
        <div class="ms-score-item">
            <div class="ms-score-label">Lit Score</div>
            <div class="ms-score-track">
                <div class="ms-score-fill"
                     style="width:{lit_score*100:.1f}%;background:{lc};"></div>
            </div>
            <div class="ms-score-val">{fsc(lit_score)}</div>
        </div>
    </div>
    <div class="ms-outcomes">
        <div class="ms-flag {'on' if death else ''}">{'&#9679; ' if death else ''}{death} Deaths</div>
        <div class="ms-flag {'on' if hosp else ''}">{'&#9679; ' if hosp else ''}{hosp} Hospitalisation</div>
        <div class="ms-flag {'on' if lt else ''}">{'&#9679; ' if lt else ''}{lt} Life-threatening</div>
    </div>
</div>
<div class="ms-card-bottom">
    <div class="ms-timestamp">Computed {fts(computed)}</div>
    <a class="ms-view-detail"
       href="/signal_detail?drug={drug_key}&pt={pt_val}">
        View Detail →
    </a>
</div>
<div style="height:16px;"></div>
</div>
""", unsafe_allow_html=True)