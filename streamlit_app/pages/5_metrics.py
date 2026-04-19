"""
streamlit_app/pages/5_metrics.py — System Metrics Dashboard

Rendering rule: every st.markdown() block must be fully self-contained.
No div opened in one st.markdown() can be closed in another.
"""

import requests
import streamlit as st
from datetime import datetime

st.set_page_config(
    page_title="MedSignal — Metrics",
    page_icon ="⚕",
    layout    ="wide",
    initial_sidebar_state="collapsed",
)

API_BASE = "http://localhost:8000"

# ── CSS ────────────────────────────────────────────────────────────────────────
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

/* ── Centre Streamlit's own container to match ms-wrap ──────────────────── */
section[data-testid="stMain"] > div {
    padding-left: calc((100vw - 1100px) / 2) !important;
    padding-right: calc((100vw - 1100px) / 2) !important;
    max-width: 100% !important;
}

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

/* ── Page ───────────────────────────────────────────────────────────────── */
.ms-wrap {
    max-width: 1100px;
    margin: 0 auto;
    padding: 48px 0 40px;
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
    max-width: 500px;
    margin: 0 auto;
}

/* ── Summary strip ──────────────────────────────────────────────────────── */
.ms-summary {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
    margin-bottom: 12px;
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
.v-total  { color: var(--text-primary) }
.v-accent { color: var(--accent) }
.v-p3     { color: var(--p3) }
.v-p4     { color: var(--p4) }

/* ── Metric panels ──────────────────────────────────────────────────────── */
.ms-panel {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 28px 28px 24px;
    margin-bottom: 16px;
}
.ms-panel-title {
    font-family: var(--font-display);
    font-size: 17px;
    font-weight: 600;
    color: var(--text-primary);
    letter-spacing: -0.3px;
    margin-bottom: 24px;
    padding-bottom: 14px;
    border-bottom: 1px solid var(--border);
}

/* Distribution bar rows */
.ms-bar-row {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 16px;
}
.ms-bar-label {
    font-family: var(--font-mono);
    font-size: 12px;
    font-weight: 500;
    letter-spacing: 0.8px;
    text-transform: uppercase;
    color: var(--text-muted);
    width: 100px;
    flex-shrink: 0;
}
.ms-bar-track {
    flex: 1;
    height: 6px;
    background: var(--bg-hover);
    border-radius: 3px;
    overflow: hidden;
}
.ms-bar-fill { height: 100%; border-radius: 3px; }
.ms-bar-val {
    font-family: var(--font-mono);
    font-size: 13px;
    font-weight: 500;
    color: var(--text-secondary);
    width: 80px;
    text-align: right;
    flex-shrink: 0;
}
.ms-bar-dim { color: var(--text-dim); }

/* Coverage rows */
.ms-cov-row { margin-bottom: 24px; }
.ms-cov-row:last-child { margin-bottom: 0; }
.ms-cov-header {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    margin-bottom: 8px;
}
.ms-cov-name {
    font-family: var(--font-mono);
    font-size: 11px;
    font-weight: 500;
    letter-spacing: 1.2px;
    text-transform: uppercase;
    color: var(--text-muted);
}
.ms-cov-pct {
    font-family: var(--font-mono);
    font-size: 22px;
    font-weight: 500;
    color: var(--accent);
}
.ms-cov-track {
    width: 100%;
    height: 8px;
    background: var(--bg-hover);
    border-radius: 4px;
    overflow: hidden;
}
.ms-cov-fill { height: 100%; border-radius: 4px; }
.ms-cov-sub {
    font-family: var(--font-mono);
    font-size: 11px;
    color: var(--text-dim);
    margin-top: 7px;
}

/* ── Footer ─────────────────────────────────────────────────────────────── */
.ms-footer {
    padding: 24px 0 64px;
}
.ms-timestamp {
    font-family: var(--font-mono);
    font-size: 13px;
    color: var(--text-dim);
}
.ms-refresh-note {
    font-family: var(--font-mono);
    font-size: 11px;
    color: var(--text-dim);
    margin-top: 5px;
}

/* ── Error ──────────────────────────────────────────────────────────────── */
.ms-error {
    background: rgba(220,38,38,0.08);
    border: 1px solid rgba(220,38,38,0.20);
    border-radius: 10px;
    padding: 18px 24px;
    font-family: var(--font-mono);
    font-size: 13px;
    color: #F87171;
    margin-bottom: 24px;
}

::-webkit-scrollbar { width:5px; height:5px; }
::-webkit-scrollbar-track { background:var(--bg-base); }
::-webkit-scrollbar-thumb { background:var(--bg-hover); border-radius:3px; }
::-webkit-scrollbar-thumb:hover { background:var(--text-muted); }
</style>
""", unsafe_allow_html=True)


# ── Topbar ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="ms-topbar">
    <div class="ms-brand">Med<span>Signal</span></div>
    <nav class="ms-nav">
        <a class="ms-navlink" href="/signal_feed">Signal Feed</a>
        <a class="ms-navlink" href="/signal_detail">Signal Detail</a>
        <a class="ms-navlink" href="/hitl_queue">Review Queue</a>
        <a class="ms-navlink" href="/evaluation">Evaluation</a>
        <a class="ms-navlink active" href="/metrics">Metrics</a>
    </nav>
    <div class="ms-live">
        <div class="ms-live-dot"></div>
        {datetime.utcnow().strftime("%d %b %Y")}
    </div>
</div>
""", unsafe_allow_html=True)


# ── Data fetch ────────────────────────────────────────────────────────────────

def fetch_metrics():
    try:
        r = requests.get(f"{API_BASE}/metrics", timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return None
    except Exception as e:
        return {"status": "error", "detail": str(e)}


data = fetch_metrics()

if data is None:
    st.markdown("""
    <div class="ms-wrap">
        <div class="ms-error">
            Cannot reach API at localhost:8000 —
            run: poetry run uvicorn main:app --reload --port 8000
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

if data.get("status") == "error":
    st.markdown(f"""
    <div class="ms-wrap">
        <div class="ms-error">API error — {data.get("detail", "Unknown error")}</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ── Extract values ────────────────────────────────────────────────────────────

signals_total   = int(data.get("signals_flagged", 0))
briefs_total    = int(data.get("safety_briefs",   0))
decisions_total = int(data.get("hitl_decisions",  0))
queue_depth     = int(data.get("queue_depth",     0))
priority_dist   = data.get("priority_distribution", {})
decision_bdown  = data.get("decision_breakdown",     {})
ts              = data.get("timestamp", "—")

p1 = int(priority_dist.get("P1", 0))
p2 = int(priority_dist.get("P2", 0))
p3 = int(priority_dist.get("P3", 0))
p4 = int(priority_dist.get("P4", 0))
pri_total = p1 + p2 + p3 + p4 or 1

n_approve  = int(decision_bdown.get("APPROVE",  0))
n_reject   = int(decision_bdown.get("REJECT",   0))
n_escalate = int(decision_bdown.get("ESCALATE", 0))
dec_total  = n_approve + n_reject + n_escalate or 1

brief_cov  = round(briefs_total  / signals_total   * 100, 1) if signals_total   else 0.0
review_cov = round(decisions_total / briefs_total  * 100, 1) if briefs_total    else 0.0

queue_total    = queue_depth + decisions_total or 1
q_pending_pct  = round(queue_depth     / queue_total * 100, 1)
q_decided_pct  = round(decisions_total / queue_total * 100, 1)


def pct(n, total):
    return round(n / total * 100, 1) if total else 0.0


# ── Header + Summary strip ────────────────────────────────────────────────────
st.markdown(f"""
<div class="ms-wrap">
    <div class="ms-page-header">
        <div class="ms-page-title">System Metrics</div>
        <div class="ms-page-desc">
            Live aggregates from Snowflake and Redis.
        </div>
    </div>
    <div class="ms-summary">
        <div class="ms-stat">
            <div class="ms-stat-label">Signals Flagged</div>
            <div class="ms-stat-value v-total">{signals_total:,}</div>
        </div>
        <div class="ms-stat">
            <div class="ms-stat-label">Safety Briefs</div>
            <div class="ms-stat-value v-accent">{briefs_total:,}</div>
        </div>
        <div class="ms-stat">
            <div class="ms-stat-label">Pending Review</div>
            <div class="ms-stat-value v-p3">{queue_depth:,}</div>
        </div>
        <div class="ms-stat">
            <div class="ms-stat-label">Decisions Made</div>
            <div class="ms-stat-value v-p4">{decisions_total:,}</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)


# ── Row 1: Priority Distribution  |  Decision Breakdown ──────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.markdown(f"""
<div class="ms-panel">
    <div class="ms-panel-title">Priority Distribution</div>
    <div class="ms-bar-row">
        <div class="ms-bar-label">P1 — Critical</div>
        <div class="ms-bar-track">
            <div class="ms-bar-fill" style="width:{pct(p1,pri_total):.1f}%;background:#F72A2A;"></div>
        </div>
        <div class="ms-bar-val">{p1:,} <span class="ms-bar-dim">{pct(p1,pri_total):.1f}%</span></div>
    </div>
    <div class="ms-bar-row">
        <div class="ms-bar-label">P2 — Elevated</div>
        <div class="ms-bar-track">
            <div class="ms-bar-fill" style="width:{pct(p2,pri_total):.1f}%;background:#F97316;"></div>
        </div>
        <div class="ms-bar-val">{p2:,} <span class="ms-bar-dim">{pct(p2,pri_total):.1f}%</span></div>
    </div>
    <div class="ms-bar-row">
        <div class="ms-bar-label">P3 — Moderate</div>
        <div class="ms-bar-track">
            <div class="ms-bar-fill" style="width:{pct(p3,pri_total):.1f}%;background:#EAB308;"></div>
        </div>
        <div class="ms-bar-val">{p3:,} <span class="ms-bar-dim">{pct(p3,pri_total):.1f}%</span></div>
    </div>
    <div class="ms-bar-row">
        <div class="ms-bar-label">P4 — Monitor</div>
        <div class="ms-bar-track">
            <div class="ms-bar-fill" style="width:{pct(p4,pri_total):.1f}%;background:#22C55E;"></div>
        </div>
        <div class="ms-bar-val">{p4:,} <span class="ms-bar-dim">{pct(p4,pri_total):.1f}%</span></div>
    </div>
</div>
""", unsafe_allow_html=True)

with col_b:
    st.markdown(f"""
<div class="ms-panel">
    <div class="ms-panel-title">Decision Breakdown</div>
    <div class="ms-bar-row">
        <div class="ms-bar-label">Approve</div>
        <div class="ms-bar-track">
            <div class="ms-bar-fill" style="width:{pct(n_approve,dec_total):.1f}%;background:#4ADE80;"></div>
        </div>
        <div class="ms-bar-val">{n_approve:,} <span class="ms-bar-dim">{pct(n_approve,dec_total):.1f}%</span></div>
    </div>
    <div class="ms-bar-row">
        <div class="ms-bar-label">Reject</div>
        <div class="ms-bar-track">
            <div class="ms-bar-fill" style="width:{pct(n_reject,dec_total):.1f}%;background:#F87171;"></div>
        </div>
        <div class="ms-bar-val">{n_reject:,} <span class="ms-bar-dim">{pct(n_reject,dec_total):.1f}%</span></div>
    </div>
    <div class="ms-bar-row">
        <div class="ms-bar-label">Escalate</div>
        <div class="ms-bar-track">
            <div class="ms-bar-fill" style="width:{pct(n_escalate,dec_total):.1f}%;background:#FACC15;"></div>
        </div>
        <div class="ms-bar-val">{n_escalate:,} <span class="ms-bar-dim">{pct(n_escalate,dec_total):.1f}%</span></div>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Row 2: Pipeline Coverage  |  Queue Status ─────────────────────────────────
col_c, col_d = st.columns(2)

with col_c:
    st.markdown(f"""
<div class="ms-panel">
    <div class="ms-panel-title">Pipeline Coverage</div>
    <div class="ms-cov-row">
        <div class="ms-cov-header">
            <div class="ms-cov-name">Brief Coverage</div>
            <div class="ms-cov-pct">{brief_cov:.1f}%</div>
        </div>
        <div class="ms-cov-track">
            <div class="ms-cov-fill" style="width:{min(brief_cov,100):.1f}%;background:#3B82F6;"></div>
        </div>
        <div class="ms-cov-sub">{briefs_total:,} briefs generated for {signals_total:,} signals</div>
    </div>
    <div class="ms-cov-row">
        <div class="ms-cov-header">
            <div class="ms-cov-name">Review Coverage</div>
            <div class="ms-cov-pct">{review_cov:.1f}%</div>
        </div>
        <div class="ms-cov-track">
            <div class="ms-cov-fill" style="width:{min(review_cov,100):.1f}%;background:#8B5CF6;"></div>
        </div>
        <div class="ms-cov-sub">{decisions_total:,} decisions made on {briefs_total:,} briefs</div>
    </div>
</div>
""", unsafe_allow_html=True)

with col_d:
    st.markdown(f"""
<div class="ms-panel">
    <div class="ms-panel-title">Queue Status</div>
    <div class="ms-bar-row">
        <div class="ms-bar-label">Pending</div>
        <div class="ms-bar-track">
            <div class="ms-bar-fill" style="width:{q_pending_pct:.1f}%;background:#EAB308;"></div>
        </div>
        <div class="ms-bar-val">{queue_depth:,} <span class="ms-bar-dim">{q_pending_pct:.1f}%</span></div>
    </div>
    <div class="ms-bar-row">
        <div class="ms-bar-label">Decided</div>
        <div class="ms-bar-track">
            <div class="ms-bar-fill" style="width:{q_decided_pct:.1f}%;background:#22C55E;"></div>
        </div>
        <div class="ms-bar-val">{decisions_total:,} <span class="ms-bar-dim">{q_decided_pct:.1f}%</span></div>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Footer: refresh button + timestamp ───────────────────────────────────────
st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
if st.button("Refresh", key="refresh_metrics", use_container_width=False):
    st.rerun()
st.markdown(f"""
<div class="ms-footer">
    <div class="ms-timestamp">Last updated {ts}</div>
</div>
""", unsafe_allow_html=True)
