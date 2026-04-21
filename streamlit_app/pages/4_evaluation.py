"""
streamlit_app/pages/4_evaluation.py — Evaluation Dashboard

Displays detection lead time bar chart and precision-recall table
for the 10 golden drug-reaction signals.

API calls:
    GET /evaluation/summary        — header metric cards
    GET /evaluation/lead-times     — bar chart + per-signal results
    GET /evaluation/precision-recall — precision table

Rendering rule: every st.markdown() block must be fully self-contained.
No div opened in one st.markdown() can be closed in another.
"""

import requests
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")

st.set_page_config(
    page_title="MedSignal — Evaluation",
    page_icon ="⚕",
    layout    ="wide",
    initial_sidebar_state="collapsed",
)

API_BASE = os.getenv("MEDSIGNAL_API_BASE", "http://localhost:8000").strip().strip('"').strip("'").rstrip("/")


# ── CSS ───────────────────────────────────────────────────────────────────────

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
    --p4            : #22C55E;
    --p4-dim        : rgba(34,197,94,0.10);
    --p4-border     : rgba(34,197,94,0.25);
    --accent        : #3B82F6;
    --accent-dim    : rgba(59,130,246,0.15);
    --gold          : #F59E0B;
    --gold-dim      : rgba(245,158,11,0.12);
    --gold-border   : rgba(245,158,11,0.30);
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

/* ── Page header ─────────────────────────────────────────────────────────── */
.ms-page-header { text-align:center; margin-bottom:44px; }
.ms-page-title { font-family:var(--font-display); font-size:44px; font-weight:700; color:var(--text-primary); letter-spacing:-1.2px; line-height:1; margin-bottom:12px; }
.ms-page-desc { font-size:16px; color:var(--text-secondary); line-height:1.6; max-width:560px; margin:0 auto; }

/* ── KPI cards ───────────────────────────────────────────────────────────── */
.ms-kpi-grid { display:grid; grid-template-columns:repeat(5,1fr); gap:12px; margin-bottom:48px; }
.ms-kpi {
    background:var(--bg-surface); border:1px solid var(--border);
    border-radius:10px; padding:22px 16px; text-align:center;
}
.ms-kpi-label { font-family:var(--font-mono); font-size:11px; font-weight:500; letter-spacing:1.4px; text-transform:uppercase; color:var(--text-muted); margin-bottom:12px; }
.ms-kpi-value { font-family:var(--font-mono); font-size:36px; font-weight:500; line-height:1; color:var(--text-primary); }
.ms-kpi-value.accent { color:var(--accent); }
.ms-kpi-value.gold   { color:var(--gold); }
.ms-kpi-value.green  { color:var(--p4); }
.ms-kpi-sub { font-family:var(--font-mono); font-size:11px; color:var(--text-dim); margin-top:8px; }

/* ── Section titles ──────────────────────────────────────────────────────── */
.ms-section-title {
    font-family:var(--font-display); font-size:22px; font-weight:700;
    color:var(--text-primary); letter-spacing:-0.4px; margin-bottom:6px;
}
.ms-section-desc { font-size:14px; color:var(--text-muted); margin-bottom:24px; line-height:1.5; }

/* ── Precision-recall table ──────────────────────────────────────────────── */
.ms-pr-table {
    width:100%; border-collapse:collapse;
    background:var(--bg-surface); border:1px solid var(--border);
    border-radius:10px; overflow:hidden;
    font-family:var(--font-mono); font-size:13px;
}
.ms-pr-table th {
    background:var(--bg-elevated); color:var(--text-muted);
    font-size:10px; font-weight:500; letter-spacing:1.4px;
    text-transform:uppercase; padding:14px 20px; text-align:left;
    border-bottom:1px solid var(--border-strong);
}
.ms-pr-table td { padding:14px 20px; border-bottom:1px solid var(--border); color:var(--text-secondary); vertical-align:middle; }
.ms-pr-table tr:last-child td { border-bottom:none; }
.ms-pr-table tr:hover td { background:var(--bg-elevated); }
.ms-drug-name { color:var(--text-primary); font-weight:500; text-transform:capitalize; }
.ms-pt-name { color:var(--text-secondary); font-size:12px; }
.ms-flagged-yes {
    display:inline-flex; align-items:center; gap:6px;
    color:var(--p4); background:var(--p4-dim); border:1px solid var(--p4-border);
    padding:3px 10px; border-radius:4px; font-size:12px; letter-spacing:0.8px;
}
.ms-flagged-no {
    display:inline-flex; align-items:center; gap:6px;
    color:var(--text-dim); background:var(--bg-elevated); border:1px solid var(--border);
    padding:3px 10px; border-radius:4px; font-size:12px; letter-spacing:0.8px;
}
.ms-prr-val { color:var(--accent); font-weight:500; }
.ms-stat-val { color:var(--text-secondary); }
.ms-fda-label { color:var(--text-dim); font-size:11px; }

/* ── Error ───────────────────────────────────────────────────────────────── */
.ms-error {
    background:rgba(220,38,38,0.08); border:1px solid rgba(220,38,38,0.20);
    border-radius:10px; padding:18px 24px;
    font-family:var(--font-mono); font-size:13px; color:#F87171; margin-bottom:24px;
}

/* ── Plotly container ────────────────────────────────────────────────────── */

[data-testid="stPlotlyChart"] {
    background: #0E1421 !important;
    border: 1px solid rgba(255,255,255,0.06) !important;
    border-radius: 12px !important;
    padding: 24px !important;
    margin: 0 auto 48px auto !important;
}

/* ── Threshold badge ─────────────────────────────────────────────────────── */
.ms-threshold {
    display:inline-flex; align-items:center; gap:8px;
    font-family:var(--font-mono); font-size:12px;
    color:var(--text-muted); background:var(--bg-elevated);
    border:1px solid var(--border); padding:6px 14px; border-radius:6px;
    margin-bottom:32px;
}
.ms-threshold span { color:var(--accent); }
</style>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch(endpoint):
    try:
        r = requests.get(f"{API_BASE}{endpoint}", timeout=20)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return None
    except Exception as e:
        return {"error": str(e)}


def fmt_days(d):
    if d is None: return "—"
    return f"{d:,}d"


def fmt_pct(v):
    if v is None: return "—"
    return f"{float(v)*100:.0f}%"


# ── Topbar ────────────────────────────────────────────────────────────────────

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from components.topbar import render_topbar

render_topbar("Evaluation")
# ── Data fetch ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def fetch_summary():
    return fetch("/evaluation/summary")

@st.cache_data(ttl=300)
def fetch_lead_times():
    return fetch("/evaluation/lead-times")

@st.cache_data(ttl=300)
def fetch_pr():
    return fetch("/evaluation/precision-recall")

summary    = fetch_summary()
lead_times = fetch_lead_times()
pr         = fetch_pr()

if summary is None or lead_times is None or pr is None:
    st.markdown("""
<div style="max-width:1100px;margin:40px auto;padding:0 56px;">
    <div class="ms-error">
        Cannot reach API at localhost:8000 —
        run: <code>poetry run uvicorn main:app --reload --port 8000</code>
    </div>
</div>
""", unsafe_allow_html=True)
    st.stop()

if "error" in (summary or {}):
    st.markdown(f"""
<div style="max-width:1100px;margin:40px auto;padding:0 56px;">
    <div class="ms-error">API error — {summary.get("error","unknown")}</div>
</div>
""", unsafe_allow_html=True)
    st.stop()


# ── Page header ───────────────────────────────────────────────────────────────

flagged          = summary.get("flagged", 0)
total_golden     = summary.get("total_golden", 10)
precision        = summary.get("precision", 0)
median_lead      = summary.get("median_lead_time")
positive_dets    = summary.get("positive_detections", 0)
prr_threshold    = summary.get("prr_threshold", 2.0)
min_cases        = summary.get("min_cases", 50)

st.markdown(f"""
<div style="max-width:1100px;margin:0 auto;padding:48px 56px 0;">
    <div class="ms-page-header">
        <div class="ms-page-title">Evaluation Dashboard</div>
        <div class="ms-page-desc">
            Detection lead time and precision metrics for 10 golden drug-reaction
            signals with documented FDA safety communications in 2023.
        </div>
    </div>
    <div class="ms-kpi-grid">
        <div class="ms-kpi">
            <div class="ms-kpi-label">Golden Signals</div>
            <div class="ms-kpi-value">{total_golden}</div>
            <div class="ms-kpi-sub">total evaluated</div>
        </div>
        <div class="ms-kpi">
            <div class="ms-kpi-label">Detected</div>
            <div class="ms-kpi-value green">{flagged}</div>
            <div class="ms-kpi-sub">of {total_golden} flagged</div>
        </div>
        <div class="ms-kpi">
            <div class="ms-kpi-label">Precision</div>
            <div class="ms-kpi-value accent">{fmt_pct(precision)}</div>
            <div class="ms-kpi-sub">{flagged} / {total_golden}</div>
        </div>
        <div class="ms-kpi">
            <div class="ms-kpi-label">Median Lead Time</div>
            <div class="ms-kpi-value gold">{fmt_days(median_lead)}</div>
            <div class="ms-kpi-sub">before FDA comm.</div>
        </div>
        <div class="ms-kpi">
            <div class="ms-kpi-label">Early Detections</div>
            <div class="ms-kpi-value green">{positive_dets}</div>
            <div class="ms-kpi-sub">positive lead time</div>
        </div>
    </div>
    <div class="ms-threshold">
        Thresholds — PRR ≥ <span>{prr_threshold}</span> &nbsp;|&nbsp; Min cases ≥ <span>{min_cases}</span>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Bar chart — Detection Lead Time ───────────────────────────────────────────

results = lead_times.get("results", [])

st.markdown("""
<div style="max-width:1100px;margin:0 auto;padding:0 56px;">
    <div class="ms-section-title">Detection Lead Time</div>
    <div class="ms-section-desc">
        Days between MedSignal's first detection (MIN fda_dt in drug_reaction_pairs)
        and the FDA's official safety communication. Positive = detected before FDA communicated.
    </div>
</div>
""", unsafe_allow_html=True)

if results:
    # Prepare chart data
    detected_results = [r for r in results if r["lead_time_days"] is not None]
    not_detected     = [r["drug_key"].capitalize() for r in results if r["lead_time_days"] is None]

    drugs      = [r["drug_key"].capitalize() for r in detected_results]
    lead_days  = [r["lead_time_days"] for r in detected_results]
    flagged_l  = [r["flagged"] for r in detected_results]
    hover_pts  = [r["pt"].capitalize() for r in detected_results]
    hover_fda  = [r["fda_comm_label"] for r in detected_results]
    hover_date = [r["first_flagged_date"] for r in detected_results]

    bar_colors = [
        "#22C55E" if (f and d > 0) else
        "#F72A2A" if (f and d <= 0) else
        "#4A5D7A"
        for f, d in zip(flagged_l, lead_days)
    ]

    hover_texts = [
        f"<b>{drugs[i]}</b><br>"
        f"Reaction: {hover_pts[i]}<br>"
        f"Lead time: {lead_days[i]:,} days<br>"
        f"First seen: {hover_date[i]}<br>"
        f"FDA comm: {hover_fda[i]}"
        for i in range(len(drugs))
    ]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=drugs,
        y=lead_days,
        marker_color=bar_colors,
        marker_line_width=0,
        hovertemplate="%{customdata}<extra></extra>",
        customdata=hover_texts,
        width=0.45,
    ))

    fig.update_traces(
        hoverlabel_align="left",
        hovertemplate="%{customdata}<extra></extra>",
        customdata=hover_texts,
        width=0.45,
    )

    # Median line
    if median_lead:
        fig.add_hline(
            y=median_lead,
            line_dash="dot",
            line_color="#F59E0B",
            line_width=1.5,
            annotation_text=f"  Median {median_lead}d",
            annotation_font_color="#F59E0B",
            annotation_font_size=12,
            annotation_position="top right",
        )

    # Zero line
    fig.add_hline(y=0, line_color="rgba(255,255,255,0.15)", line_width=1)

    fig.update_layout(
        paper_bgcolor="#0E1421",
        plot_bgcolor ="rgba(0,0,0,0)",
        width=1000,
        height=520,
        margin=dict(l=20, r=40, t=40, b=0), 
        xaxis=dict(
            tickfont=dict(family="JetBrains Mono", size=12, color="#9BAEC8"),
            gridcolor="rgba(255,255,255,0.04)",
            showgrid=False,
            zeroline=False,
        ),
        yaxis=dict(
            title="Days before FDA communication",
            title_font=dict(family="JetBrains Mono", size=11, color="#5E7498"),
            tickfont=dict(family="JetBrains Mono", size=11, color="#5E7498"),
            gridcolor="rgba(255,255,255,0.05)",
            zeroline=False,
        ),
        hoverlabel=dict(
            bgcolor="#141C2E",
            bordercolor="rgba(255,255,255,0.10)",
            font=dict(family="Inter", size=13, color="#EEF2FF"),
            namelength=-1,  
        ),
        showlegend=False,
        annotations=[                         
            dict(
                x=0.00, y=1.06, xref="paper", yref="paper",
                text="<b style='color:#22C55E'>■</b> Detected before FDA  &nbsp;&nbsp;"
                    "<b style='color:#F72A2A'>■</b> Detected after FDA  &nbsp;&nbsp;"
                    "<b style='color:#4A5D7A'>■</b> Not detected",
                showarrow=False,
                font=dict(family="JetBrains Mono", size=11, color="#9BAEC8"),
                align="left",
            )
        ],
        hovermode="closest",
    )

    # Chart in styled container
    _, col, _ = st.columns([0.5, 9, 0.5])
    with col:
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    if not_detected:
        st.markdown(f"""
            <div style="max-width:988px;margin:-32px auto 48px;padding:0 0 0 24px;">
            <div style="font-family:'JetBrains Mono',monospace;font-size:12px;color:#5E7498;">
            Not detected (excluded from chart): {", ".join(not_detected)}
            </div>
            </div>
        """, unsafe_allow_html=True)

# ── Precision-Recall table ────────────────────────────────────────────────────

breakdown = pr.get("breakdown", [])

st.markdown(f"""
<div style="max-width:1100px;margin:0 auto;padding:0 56px 80px;">
    <div class="ms-section-title">Precision — Golden Signal Detection</div>
    <div class="ms-section-desc">
        {flagged} of {total_golden} golden signals correctly flagged above
        PRR ≥ {prr_threshold} with ≥ {min_cases} cases.
        Precision = {fmt_pct(precision)}.
    </div>
    <table class="ms-pr-table">
        <thead>
            <tr>
                <th>Drug</th>
                <th>Reaction (PT)</th>
                <th>Status</th>
                <th>PRR</th>
                <th>Cases</th>
                <th>Stat Score</th>
                <th>FDA Communication</th>
            </tr>
        </thead>
        <tbody>
            {"".join([
                f'''<tr>
                    <td class="ms-drug-name">{r["drug_key"].capitalize()}</td>
                    <td class="ms-pt-name">{r["pt"].capitalize()}</td>
                    <td>
                        {"<span class='ms-flagged-yes'>✓ Detected</span>"
                         if r["flagged"]
                         else "<span class='ms-flagged-no'>✗ Not detected</span>"}
                    </td>
                    <td class="ms-prr-val">{f"{r['prr']:.2f}" if r["prr"] else "—"}</td>
                    <td>{f"{r['drug_reaction_count']:,}" if r["drug_reaction_count"] else "—"}</td>
                    <td class="ms-stat-val">{f"{r['stat_score']:.3f}" if r["stat_score"] else "—"}</td>
                    <td class="ms-fda-label">{r["fda_comm_label"]}</td>
                </tr>'''
                for r in breakdown
            ])}
        </tbody>
    </table>
</div>
""", unsafe_allow_html=True)