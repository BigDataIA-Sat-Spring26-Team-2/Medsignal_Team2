# """
# streamlit_app/pages/2_signal_detail.py — Signal Detail Page

# Shows full SafetyBrief for a selected signal including:
#     - Drug + reaction header with priority tier
#     - StatScore and LitScore with visual bars
#     - Outcome severity flags
#     - Full SafetyBrief text
#     - Key findings list
#     - Cited PMIDs with PubMed links
#     - Recommended action
#     - Investigate button — triggers on-demand pipeline via FastAPI


# """

# import requests
# import streamlit as st
# from datetime import datetime

# st.set_page_config(
#     page_title="MedSignal — Signal Detail",
#     page_icon ="⚕",
#     layout    ="wide",
#     initial_sidebar_state="collapsed",
# )

# API_BASE = "http://localhost:8000"

# st.markdown("""
# <style>
# @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&family=Inter:wght@300;400;500;600&display=swap');

# :root {
#     --bg-base       : #080C14;
#     --bg-surface    : #0E1421;
#     --bg-elevated   : #141C2E;
#     --bg-hover      : #1A2238;
#     --border        : rgba(255,255,255,0.06);
#     --border-strong : rgba(255,255,255,0.10);
#     --text-primary  : #EEF2FF;
#     --text-secondary: #A0AECA;
#     --text-muted    : #5A6E8E;
#     --text-dim      : #2A3850;
#     --p1            : #F72A2A;
#     --p1-dim        : rgba(247,42,42,0.12);
#     --p1-border     : rgba(247,42,42,0.30);
#     --p2            : #F97316;
#     --p2-dim        : rgba(249,115,22,0.12);
#     --p2-border     : rgba(249,115,22,0.30);
#     --p3            : #EAB308;
#     --p3-dim        : rgba(234,179,8,0.12);
#     --p3-border     : rgba(234,179,8,0.30);
#     --p4            : #22C55E;
#     --p4-dim        : rgba(34,197,94,0.10);
#     --p4-border     : rgba(34,197,94,0.25);
#     --accent        : #3B82F6;
#     --accent-dim    : rgba(59,130,246,0.15);
#     --accent-bright : #60A5FA;
#     --font-display  : 'Syne', sans-serif;
#     --font-mono     : 'JetBrains Mono', monospace;
#     --font-body     : 'Inter', sans-serif;
# }

# *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

# html, body, .stApp {
#     background: var(--bg-base) !important;
#     color: var(--text-primary) !important;
#     font-family: var(--font-body) !important;
#     -webkit-font-smoothing: antialiased;
# }

# #MainMenu, footer, header,
# [data-testid="stToolbar"],
# [data-testid="stDecoration"],
# [data-testid="stStatusWidget"] { display: none !important; }
# [data-testid="stSidebar"] { display: none !important; }
# .block-container { padding: 0 !important; max-width: 100% !important; }

# /* ── Topbar ── */
# .ms-topbar {
#     display: flex;
#     align-items: center;
#     justify-content: space-between;
#     height: 56px;
#     padding: 0 36px;
#     background: var(--bg-surface);
#     border-bottom: 1px solid var(--border);
#     position: sticky;
#     top: 0;
#     z-index: 200;
# }
# .ms-brand {
#     font-family: var(--font-display);
#     font-size: 17px;
#     font-weight: 700;
#     letter-spacing: -0.3px;
#     color: var(--text-primary);
# }
# .ms-brand span { color: var(--accent); }
# .ms-nav { display: flex; gap: 2px; }
# .ms-navlink {
#     font-family: var(--font-body);
#     font-size: 12px;
#     font-weight: 500;
#     color: var(--text-secondary);
#     text-decoration: none;
#     padding: 6px 14px;
#     border-radius: 6px;
#     transition: background 0.12s, color 0.12s;
# }
# .ms-navlink:hover  { background: var(--bg-elevated); color: var(--text-primary); }
# .ms-navlink.active { background: var(--bg-elevated); color: var(--text-primary); }
# .ms-topbar-right { display: flex; align-items: center; }
# .ms-live {
#     display: flex; align-items: center; gap: 7px;
#     font-family: var(--font-mono); font-size: 11px;
#     color: var(--text-muted);
# }
# .ms-live-dot {
#     width: 5px; height: 5px; border-radius: 50%;
#     background: var(--accent);
#     animation: blink 2.5s ease-in-out infinite;
# }
# @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.25} }

# /* ── Layout ── */
# .ms-wrap { padding: 36px 40px 60px; max-width: 1440px; margin: 0 auto; }

# /* ── Selector bar ── */
# .ms-selector-bar {
#     display: flex;
#     align-items: center;
#     gap: 16px;
#     margin-bottom: 32px;
#     padding: 16px 20px;
#     background: var(--bg-surface);
#     border: 1px solid var(--border);
#     border-radius: 10px;
# }
# .ms-selector-label {
#     font-family: var(--font-mono);
#     font-size: 10px;
#     font-weight: 500;
#     letter-spacing: 1.8px;
#     text-transform: uppercase;
#     color: var(--text-muted);
#     white-space: nowrap;
# }

# /* ── Hero header ── */
# .ms-hero {
#     background: var(--bg-surface);
#     border: 1px solid var(--border);
#     border-radius: 12px;
#     padding: 28px 32px;
#     margin-bottom: 20px;
#     position: relative;
#     overflow: hidden;
# }
# .ms-hero::before {
#     content: '';
#     position: absolute;
#     left: 0; top: 0; bottom: 0;
#     width: 4px;
#     border-radius: 12px 0 0 12px;
# }
# .ms-hero.tier-p1::before { background: var(--p1); }
# .ms-hero.tier-p2::before { background: var(--p2); }
# .ms-hero.tier-p3::before { background: var(--p3); }
# .ms-hero.tier-p4::before { background: var(--p4); }

# .ms-hero-top {
#     display: flex;
#     align-items: flex-start;
#     justify-content: space-between;
#     margin-bottom: 24px;
# }
# .ms-hero-left {}
# .ms-drug-name {
#     font-family: var(--font-display);
#     font-size: 28px;
#     font-weight: 700;
#     color: var(--text-primary);
#     letter-spacing: -0.5px;
#     text-transform: capitalize;
#     line-height: 1;
#     margin-bottom: 6px;
# }
# .ms-reaction {
#     font-size: 15px;
#     color: var(--text-secondary);
#     font-weight: 400;
#     text-transform: capitalize;
# }
# .ms-hero-badges {
#     display: flex;
#     align-items: center;
#     gap: 10px;
# }
# .ms-priority-badge {
#     font-family: var(--font-mono);
#     font-size: 13px;
#     font-weight: 600;
#     letter-spacing: 1.5px;
#     padding: 6px 14px;
#     border-radius: 6px;
#     border: 1px solid;
# }
# .ms-priority-badge.p1 { color:var(--p1); background:var(--p1-dim); border-color:var(--p1-border); }
# .ms-priority-badge.p2 { color:var(--p2); background:var(--p2-dim); border-color:var(--p2-border); }
# .ms-priority-badge.p3 { color:var(--p3); background:var(--p3-dim); border-color:var(--p3-border); }
# .ms-priority-badge.p4 { color:var(--p4); background:var(--p4-dim); border-color:var(--p4-border); }

# .ms-action-pill {
#     font-family: var(--font-mono);
#     font-size: 10px;
#     font-weight: 500;
#     letter-spacing: 1.2px;
#     text-transform: uppercase;
#     color: var(--text-secondary);
#     background: var(--bg-elevated);
#     border: 1px solid var(--border-strong);
#     padding: 6px 12px;
#     border-radius: 6px;
# }

# /* ── Key metrics row ── */
# .ms-kpi-row {
#     display: flex;
#     gap: 32px;
#     padding-top: 20px;
#     border-top: 1px solid var(--border);
# }
# .ms-kpi { display: flex; flex-direction: column; gap: 4px; }
# .ms-kpi-label {
#     font-family: var(--font-mono);
#     font-size: 9px; font-weight: 500;
#     letter-spacing: 1.8px; text-transform: uppercase;
#     color: var(--text-muted);
# }
# .ms-kpi-value {
#     font-family: var(--font-mono);
#     font-size: 22px; font-weight: 500;
#     color: var(--text-primary); line-height: 1;
# }
# .ms-kpi-value.accent { color: var(--accent-bright); }
# .ms-kpi-sep {
#     width: 1px;
#     background: var(--border);
#     align-self: stretch;
#     margin: 0 8px;
# }

# /* ── Two column layout ── */
# .ms-grid {
#     display: grid;
#     grid-template-columns: 1fr 360px;
#     gap: 20px;
#     align-items: start;
# }

# /* ── Section card ── */
# .ms-section {
#     background: var(--bg-surface);
#     border: 1px solid var(--border);
#     border-radius: 10px;
#     padding: 24px 26px;
#     margin-bottom: 20px;
# }
# .ms-section-title {
#     font-family: var(--font-mono);
#     font-size: 10px; font-weight: 500;
#     letter-spacing: 2px; text-transform: uppercase;
#     color: var(--text-muted);
#     margin-bottom: 18px;
#     padding-bottom: 12px;
#     border-bottom: 1px solid var(--border);
# }

# /* ── Brief text ── */
# .ms-brief-text {
#     font-size: 14px;
#     color: var(--text-secondary);
#     line-height: 1.75;
# }
# .ms-brief-text p { margin-bottom: 14px; }
# .ms-brief-text p:last-child { margin-bottom: 0; }

# /* ── Key findings ── */
# .ms-finding {
#     display: flex;
#     gap: 12px;
#     align-items: flex-start;
#     padding: 12px 0;
#     border-bottom: 1px solid var(--border);
# }
# .ms-finding:last-child { border-bottom: none; padding-bottom: 0; }
# .ms-finding-num {
#     font-family: var(--font-mono);
#     font-size: 10px; font-weight: 500;
#     color: var(--accent);
#     background: var(--accent-dim);
#     border: 1px solid rgba(59,130,246,0.20);
#     width: 22px; height: 22px;
#     border-radius: 4px;
#     display: flex; align-items: center; justify-content: center;
#     flex-shrink: 0; margin-top: 1px;
# }
# .ms-finding-text {
#     font-size: 13px;
#     color: var(--text-secondary);
#     line-height: 1.6;
# }

# /* ── Scores ── */
# .ms-score-row {
#     display: flex; align-items: center; gap: 12px;
#     margin-bottom: 14px;
# }
# .ms-score-row:last-child { margin-bottom: 0; }
# .ms-score-name {
#     font-family: var(--font-mono);
#     font-size: 10px; font-weight: 500;
#     letter-spacing: 1px; text-transform: uppercase;
#     color: var(--text-secondary);
#     width: 72px; flex-shrink: 0;
# }
# .ms-score-bar {
#     flex: 1; height: 4px;
#     background: var(--bg-elevated);
#     border-radius: 3px; overflow: hidden;
# }
# .ms-score-bar-fill { height: 100%; border-radius: 3px; }
# .ms-score-number {
#     font-family: var(--font-mono);
#     font-size: 13px; font-weight: 500;
#     color: var(--text-primary);
#     width: 40px; text-align: right; flex-shrink: 0;
# }

# /* ── Outcome flags ── */
# .ms-flags { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 20px; }
# .ms-flag {
#     font-family: var(--font-mono);
#     font-size: 10px; font-weight: 500;
#     letter-spacing: 1px; text-transform: uppercase;
#     padding: 5px 10px; border-radius: 5px;
#     border: 1px solid var(--border);
#     color: var(--text-muted);
#     background: var(--bg-elevated);
# }
# .ms-flag.on {
#     color: var(--p1);
#     border-color: var(--p1-border);
#     background: var(--p1-dim);
# }

# /* ── PMIDs ── */
# .ms-pmid-list { display: flex; flex-direction: column; gap: 8px; }
# .ms-pmid-item {
#     display: flex; align-items: center;
#     justify-content: space-between;
#     padding: 10px 14px;
#     background: var(--bg-elevated);
#     border: 1px solid var(--border);
#     border-radius: 6px;
#     text-decoration: none;
#     transition: border-color 0.12s, background 0.12s;
# }
# .ms-pmid-item:hover {
#     border-color: var(--accent);
#     background: var(--accent-dim);
# }
# .ms-pmid-label {
#     font-family: var(--font-mono);
#     font-size: 12px; font-weight: 500;
#     color: var(--accent-bright);
# }
# .ms-pmid-arrow {
#     font-size: 11px;
#     color: var(--text-muted);
# }
# .ms-no-pmids {
#     font-family: var(--font-mono);
#     font-size: 12px;
#     color: var(--text-muted);
#     padding: 12px 0;
#     text-align: center;
# }

# /* ── Recommended action ── */
# .ms-rec-action {
#     display: flex;
#     align-items: center;
#     gap: 14px;
#     padding: 16px 18px;
#     background: var(--bg-elevated);
#     border: 1px solid var(--border-strong);
#     border-radius: 8px;
# }
# .ms-rec-icon { font-size: 20px; }
# .ms-rec-label {
#     font-family: var(--font-mono);
#     font-size: 9px; font-weight: 500;
#     letter-spacing: 1.8px; text-transform: uppercase;
#     color: var(--text-muted);
#     margin-bottom: 4px;
# }
# .ms-rec-value {
#     font-family: var(--font-mono);
#     font-size: 14px; font-weight: 600;
#     color: var(--text-primary);
#     letter-spacing: 0.5px;
# }
# .ms-rec-value.MONITOR      { color: #60A5FA; }
# .ms-rec-value.LABEL_UPDATE { color: #FACC15; }
# .ms-rec-value.RESTRICT     { color: #FB923C; }
# .ms-rec-value.WITHDRAW     { color: #F87171; }

# /* ── Meta info ── */
# .ms-meta-row {
#     display: flex; justify-content: space-between;
#     align-items: center;
#     padding: 8px 0;
#     border-bottom: 1px solid var(--border);
# }
# .ms-meta-row:last-child { border-bottom: none; }
# .ms-meta-key {
#     font-family: var(--font-mono);
#     font-size: 10px; font-weight: 500;
#     letter-spacing: 1px; text-transform: uppercase;
#     color: var(--text-muted);
# }
# .ms-meta-val {
#     font-family: var(--font-mono);
#     font-size: 11px;
#     color: var(--text-secondary);
# }

# /* ── No brief state ── */
# .ms-no-brief {
#     text-align: center;
#     padding: 60px 40px;
#     background: var(--bg-surface);
#     border: 1px solid var(--border);
#     border-radius: 10px;
# }
# .ms-no-brief-title {
#     font-family: var(--font-display);
#     font-size: 20px; color: var(--text-secondary);
#     margin-bottom: 10px; letter-spacing: -0.3px;
# }
# .ms-no-brief-desc {
#     font-size: 13px; color: var(--text-muted);
#     line-height: 1.6; margin-bottom: 24px;
# }

# /* ── Streamlit overrides ── */
# .stSelectbox > div > div {
#     background: var(--bg-elevated) !important;
#     border: 1px solid var(--border-strong) !important;
#     border-radius: 6px !important;
#     color: var(--text-primary) !important;
#     font-family: var(--font-mono) !important;
#     font-size: 12px !important;
# }
# .stSelectbox label {
#     font-family: var(--font-mono) !important;
#     font-size: 10px !important;
#     font-weight: 500 !important;
#     letter-spacing: 1.5px !important;
#     text-transform: uppercase !important;
#     color: var(--text-muted) !important;
# }
# [data-testid="stButton"] button {
#     font-family: var(--font-mono) !important;
#     font-size: 11px !important;
#     font-weight: 500 !important;
#     letter-spacing: 1.2px !important;
#     text-transform: uppercase !important;
#     padding: 10px 20px !important;
#     border-radius: 6px !important;
#     border: 1px solid var(--accent) !important;
#     background: var(--accent-dim) !important;
#     color: var(--accent-bright) !important;
#     transition: all 0.12s !important;
#     width: 100% !important;
# }
# [data-testid="stButton"] button:hover {
#     background: var(--accent) !important;
#     color: #fff !important;
# }
# .stSpinner > div {
#     border-top-color: var(--accent) !important;
# }
# .ms-error {
#     background: rgba(220,38,38,0.08);
#     border: 1px solid rgba(220,38,38,0.20);
#     border-radius: 8px; padding: 16px 20px;
#     font-family: var(--font-mono);
#     font-size: 12px; color: #F87171;
#     margin-bottom: 20px;
# }
# .ms-success {
#     background: rgba(34,197,94,0.08);
#     border: 1px solid rgba(34,197,94,0.20);
#     border-radius: 8px; padding: 14px 18px;
#     font-family: var(--font-mono);
#     font-size: 12px; color: #4ADE80;
#     margin-bottom: 16px;
# }

# ::-webkit-scrollbar { width: 5px; height: 5px; }
# ::-webkit-scrollbar-track { background: var(--bg-base); }
# ::-webkit-scrollbar-thumb { background: var(--bg-hover); border-radius: 3px; }
# ::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }
# </style>
# """, unsafe_allow_html=True)


# # ── Helpers ───────────────────────────────────────────────────────────────────

# def fetch_signals():
#     try:
#         r = requests.get(f"{API_BASE}/signals", params={"limit": 200}, timeout=60)
#         r.raise_for_status()
#         return r.json()
#     except requests.exceptions.ConnectionError:
#         return None
#     except Exception as e:
#         print(f"ERROR in fetch_signals: {type(e).__name__}: {e}")
#         return []


# def fetch_brief(drug_key: str, pt: str):
#     try:
#         from urllib.parse import quote
#         encoded_drug = quote(drug_key, safe="")
#         encoded_pt   = quote(pt, safe="")
#         url = f"{API_BASE}/signals/{encoded_drug}/{encoded_pt}/brief"
#         print(f"Fetching brief from: {url}")
#         r = requests.get(url, timeout=60)
#         print(f"Brief response status: {r.status_code}")
#         if r.status_code == 404:
#             print(f"Brief not found (404) for {drug_key} × {pt}")
#             return None
#         r.raise_for_status()
#         return r.json()
#     except Exception as e:
#         print(f"ERROR in fetch_brief for {drug_key} × {pt}: {type(e).__name__}: {e}")
#         return None


# def trigger_investigate(drug_key: str, pt: str) -> bool:
#     try:
#         from urllib.parse import quote
#         encoded_drug = quote(drug_key, safe="")
#         encoded_pt   = quote(pt, safe="")
#         r = requests.post(
#             f"{API_BASE}/signals/{encoded_drug}/{encoded_pt}/investigate",
#             timeout=120,
#         )
#         return r.status_code == 200
#     except Exception:
#         return False


# def score_color(score: float, kind: str) -> str:
#     if kind == "stat":
#         return "#F72A2A" if score >= 0.7 else "#F97316" if score >= 0.5 else "#3B82F6"
#     return "#22C55E" if score >= 0.5 else "#EAB308" if score >= 0.3 else "#4A5568"


# def fmt_score(v) -> str:
#     try:    return f"{float(v):.3f}"
#     except: return "—"


# def fmt_prr(v) -> str:
#     try:    return f"{float(v):.2f}"
#     except: return "—"


# def fmt_ts(ts) -> str:
#     if not ts: return "—"
#     try:
#         dt = datetime.fromisoformat(str(ts).replace(" ", "T"))
#         return dt.strftime("%d %b %Y  %H:%M UTC")
#     except: return str(ts)


# def rec_icon(action: str) -> str:
#     return {
#         "MONITOR"     : "👁",
#         "LABEL_UPDATE": "📋",
#         "RESTRICT"    : "⚠️",
#         "WITHDRAW"    : "🚫",
#     }.get(action, "📌")


# def pc(priority: str) -> str:
#     return (priority or "p4").lower()


# # ── Topbar ────────────────────────────────────────────────────────────────────

# st.markdown(f"""
# <div class="ms-topbar">
#     <div class="ms-brand">Med<span>Signal</span></div>
#     <nav class="ms-nav">
#         <a class="ms-navlink" href="/signal_feed">Signal Feed</a>
#         <a class="ms-navlink active" href="/signal_detail">Signal Detail</a>
#         <a class="ms-navlink" href="/hitl_queue">Review Queue</a>
#         <a class="ms-navlink" href="/evaluation">Evaluation</a>
#     </nav>
#     <div class="ms-topbar-right">
#         <div class="ms-live">
#             <div class="ms-live-dot"></div>
#             {datetime.utcnow().strftime("%d %b %Y")}
#         </div>
#     </div>
# </div>
# """, unsafe_allow_html=True)


# # ── Data ──────────────────────────────────────────────────────────────────────

# signals = fetch_signals()

# if signals is None:
#     st.markdown("""
#     <div class="ms-wrap">
#         <div class="ms-error">
#             Cannot reach API at localhost:8000 —
#             run: poetry run uvicorn main:app --reload --port 8000
#         </div>
#     </div>
#     """, unsafe_allow_html=True)
#     st.stop()

# if not signals:
#     st.markdown("""
#     <div class="ms-wrap">
#         <div class="ms-error">
#             No signals found. Run Branch 2 and the agent pipeline first.
#         </div>
#     </div>
#     """, unsafe_allow_html=True)
#     st.stop()

# # Build dropdown options — sorted P1 first then by stat_score
# tier_order = {"P1": 0, "P2": 1, "P3": 2, "P4": 3, None: 4}
# signals_sorted = sorted(
#     signals,
#     key=lambda s: (
#         tier_order.get((s.get("priority") or "").upper(), 4),
#         -(s.get("stat_score") or 0),
#     )
# )

# options = [
#     f"{s['drug_key'].title()}  ×  {s['pt'].title()}  [{(s.get('priority') or 'P4').upper()}]"
#     for s in signals_sorted
# ]


# # ── Page wrap open ────────────────────────────────────────────────────────────

# st.markdown('<div class="ms-wrap">', unsafe_allow_html=True)

# # ── Signal selector ───────────────────────────────────────────────────────────

# st.markdown("""
# <div class="ms-selector-label" style="margin-bottom:6px;">Select Signal</div>
# """, unsafe_allow_html=True)

# selected_idx = st.selectbox(
#     "select_signal",
#     range(len(options)),
#     format_func=lambda i: options[i],
#     label_visibility="collapsed",
# )

# selected_signal = signals_sorted[selected_idx]
# drug_key        = selected_signal["drug_key"]
# pt_val          = selected_signal["pt"]
# priority        = (selected_signal.get("priority") or "P4").upper()
# stat_score      = float(selected_signal.get("stat_score") or 0)
# prr             = selected_signal.get("prr")
# case_count = selected_signal.get("case_count") or selected_signal.get("drug_reaction_count") or 0
# death_count     = int(selected_signal.get("death_count") or 0)
# hosp_count      = int(selected_signal.get("hosp_count") or 0)
# lt_count        = int(selected_signal.get("lt_count") or 0)
# pclass          = pc(priority)

# st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)


# # ── Hero header ───────────────────────────────────────────────────────────────

# brief = fetch_brief(drug_key, pt_val)
# lit_score   = float(brief.get("lit_score") or 0)   if brief else 0.0
# rec_action  = (brief.get("recommended_action") or "MONITOR") if brief else "—"
# brief_text  = (brief.get("brief_text") or "")       if brief else ""
# key_findings= (brief.get("key_findings") or [])     if brief else []
# pmids_cited = (brief.get("pmids_cited") or [])       if brief else []
# generated   = (brief.get("generated_at") or "")     if brief else ""
# model_used  = (brief.get("model_used") or "—")      if brief else "—"

# st.markdown(f"""
# <div class="ms-hero tier-{pclass}">
#     <div class="ms-hero-top">
#         <div class="ms-hero-left">
#             <div class="ms-drug-name">{drug_key}</div>
#             <div class="ms-reaction">{pt_val}</div>
#         </div>
#         <div class="ms-hero-badges">
#             <div class="ms-priority-badge {pclass}">{priority}</div>
#             <div class="ms-action-pill">{rec_action}</div>
#         </div>
#     </div>
#     <div class="ms-kpi-row">
#         <div class="ms-kpi">
#             <div class="ms-kpi-label">PRR</div>
#             <div class="ms-kpi-value accent">{fmt_prr(prr)}</div>
#         </div>
#         <div class="ms-kpi-sep"></div>
#         <div class="ms-kpi">
#             <div class="ms-kpi-label">Cases</div>
#             <div class="ms-kpi-value">{int(case_count):,}</div>
#         </div>
#         <div class="ms-kpi-sep"></div>
#         <div class="ms-kpi">
#             <div class="ms-kpi-label">Deaths</div>
#             <div class="ms-kpi-value">{death_count}</div>
#         </div>
#         <div class="ms-kpi-sep"></div>
#         <div class="ms-kpi">
#             <div class="ms-kpi-label">Hosp.</div>
#             <div class="ms-kpi-value">{hosp_count}</div>
#         </div>
#         <div class="ms-kpi-sep"></div>
#         <div class="ms-kpi">
#             <div class="ms-kpi-label">Life-Threat.</div>
#             <div class="ms-kpi-value">{lt_count}</div>
#         </div>
#         <div class="ms-kpi-sep"></div>
#         <div class="ms-kpi">
#             <div class="ms-kpi-label">StatScore</div>
#             <div class="ms-kpi-value">{fmt_score(stat_score)}</div>
#         </div>
#         <div class="ms-kpi-sep"></div>
#         <div class="ms-kpi">
#             <div class="ms-kpi-label">LitScore</div>
#             <div class="ms-kpi-value">{fmt_score(lit_score)}</div>
#         </div>
#     </div>
# </div>
# """, unsafe_allow_html=True)


# # ── No brief — show investigate button ───────────────────────────────────────

# if not brief:
#     st.markdown(f"""
#     <div class="ms-no-brief">
#         <div class="ms-no-brief-title">No SafetyBrief yet</div>
#         <div class="ms-no-brief-desc">
#             This signal has not been investigated.<br>
#             Click below to run the agent pipeline for this signal.
#         </div>
#     </div>
#     """, unsafe_allow_html=True)

#     col1, col2, col3 = st.columns([3, 2, 3])
#     with col2:
#         if st.button(
#             f"Investigate {drug_key} × {pt_val}",
#             key="investigate_btn",
#         ):
#             with st.spinner("Running agent pipeline — this may take 30–60 seconds..."):
#                 ok = trigger_investigate(drug_key, pt_val)
#             if ok:
#                 st.markdown(
#                     '<div class="ms-success">SafetyBrief generated. Reloading...</div>',
#                     unsafe_allow_html=True,
#                 )
#                 st.rerun()
#             else:
#                 st.markdown(
#                     '<div class="ms-error">Pipeline failed — check FastAPI logs.</div>',
#                     unsafe_allow_html=True,
#                 )

#     st.markdown("</div>", unsafe_allow_html=True)
#     st.stop()


# # ── Two column layout ─────────────────────────────────────────────────────────

# left_col, right_col = st.columns([2, 1], gap="medium")


# with left_col:

#     # ── Safety Brief ─────────────────────────────────────────────────────────
#     if brief_text:
#         paragraphs = [p.strip() for p in brief_text.split("\n") if p.strip()]
#         para_html  = "".join(f"<p>{p}</p>" for p in paragraphs)

#         st.markdown(f"""
#         <div class="ms-section">
#             <div class="ms-section-title">Safety Brief</div>
#             <div class="ms-brief-text">{para_html}</div>
#         </div>
#         """, unsafe_allow_html=True)

#     # ── Key Findings ─────────────────────────────────────────────────────────
#     if key_findings:
#         findings_html = "".join(
#             f"""<div class="ms-finding">
#                     <div class="ms-finding-num">{i+1}</div>
#                     <div class="ms-finding-text">{f}</div>
#                 </div>"""
#             for i, f in enumerate(key_findings)
#         )
#         st.markdown(f"""
#         <div class="ms-section">
#             <div class="ms-section-title">Key Findings</div>
#             {findings_html}
#         </div>
#         """, unsafe_allow_html=True)


# with right_col:

#     # ── Scores ───────────────────────────────────────────────────────────────
#     sc_color = score_color(stat_score, "stat")
#     lc_color = score_color(lit_score,  "lit")

#     st.markdown(f"""
#     <div class="ms-section">
#         <div class="ms-section-title">Evidence Scores</div>
#         <div class="ms-score-row">
#             <div class="ms-score-name">StatScore</div>
#             <div class="ms-score-bar">
#                 <div class="ms-score-bar-fill"
#                      style="width:{stat_score*100:.1f}%;background:{sc_color};"></div>
#             </div>
#             <div class="ms-score-number">{fmt_score(stat_score)}</div>
#         </div>
#         <div class="ms-score-row">
#             <div class="ms-score-name">LitScore</div>
#             <div class="ms-score-bar">
#                 <div class="ms-score-bar-fill"
#                      style="width:{lit_score*100:.1f}%;background:{lc_color};"></div>
#             </div>
#             <div class="ms-score-number">{fmt_score(lit_score)}</div>
#         </div>
#     </div>
#     """, unsafe_allow_html=True)

#     # ── Outcome Flags ─────────────────────────────────────────────────────────
#     st.markdown(f"""
#     <div class="ms-section">
#         <div class="ms-section-title">Outcome Severity</div>
#         <div class="ms-flags">
#             <div class="ms-flag {'on' if death_count else ''}">
#                 Death {'(' + str(death_count) + ')' if death_count else ''}
#             </div>
#             <div class="ms-flag {'on' if lt_count else ''}">
#                 Life-Threat {'(' + str(lt_count) + ')' if lt_count else ''}
#             </div>
#             <div class="ms-flag {'on' if hosp_count else ''}">
#                 Hosp {'(' + str(hosp_count) + ')' if hosp_count else ''}
#             </div>
#         </div>
#     </div>
#     """, unsafe_allow_html=True)

#     # ── Recommended Action ────────────────────────────────────────────────────
#     st.markdown(f"""
#     <div class="ms-section">
#         <div class="ms-section-title">Recommended Action</div>
#         <div class="ms-rec-action">
#             <div class="ms-rec-icon">{rec_icon(rec_action)}</div>
#             <div>
#                 <div class="ms-rec-label">Action</div>
#                 <div class="ms-rec-value {rec_action}">{rec_action}</div>
#             </div>
#         </div>
#     </div>
#     """, unsafe_allow_html=True)

#     # ── Cited PMIDs ───────────────────────────────────────────────────────────
#     if pmids_cited:
#         pmid_items = "".join(
#             f"""<a class="ms-pmid-item"
#                    href="https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
#                    target="_blank">
#                     <span class="ms-pmid-label">PMID {pmid}</span>
#                     <span class="ms-pmid-arrow">↗</span>
#                </a>"""
#             for pmid in pmids_cited
#         )
#         st.markdown(f"""
#         <div class="ms-section">
#             <div class="ms-section-title">Cited Literature</div>
#             <div class="ms-pmid-list">{pmid_items}</div>
#         </div>
#         """, unsafe_allow_html=True)
#     else:
#         st.markdown(f"""
#         <div class="ms-section">
#             <div class="ms-section-title">Cited Literature</div>
#             <div class="ms-no-pmids">No citations — literature not specific to this reaction</div>
#         </div>
#         """, unsafe_allow_html=True)

#     # ── Meta ──────────────────────────────────────────────────────────────────
#     st.markdown(f"""
#     <div class="ms-section">
#         <div class="ms-section-title">Signal Metadata</div>
#         <div class="ms-meta-row">
#             <div class="ms-meta-key">Drug</div>
#             <div class="ms-meta-val">{drug_key}</div>
#         </div>
#         <div class="ms-meta-row">
#             <div class="ms-meta-key">Reaction</div>
#             <div class="ms-meta-val">{pt_val}</div>
#         </div>
#         <div class="ms-meta-row">
#             <div class="ms-meta-key">PRR</div>
#             <div class="ms-meta-val">{fmt_prr(prr)}</div>
#         </div>
#         <div class="ms-meta-row">
#             <div class="ms-meta-key">Cases (A)</div>
#             <div class="ms-meta-val">{int(case_count):,}</div>
#         </div>
#         <div class="ms-meta-row">
#             <div class="ms-meta-key">Model</div>
#             <div class="ms-meta-val">{model_used}</div>
#         </div>
#         <div class="ms-meta-row">
#             <div class="ms-meta-key">Generated</div>
#             <div class="ms-meta-val">{fmt_ts(generated)}</div>
#         </div>
#     </div>
#     """, unsafe_allow_html=True)

#     # ── Re-investigate button ─────────────────────────────────────────────────
#     st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
#     if st.button("Re-investigate Signal", key="reinvestigate_btn"):
#         with st.spinner("Running agent pipeline..."):
#             ok = trigger_investigate(drug_key, pt_val)
#         if ok:
#             st.markdown(
#                 '<div class="ms-success">Brief updated. Reloading...</div>',
#                 unsafe_allow_html=True,
#             )
#             st.rerun()
#         else:
#             st.markdown(
#                 '<div class="ms-error">Pipeline failed — check FastAPI logs.</div>',
#                 unsafe_allow_html=True,
#             )


# # ── Close wrap ────────────────────────────────────────────────────────────────

# st.markdown("</div>", unsafe_allow_html=True)

"""
streamlit_app/pages/2_signal_detail.py — Signal Detail Page

Two-section design:
    Section 1 — Already investigated signals (have SafetyBrief) → dropdown
    Section 2 — Uninvestigated signals (no brief yet) → separate picker + investigate button

Performance fix:
    - fetch_signals() called once, split client-side (no extra API call)
    - fetch_brief() called only when a signal is selected, not on every render
"""

import os
from pathlib import Path
from datetime import datetime

import requests
import streamlit as st
from dotenv import load_dotenv

st.set_page_config(
    page_title="MedSignal — Signal Detail",
    page_icon ="⚕",
    layout    ="wide",
    initial_sidebar_state="collapsed",
)

ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

DEFAULT_API_BASE = "http://localhost:8001"


def get_api_base() -> str:
    configured = os.getenv("MEDSIGNAL_API_BASE", "").strip().strip('"').strip("'")
    return (configured or DEFAULT_API_BASE).rstrip("/")


API_BASE = get_api_base()

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
    --text-secondary: #A0AECA;
    --text-muted    : #5A6E8E;
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
    --accent-bright : #60A5FA;
    --uninvestigated: #8B5CF6;
    --uninvestigated-dim   : rgba(139,92,246,0.12);
    --uninvestigated-border: rgba(139,92,246,0.30);
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

/* ── Topbar ── */
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
.ms-navlink:hover  { background: var(--bg-elevated); color: var(--text-primary); }
.ms-navlink.active { background: var(--bg-elevated); color: var(--text-primary); }
.ms-topbar-right { display: flex; align-items: center; }
.ms-live {
    display: flex; align-items: center; gap: 7px;
    font-family: var(--font-mono); font-size: 11px;
    color: var(--text-muted);
}
.ms-live-dot {
    width: 5px; height: 5px; border-radius: 50%;
    background: var(--accent);
    animation: blink 2.5s ease-in-out infinite;
}
@keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.25} }

/* ── Layout ── */
.ms-wrap { padding: 32px 40px 60px; max-width: 1440px; margin: 0 auto; }

/* ── Section divider ── */
.ms-section-divider {
    display: flex;
    align-items: center;
    gap: 16px;
    margin-bottom: 24px;
}
.ms-section-divider-line {
    flex: 1;
    height: 1px;
    background: var(--border);
}
.ms-section-divider-label {
    font-family: var(--font-mono);
    font-size: 10px;
    font-weight: 500;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: var(--text-muted);
    white-space: nowrap;
}

/* ── Two-panel selector ── */
.ms-selector-panels {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    margin-bottom: 32px;
}
.ms-selector-panel {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 18px 20px 20px;
}
.ms-selector-panel.uninvestigated {
    border-color: var(--uninvestigated-border);
    background: linear-gradient(135deg, var(--bg-surface) 0%, rgba(139,92,246,0.04) 100%);
}
.ms-panel-header {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 14px;
}
.ms-panel-dot {
    width: 8px; height: 8px; border-radius: 50%;
    flex-shrink: 0;
}
.ms-panel-dot.investigated   { background: var(--accent); }
.ms-panel-dot.uninvestigated { background: var(--uninvestigated); }
.ms-panel-title {
    font-family: var(--font-mono);
    font-size: 10px; font-weight: 500;
    letter-spacing: 1.8px; text-transform: uppercase;
    color: var(--text-secondary);
}
.ms-panel-count {
    margin-left: auto;
    font-family: var(--font-mono);
    font-size: 10px;
    color: var(--text-muted);
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    padding: 2px 8px;
    border-radius: 4px;
}

/* ── Uninvestigated action area ── */
.ms-investigate-action {
    margin-top: 14px;
    padding: 14px 16px;
    background: var(--uninvestigated-dim);
    border: 1px solid var(--uninvestigated-border);
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
}
.ms-investigate-info {}
.ms-investigate-label {
    font-family: var(--font-mono);
    font-size: 9px; font-weight: 500;
    letter-spacing: 1.8px; text-transform: uppercase;
    color: var(--uninvestigated);
    margin-bottom: 4px;
}
.ms-investigate-desc {
    font-size: 12px;
    color: var(--text-muted);
    line-height: 1.5;
}

/* ── Hero header ── */
.ms-hero {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 28px 32px;
    margin-bottom: 20px;
    position: relative;
    overflow: hidden;
}
.ms-hero::before {
    content: '';
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 4px;
    border-radius: 12px 0 0 12px;
}
.ms-hero.tier-p1::before { background: var(--p1); }
.ms-hero.tier-p2::before { background: var(--p2); }
.ms-hero.tier-p3::before { background: var(--p3); }
.ms-hero.tier-p4::before { background: var(--p4); }
.ms-hero.tier-uninvestigated::before { background: var(--uninvestigated); }

.ms-hero-top {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    margin-bottom: 24px;
}
.ms-drug-name {
    font-family: var(--font-display);
    font-size: 28px;
    font-weight: 700;
    color: var(--text-primary);
    letter-spacing: -0.5px;
    text-transform: capitalize;
    line-height: 1;
    margin-bottom: 6px;
}
.ms-reaction {
    font-size: 15px;
    color: var(--text-secondary);
    font-weight: 400;
    text-transform: capitalize;
}
.ms-hero-badges {
    display: flex;
    align-items: center;
    gap: 10px;
}
.ms-priority-badge {
    font-family: var(--font-mono);
    font-size: 13px;
    font-weight: 600;
    letter-spacing: 1.5px;
    padding: 6px 14px;
    border-radius: 6px;
    border: 1px solid;
}
.ms-priority-badge.p1 { color:var(--p1); background:var(--p1-dim); border-color:var(--p1-border); }
.ms-priority-badge.p2 { color:var(--p2); background:var(--p2-dim); border-color:var(--p2-border); }
.ms-priority-badge.p3 { color:var(--p3); background:var(--p3-dim); border-color:var(--p3-border); }
.ms-priority-badge.p4 { color:var(--p4); background:var(--p4-dim); border-color:var(--p4-border); }
.ms-priority-badge.uninvestigated {
    color: var(--uninvestigated);
    background: var(--uninvestigated-dim);
    border-color: var(--uninvestigated-border);
}

.ms-action-pill {
    font-family: var(--font-mono);
    font-size: 10px;
    font-weight: 500;
    letter-spacing: 1.2px;
    text-transform: uppercase;
    color: var(--text-secondary);
    background: var(--bg-elevated);
    border: 1px solid var(--border-strong);
    padding: 6px 12px;
    border-radius: 6px;
}

/* ── KPI row ── */
.ms-kpi-row {
    display: flex;
    gap: 32px;
    padding-top: 20px;
    border-top: 1px solid var(--border);
}
.ms-kpi { display: flex; flex-direction: column; gap: 4px; }
.ms-kpi-label {
    font-family: var(--font-mono);
    font-size: 9px; font-weight: 500;
    letter-spacing: 1.8px; text-transform: uppercase;
    color: var(--text-muted);
}
.ms-kpi-value {
    font-family: var(--font-mono);
    font-size: 22px; font-weight: 500;
    color: var(--text-primary); line-height: 1;
}
.ms-kpi-value.accent { color: var(--accent-bright); }
.ms-kpi-value.muted  { color: var(--text-muted); font-size: 16px; }
.ms-kpi-sep {
    width: 1px;
    background: var(--border);
    align-self: stretch;
    margin: 0 8px;
}

/* ── Two column layout ── */
.ms-grid { display: grid; grid-template-columns: 1fr 360px; gap: 20px; align-items: start; }

/* ── Section card ── */
.ms-section {
    background: var(--bg-surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 24px 26px;
    margin-bottom: 20px;
}
.ms-section-title {
    font-family: var(--font-mono);
    font-size: 10px; font-weight: 500;
    letter-spacing: 2px; text-transform: uppercase;
    color: var(--text-muted);
    margin-bottom: 18px;
    padding-bottom: 12px;
    border-bottom: 1px solid var(--border);
}

/* ── Brief text ── */
.ms-brief-text { font-size: 14px; color: var(--text-secondary); line-height: 1.75; }
.ms-brief-text p { margin-bottom: 14px; }
.ms-brief-text p:last-child { margin-bottom: 0; }

/* ── Key findings ── */
.ms-finding {
    display: flex; gap: 12px; align-items: flex-start;
    padding: 12px 0; border-bottom: 1px solid var(--border);
}
.ms-finding:last-child { border-bottom: none; padding-bottom: 0; }
.ms-finding-num {
    font-family: var(--font-mono); font-size: 10px; font-weight: 500;
    color: var(--accent); background: var(--accent-dim);
    border: 1px solid rgba(59,130,246,0.20);
    width: 22px; height: 22px; border-radius: 4px;
    display: flex; align-items: center; justify-content: center;
    flex-shrink: 0; margin-top: 1px;
}
.ms-finding-text { font-size: 13px; color: var(--text-secondary); line-height: 1.6; }

/* ── Scores ── */
.ms-score-row { display: flex; align-items: center; gap: 12px; margin-bottom: 14px; }
.ms-score-row:last-child { margin-bottom: 0; }
.ms-score-name {
    font-family: var(--font-mono); font-size: 10px; font-weight: 500;
    letter-spacing: 1px; text-transform: uppercase;
    color: var(--text-secondary); width: 72px; flex-shrink: 0;
}
.ms-score-bar { flex: 1; height: 4px; background: var(--bg-elevated); border-radius: 3px; overflow: hidden; }
.ms-score-bar-fill { height: 100%; border-radius: 3px; }
.ms-score-number {
    font-family: var(--font-mono); font-size: 13px; font-weight: 500;
    color: var(--text-primary); width: 40px; text-align: right; flex-shrink: 0;
}

/* ── Outcome flags ── */
.ms-flags { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 20px; }
.ms-flag {
    font-family: var(--font-mono); font-size: 10px; font-weight: 500;
    letter-spacing: 1px; text-transform: uppercase;
    padding: 5px 10px; border-radius: 5px;
    border: 1px solid var(--border); color: var(--text-muted); background: var(--bg-elevated);
}
.ms-flag.on { color: var(--p1); border-color: var(--p1-border); background: var(--p1-dim); }

/* ── PMIDs ── */
.ms-pmid-list { display: flex; flex-direction: column; gap: 8px; }
.ms-pmid-item {
    display: flex; align-items: center; justify-content: space-between;
    padding: 10px 14px; background: var(--bg-elevated);
    border: 1px solid var(--border); border-radius: 6px;
    text-decoration: none; transition: border-color 0.12s, background 0.12s;
}
.ms-pmid-item:hover { border-color: var(--accent); background: var(--accent-dim); }
.ms-pmid-label { font-family: var(--font-mono); font-size: 12px; font-weight: 500; color: var(--accent-bright); }
.ms-pmid-arrow { font-size: 11px; color: var(--text-muted); }
.ms-no-pmids { font-family: var(--font-mono); font-size: 12px; color: var(--text-muted); padding: 12px 0; text-align: center; }

/* ── Recommended action ── */
.ms-rec-action {
    display: flex; align-items: center; gap: 14px;
    padding: 16px 18px; background: var(--bg-elevated);
    border: 1px solid var(--border-strong); border-radius: 8px;
}
.ms-rec-icon { font-size: 20px; }
.ms-rec-label { font-family: var(--font-mono); font-size: 9px; font-weight: 500; letter-spacing: 1.8px; text-transform: uppercase; color: var(--text-muted); margin-bottom: 4px; }
.ms-rec-value { font-family: var(--font-mono); font-size: 14px; font-weight: 600; color: var(--text-primary); letter-spacing: 0.5px; }
.ms-rec-value.MONITOR      { color: #60A5FA; }
.ms-rec-value.LABEL_UPDATE { color: #FACC15; }
.ms-rec-value.RESTRICT     { color: #FB923C; }
.ms-rec-value.WITHDRAW     { color: #F87171; }

/* ── Meta ── */
.ms-meta-row { display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid var(--border); }
.ms-meta-row:last-child { border-bottom: none; }
.ms-meta-key { font-family: var(--font-mono); font-size: 10px; font-weight: 500; letter-spacing: 1px; text-transform: uppercase; color: var(--text-muted); }
.ms-meta-val { font-family: var(--font-mono); font-size: 11px; color: var(--text-secondary); }

/* ── Pending investigation state ── */
.ms-pending-card {
    text-align: center;
    padding: 52px 40px;
    background: var(--bg-surface);
    border: 1px solid var(--uninvestigated-border);
    border-radius: 10px;
    margin-bottom: 20px;
}
.ms-pending-icon { font-size: 36px; margin-bottom: 16px; }
.ms-pending-title {
    font-family: var(--font-display); font-size: 20px;
    color: var(--uninvestigated); margin-bottom: 8px; letter-spacing: -0.3px;
}
.ms-pending-desc { font-size: 13px; color: var(--text-muted); line-height: 1.6; }
.ms-pending-stats {
    display: flex; justify-content: center; gap: 32px;
    margin-top: 24px; padding-top: 20px;
    border-top: 1px solid var(--border);
}
.ms-pending-stat { display: flex; flex-direction: column; gap: 4px; align-items: center; }
.ms-pending-stat-label { font-family: var(--font-mono); font-size: 9px; letter-spacing: 1.8px; text-transform: uppercase; color: var(--text-muted); }
.ms-pending-stat-val { font-family: var(--font-mono); font-size: 20px; font-weight: 500; color: var(--text-primary); }

/* ── Streamlit overrides ── */
.stSelectbox > div > div {
    background: var(--bg-elevated) !important;
    border: 1px solid var(--border-strong) !important;
    border-radius: 6px !important;
    color: var(--text-primary) !important;
    font-family: var(--font-mono) !important;
    font-size: 12px !important;
}
.stSelectbox label {
    font-family: var(--font-mono) !important;
    font-size: 10px !important; font-weight: 500 !important;
    letter-spacing: 1.5px !important; text-transform: uppercase !important;
    color: var(--text-muted) !important;
}
[data-testid="stButton"] button {
    font-family: var(--font-mono) !important;
    font-size: 11px !important; font-weight: 500 !important;
    letter-spacing: 1.2px !important; text-transform: uppercase !important;
    padding: 10px 20px !important; border-radius: 6px !important;
    border: 1px solid var(--accent) !important;
    background: var(--accent-dim) !important;
    color: var(--accent-bright) !important;
    transition: all 0.12s !important; width: 100% !important;
}
[data-testid="stButton"] button:hover { background: var(--accent) !important; color: #fff !important; }

/* Investigate button — purple */
.uninvestigated-btn [data-testid="stButton"] button {
    border-color: var(--uninvestigated) !important;
    background: var(--uninvestigated-dim) !important;
    color: var(--uninvestigated) !important;
}
.uninvestigated-btn [data-testid="stButton"] button:hover {
    background: var(--uninvestigated) !important;
    color: #fff !important;
}

.stSpinner > div { border-top-color: var(--accent) !important; }
.ms-error {
    background: rgba(220,38,38,0.08); border: 1px solid rgba(220,38,38,0.20);
    border-radius: 8px; padding: 16px 20px;
    font-family: var(--font-mono); font-size: 12px; color: #F87171; margin-bottom: 20px;
}
.ms-success {
    background: rgba(34,197,94,0.08); border: 1px solid rgba(34,197,94,0.20);
    border-radius: 8px; padding: 14px 18px;
    font-family: var(--font-mono); font-size: 12px; color: #4ADE80; margin-bottom: 16px;
}

::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: var(--bg-base); }
::-webkit-scrollbar-thumb { background: var(--bg-hover); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_signals():
    """Fetch all signals once, cached for 60s. Split client-side into investigated/uninvestigated."""
    try:
        url = f"{API_BASE}/signals"
        print(f"DEBUG: Fetching from {url}")
        r = requests.get(url, params={"limit": 200}, timeout=60)
        print(f"DEBUG: Status code: {r.status_code}")
        r.raise_for_status()
        data = r.json()
        print(f"DEBUG: Got {len(data)} signals")
        return data
    except requests.exceptions.ConnectionError as e:
        print(f"DEBUG: ConnectionError - API not reachable: {e}")
        return None
    except Exception as e:
        print(f"DEBUG ERROR in fetch_signals: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return []


@st.cache_data(ttl=300, show_spinner=False)
def fetch_brief(drug_key: str, pt: str):
    try:
        from urllib.parse import quote
        encoded_drug = quote(drug_key, safe="")
        encoded_pt   = quote(pt, safe="")
        url = f"{API_BASE}/signals/{encoded_drug}/{encoded_pt}/brief"
        r = requests.get(url, timeout=60)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"ERROR in fetch_brief: {type(e).__name__}: {e}")
        return None


# def trigger_investigate(drug_key: str, pt: str) -> bool:
#     try:
#         from urllib.parse import quote
#         encoded_drug = quote(drug_key, safe="")
#         encoded_pt   = quote(pt, safe="")
#         r = requests.post(
#             f"{API_BASE}/signals/{encoded_drug}/{encoded_pt}/investigate",
#             timeout=120,
#         )
#         return r.status_code == 200
#     except Exception:
#         return False
def trigger_investigate(drug_key: str, pt: str) -> bool:
    try:
        from urllib.parse import quote
        encoded_drug = quote(drug_key, safe="")
        encoded_pt   = quote(pt, safe="")
        url = f"{API_BASE}/signals/{encoded_drug}/{encoded_pt}/investigate"
        st.write(f"DEBUG: posting to {url}")          # show in UI
        r = requests.post(url, timeout=300)
        st.write(f"DEBUG: status={r.status_code}")    # show in UI
        return r.status_code == 200
    except Exception as e:
        st.write(f"DEBUG EXCEPTION: {type(e).__name__}: {e}")  # show in UI
        return False

def score_color(score: float, kind: str) -> str:
    if kind == "stat":
        return "#F72A2A" if score >= 0.7 else "#F97316" if score >= 0.5 else "#3B82F6"
    return "#22C55E" if score >= 0.5 else "#EAB308" if score >= 0.3 else "#4A5568"


def fmt_score(v) -> str:
    try:    return f"{float(v):.3f}"
    except: return "—"


def fmt_prr(v) -> str:
    try:    return f"{float(v):.2f}"
    except: return "—"


def fmt_ts(ts) -> str:
    if not ts: return "—"
    try:
        dt = datetime.fromisoformat(str(ts).replace(" ", "T"))
        return dt.strftime("%d %b %Y  %H:%M UTC")
    except: return str(ts)


def rec_icon(action: str) -> str:
    return {"MONITOR": "👁", "LABEL_UPDATE": "📋", "RESTRICT": "⚠️", "WITHDRAW": "🚫"}.get(action, "📌")


def pc(priority: str) -> str:
    return (priority or "p4").lower()


# ── Topbar ────────────────────────────────────────────────────────────────────

st.markdown(f"""
<div class="ms-topbar">
    <div class="ms-brand">Med<span>Signal</span></div>
    <nav class="ms-nav">
        <a class="ms-navlink" href="/signal_feed">Signal Feed</a>
        <a class="ms-navlink active" href="/signal_detail">Signal Detail</a>
        <a class="ms-navlink" href="/hitl_queue">Review Queue</a>
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

signals = fetch_signals()

if signals is None:
    st.markdown("""
    <div class="ms-wrap">
        <div class="ms-error">
            Cannot reach MedSignal API —
            run: poetry run uvicorn app.main:app --reload --port 8001
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.caption(f"Checked base URL: {API_BASE}")
    st.caption("Set MEDSIGNAL_API_BASE in .env if your FastAPI app is on a different port.")
    st.stop()

if not signals:
    st.markdown("""
    <div class="ms-wrap">
        <div class="ms-error">No signals found. Run Branch 2 and the agent pipeline first.</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ── Split into investigated vs uninvestigated ─────────────────────────────────
# LEFT JOIN means priority=None when no brief exists
tier_order = {"P1": 0, "P2": 1, "P3": 2, "P4": 3}

investigated = sorted(
    [s for s in signals if s.get("priority") is not None],
    key=lambda s: (tier_order.get((s.get("priority") or "").upper(), 4), -(s.get("stat_score") or 0)),
)
uninvestigated = sorted(
    [s for s in signals if s.get("priority") is None],
    key=lambda s: -(s.get("prr") or 0),
)

# inv_options   = [
#     f"{s['drug_key'].title()}  ×  {s['pt'].title()}  [{s['priority'].upper()}]"
#     for s in investigated
# ]
inv_options = [
    f"{s['drug_key'].title()}  ×  {s['pt'].title()}  [{s['priority'].upper()}]"
    for s in investigated
]
# Pre-select the just-investigated signal if set
default_inv_idx = 0
pending_drug = st.session_state.pop("pending_select_drug", None)
pending_pt   = st.session_state.pop("pending_select_pt", None)
if pending_drug and pending_pt:
    for i, s in enumerate(investigated):
        if s["drug_key"] == pending_drug and s["pt"] == pending_pt:
            default_inv_idx = i
            break
uninv_options = [
    f"{s['drug_key'].title()}  ×  {s['pt'].title()}  [PRR {fmt_prr(s.get('prr'))}]"
    for s in uninvestigated
]


# ── Page wrap ─────────────────────────────────────────────────────────────────

st.markdown('<div class="ms-wrap">', unsafe_allow_html=True)

# ── Two-panel selector ────────────────────────────────────────────────────────

st.markdown('<div class="ms-selector-panels">', unsafe_allow_html=True)

col_inv, col_uninv = st.columns(2, gap="medium")

with col_inv:
    st.markdown(f"""
    <div class="ms-panel-header">
        <div class="ms-panel-dot investigated"></div>
        <div class="ms-panel-title">Investigated Signals</div>
        <div class="ms-panel-count">{len(investigated)}</div>
    </div>
    """, unsafe_allow_html=True)

    if investigated:
        inv_idx = st.selectbox(
            "investigated_signal",
            range(len(inv_options)),
            format_func=lambda i: inv_options[i],
            label_visibility="collapsed",
            key="inv_select",
        )
    else:
        st.markdown(
            '<div style="font-family:var(--font-mono);font-size:12px;color:var(--text-muted);padding:10px 0;">'
            'No investigated signals yet.</div>',
            unsafe_allow_html=True,
        )
        inv_idx = None

with col_uninv:
    st.markdown(f"""
    <div class="ms-panel-header">
        <div class="ms-panel-dot uninvestigated"></div>
        <div class="ms-panel-title">Uninvestigated Signals</div>
        <div class="ms-panel-count">{len(uninvestigated)}</div>
    </div>
    """, unsafe_allow_html=True)

    if uninvestigated:
        uninv_idx = st.selectbox(
            "uninvestigated_signal",
            range(len(uninv_options)),
            index=default_inv_idx,
            format_func=lambda i: uninv_options[i],
            label_visibility="collapsed",
            key="uninv_select",
        )
    else:
        st.markdown(
            '<div style="font-family:var(--font-mono);font-size:12px;color:var(--text-muted);padding:10px 0;">'
            'All signals have been investigated.</div>',
            unsafe_allow_html=True,
        )
        uninv_idx = None

st.markdown('</div>', unsafe_allow_html=True)  # close selector-panels


# ── Determine active signal based on which panel was last interacted with ─────
# Use session state to track which panel is "active"
if "active_panel" not in st.session_state:
    st.session_state.active_panel = "investigated" if investigated else "uninvestigated"

# Detect panel switches via buttons
c1, c2 = st.columns(2, gap="medium")
with c1:
    if st.button("View Investigated Signal", key="btn_view_inv", disabled=(inv_idx is None)):
        st.session_state.active_panel = "investigated"
with c2:
    if st.button("View Uninvestigated Signal", key="btn_view_uninv", disabled=(uninv_idx is None)):
        st.session_state.active_panel = "uninvestigated"

st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)


# ── Resolve active signal ─────────────────────────────────────────────────────

active_panel = st.session_state.active_panel

if active_panel == "investigated" and inv_idx is not None:
    selected_signal = investigated[inv_idx]
    is_investigated = True
elif active_panel == "uninvestigated" and uninv_idx is not None:
    selected_signal = uninvestigated[uninv_idx]
    is_investigated = False
elif investigated and inv_idx is not None:
    selected_signal = investigated[inv_idx]
    is_investigated = True
    st.session_state.active_panel = "investigated"
elif uninvestigated and uninv_idx is not None:
    selected_signal = uninvestigated[uninv_idx]
    is_investigated = False
    st.session_state.active_panel = "uninvestigated"
else:
    st.markdown(
        '<div class="ms-error">No signals available.</div>',
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

drug_key   = selected_signal["drug_key"]
pt_val     = selected_signal["pt"]
priority   = (selected_signal.get("priority") or "—").upper()
stat_score = float(selected_signal.get("stat_score") or 0)
prr        = selected_signal.get("prr")
case_count = int(selected_signal.get("drug_reaction_count") or selected_signal.get("case_count") or 0)
death_count = int(selected_signal.get("death_count") or 0)
hosp_count  = int(selected_signal.get("hosp_count") or 0)
lt_count    = int(selected_signal.get("lt_count") or 0)
pclass      = pc(priority) if is_investigated else "uninvestigated"


# ── Fetch brief only for investigated signals ─────────────────────────────────

if is_investigated:
    brief = fetch_brief(drug_key, pt_val)
else:
    brief = None

lit_score    = float(brief.get("lit_score") or 0)   if brief else 0.0
rec_action   = (brief.get("recommended_action") or "MONITOR") if brief else "—"
brief_text   = (brief.get("brief_text") or "")       if brief else ""
key_findings = (brief.get("key_findings") or [])     if brief else []
pmids_cited  = (brief.get("pmids_cited") or [])      if brief else []
generated    = (brief.get("generated_at") or "")     if brief else ""
model_used   = (brief.get("model_used") or "—")      if brief else "—"


# ── Hero ──────────────────────────────────────────────────────────────────────

badge_html = (
    f'<div class="ms-priority-badge {pclass}">{priority}</div>'
    if is_investigated
    else '<div class="ms-priority-badge uninvestigated">NOT INVESTIGATED</div>'
)
action_pill = (
    f'<div class="ms-action-pill">{rec_action}</div>'
    if is_investigated
    else '<div class="ms-action-pill" style="color:var(--uninvestigated);border-color:var(--uninvestigated-border)">PENDING</div>'
)

lit_kpi = (
    f'<div class="ms-kpi-value">{fmt_score(lit_score)}</div>'
    if is_investigated
    else '<div class="ms-kpi-value muted">—</div>'
)

st.markdown(f"""
<div class="ms-hero tier-{pclass}">
    <div class="ms-hero-top">
        <div class="ms-hero-left">
            <div class="ms-drug-name">{drug_key}</div>
            <div class="ms-reaction">{pt_val}</div>
        </div>
        <div class="ms-hero-badges">
            {badge_html}
            {action_pill}
        </div>
    </div>
    <div class="ms-kpi-row">
        <div class="ms-kpi">
            <div class="ms-kpi-label">PRR</div>
            <div class="ms-kpi-value accent">{fmt_prr(prr)}</div>
        </div>
        <div class="ms-kpi-sep"></div>
        <div class="ms-kpi">
            <div class="ms-kpi-label">Cases</div>
            <div class="ms-kpi-value">{case_count:,}</div>
        </div>
        <div class="ms-kpi-sep"></div>
        <div class="ms-kpi">
            <div class="ms-kpi-label">Deaths</div>
            <div class="ms-kpi-value">{death_count}</div>
        </div>
        <div class="ms-kpi-sep"></div>
        <div class="ms-kpi">
            <div class="ms-kpi-label">Hosp.</div>
            <div class="ms-kpi-value">{hosp_count}</div>
        </div>
        <div class="ms-kpi-sep"></div>
        <div class="ms-kpi">
            <div class="ms-kpi-label">Life-Threat.</div>
            <div class="ms-kpi-value">{lt_count}</div>
        </div>
        <div class="ms-kpi-sep"></div>
        <div class="ms-kpi">
            <div class="ms-kpi-label">StatScore</div>
            <div class="ms-kpi-value">{fmt_score(stat_score)}</div>
        </div>
        <div class="ms-kpi-sep"></div>
        <div class="ms-kpi">
            <div class="ms-kpi-label">LitScore</div>
            {lit_kpi}
        </div>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Uninvestigated — show pending card + investigate button ───────────────────

if not is_investigated:
    st.markdown(f"""
    <div class="ms-pending-card">
        <div class="ms-pending-icon">🔬</div>
        <div class="ms-pending-title">Signal Not Yet Investigated</div>
        <div class="ms-pending-desc">
            This signal was flagged by the PRR pipeline but has not been run through the agent pipeline yet.<br>
            Click below to run Agent 1 → Agent 2 → Agent 3 and generate a SafetyBrief.
        </div>
        <div class="ms-pending-stats">
            <div class="ms-pending-stat">
                <div class="ms-pending-stat-label">PRR</div>
                <div class="ms-pending-stat-val">{fmt_prr(prr)}</div>
            </div>
            <div class="ms-pending-stat">
                <div class="ms-pending-stat-label">Cases</div>
                <div class="ms-pending-stat-val">{case_count:,}</div>
            </div>
            <div class="ms-pending-stat">
                <div class="ms-pending-stat-label">Deaths</div>
                <div class="ms-pending-stat-val">{death_count}</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([3, 2, 3])
    with col2:
        st.markdown('<div class="uninvestigated-btn">', unsafe_allow_html=True)
        # if st.button(f"Investigate Signal", key="investigate_btn"):
        #     with st.spinner("Running agent pipeline — 30–60 seconds..."):
        #         ok = trigger_investigate(drug_key, pt_val)
        #     if ok:
        #         st.markdown(
        #             '<div class="ms-success">✓ SafetyBrief generated. Reloading...</div>',
        #             unsafe_allow_html=True,
        #         )
        #         st.cache_data.clear()   # bust the signals cache so new priority shows
        #         st.session_state.active_panel = "investigated"
        #         st.rerun()
        #     else:
        #         st.markdown(
        #             '<div class="ms-error">Pipeline failed — check FastAPI logs.</div>',
        #             unsafe_allow_html=True,
        #         )
        # st.markdown('</div>', unsafe_allow_html=True)
        if st.button(f"Investigate Signal", key="investigate_btn"):
            with st.spinner("Running agent pipeline — this may take 1-2 minutes..."):
                ok = trigger_investigate(drug_key, pt_val)
        if ok:
            st.markdown(
            '<div class="ms-success">✓ SafetyBrief generated. Reloading...</div>',
            unsafe_allow_html=True,
            )
            st.cache_data.clear()
            st.session_state.active_panel = "investigated"
        # Store the drug/pt so we can pre-select it after rerun
            st.session_state.pending_select_drug = drug_key
            st.session_state.pending_select_pt   = pt_val
            st.rerun()
        else:
            st.markdown(
            '<div class="ms-error">Pipeline failed — check FastAPI logs.</div>',
            unsafe_allow_html=True,
            )
    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()


# ── Investigated — two column brief layout ────────────────────────────────────

left_col, right_col = st.columns([2, 1], gap="medium")

with left_col:

    if brief_text:
        paragraphs = [p.strip() for p in brief_text.split("\n") if p.strip()]
        para_html  = "".join(f"<p>{p}</p>" for p in paragraphs)
        st.markdown(f"""
        <div class="ms-section">
            <div class="ms-section-title">Safety Brief</div>
            <div class="ms-brief-text">{para_html}</div>
        </div>
        """, unsafe_allow_html=True)

    if key_findings:
        findings_html = "".join(
            f'<div class="ms-finding"><div class="ms-finding-num">{i+1}</div>'
            f'<div class="ms-finding-text">{f}</div></div>'
            for i, f in enumerate(key_findings)
        )
        st.markdown(f"""
        <div class="ms-section">
            <div class="ms-section-title">Key Findings</div>
            {findings_html}
        </div>
        """, unsafe_allow_html=True)

with right_col:

    # Scores
    sc_color = score_color(stat_score, "stat")
    lc_color = score_color(lit_score, "lit")
    st.markdown(f"""
    <div class="ms-section">
        <div class="ms-section-title">Evidence Scores</div>
        <div class="ms-score-row">
            <div class="ms-score-name">StatScore</div>
            <div class="ms-score-bar"><div class="ms-score-bar-fill" style="width:{stat_score*100:.1f}%;background:{sc_color};"></div></div>
            <div class="ms-score-number">{fmt_score(stat_score)}</div>
        </div>
        <div class="ms-score-row">
            <div class="ms-score-name">LitScore</div>
            <div class="ms-score-bar"><div class="ms-score-bar-fill" style="width:{lit_score*100:.1f}%;background:{lc_color};"></div></div>
            <div class="ms-score-number">{fmt_score(lit_score)}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Outcome flags
    st.markdown(f"""
    <div class="ms-section">
        <div class="ms-section-title">Outcome Severity</div>
        <div class="ms-flags">
            <div class="ms-flag {'on' if death_count else ''}">Death {'(' + str(death_count) + ')' if death_count else ''}</div>
            <div class="ms-flag {'on' if lt_count else ''}">Life-Threat {'(' + str(lt_count) + ')' if lt_count else ''}</div>
            <div class="ms-flag {'on' if hosp_count else ''}">Hosp {'(' + str(hosp_count) + ')' if hosp_count else ''}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Recommended action
    st.markdown(f"""
    <div class="ms-section">
        <div class="ms-section-title">Recommended Action</div>
        <div class="ms-rec-action">
            <div class="ms-rec-icon">{rec_icon(rec_action)}</div>
            <div>
                <div class="ms-rec-label">Action</div>
                <div class="ms-rec-value {rec_action}">{rec_action}</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # PMIDs
    if pmids_cited:
        pmid_items = "".join(
            f'<a class="ms-pmid-item" href="https://pubmed.ncbi.nlm.nih.gov/{pmid}/" target="_blank">'
            f'<span class="ms-pmid-label">PMID {pmid}</span><span class="ms-pmid-arrow">↗</span></a>'
            for pmid in pmids_cited
        )
        st.markdown(f"""
        <div class="ms-section">
            <div class="ms-section-title">Cited Literature</div>
            <div class="ms-pmid-list">{pmid_items}</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="ms-section">
            <div class="ms-section-title">Cited Literature</div>
            <div class="ms-no-pmids">No citations — literature not specific to this reaction</div>
        </div>
        """, unsafe_allow_html=True)

    # Meta
    st.markdown(f"""
    <div class="ms-section">
        <div class="ms-section-title">Signal Metadata</div>
        <div class="ms-meta-row"><div class="ms-meta-key">Drug</div><div class="ms-meta-val">{drug_key}</div></div>
        <div class="ms-meta-row"><div class="ms-meta-key">Reaction</div><div class="ms-meta-val">{pt_val}</div></div>
        <div class="ms-meta-row"><div class="ms-meta-key">PRR</div><div class="ms-meta-val">{fmt_prr(prr)}</div></div>
        <div class="ms-meta-row"><div class="ms-meta-key">Cases (A)</div><div class="ms-meta-val">{case_count:,}</div></div>
        <div class="ms-meta-row"><div class="ms-meta-key">Model</div><div class="ms-meta-val">{model_used}</div></div>
        <div class="ms-meta-row"><div class="ms-meta-key">Generated</div><div class="ms-meta-val">{fmt_ts(generated)}</div></div>
    </div>
    """, unsafe_allow_html=True)

    # Re-investigate
    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
    if st.button("Re-investigate Signal", key="reinvestigate_btn"):
        with st.spinner("Running agent pipeline..."):
            ok = trigger_investigate(drug_key, pt_val)
        if ok:
            st.markdown('<div class="ms-success">✓ Brief updated. Reloading...</div>', unsafe_allow_html=True)
            st.cache_data.clear()
            st.rerun()
        else:
            st.markdown('<div class="ms-error">Pipeline failed — check FastAPI logs.</div>', unsafe_allow_html=True)


# ── Close wrap ────────────────────────────────────────────────────────────────

st.markdown("</div>", unsafe_allow_html=True)
