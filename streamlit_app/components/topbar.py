"""
streamlit_app/components/topbar.py — Shared topbar component.
"""

from datetime import datetime
from zoneinfo import ZoneInfo
import streamlit as st

PAGES = [
    ("Signal Feed",   "/signal_feed"),
    ("Signal Detail", "/signal_detail"),
    ("Review Queue",  "/hitl_queue"),
    ("Evaluation",    "/evaluation"),
    ("Metrics",       "/metrics"),
    ("Evidence",      "/evidence_explorer"),
]

TOPBAR_CSS = """
<style>
.ms-topbar {
    position: fixed !important;
    top: 0 !important;
    left: 0 !important;
    right: 0 !important;
    width: 100vw !important;
    margin: 0 !important;
    z-index: 200;
    display: flex !important;
    align-items: center !important;
    justify-content: space-between !important;
    height: 60px !important;
    padding: 0 48px !important;
    background: var(--bg-surface) !important;
    border-bottom: 1px solid var(--border) !important;
    flex-wrap: nowrap !important;   /* ← prevents stacking */
    min-width: 0 !important;
}
.ms-brand {
    font-family: var(--font-display);
    font-size: 20px;
    font-weight: 700;
    letter-spacing: -0.3px;
    color: var(--text-primary);
    flex-shrink: 0 !important;
}
.ms-brand span { color: var(--accent); }
.ms-nav { display: flex; gap: 4px;  flex-wrap: nowrap !important; flex-shrink: 0 !important;}
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
    flex-shrink: 0 !important;
    white-space: nowrap !important;
}
.ms-live-dot {
    width: 6px; height: 6px; border-radius: 50%;
    background: var(--p4);
    animation: blink 2.5s ease-in-out infinite;
}
@keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.25} }
</style>
"""

def render_topbar(active_page: str) -> None:
    """
    Renders topbar CSS + HTML with the correct active page highlighted.
    Call once at the top of each page — after st.set_page_config().

    Args:
        active_page: must match one of the labels in PAGES exactly
                     e.g. "Signal Feed", "Review Queue", "Evaluation"
    """
    est = ZoneInfo("America/New_York")
    now = datetime.now(est).strftime("%d %b %Y")

    links = "".join([
        f'<a class="ms-navlink{" active" if label == active_page else ""}" '
        f'href="{href}" target="_self">{label}</a>'
        for label, href in PAGES
    ])

    # Inject CSS first — must be before the HTML
    st.markdown(TOPBAR_CSS, unsafe_allow_html=True)

    st.markdown(f"""
<div class="ms-topbar">
    <div class="ms-brand">Med<span>Signal</span></div>
    <nav class="ms-nav">{links}</nav>
    <div class="ms-live">
        <div class="ms-live-dot"></div>
        {now} EST
    </div>
</div>
""", unsafe_allow_html=True)