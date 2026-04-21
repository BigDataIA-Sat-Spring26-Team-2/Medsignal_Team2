"""
streamlit_app/components/nav.py — Shared topbar component

Single source of truth for the MedSignal navigation header.
All pages import and call render_topbar(active) instead of
duplicating the nav HTML.

Usage:
    from components.nav import render_topbar
    render_topbar("Signal Feed")   # label must match NAVLINKS entry exactly

Navigation links are defined once in NAVLINKS.
To add, remove, or rename a tab, edit only this file.
"""

from datetime import datetime
import streamlit as st

# ── Navigation link registry ──────────────────────────────────────────────────
# Order here is the order displayed in the topbar.
# Each entry: (display_label, url_path)
NAVLINKS = [
    ("Signal Feed",   "/signal_feed"),
    ("Signal Detail", "/signal_detail"),
    ("Review Queue",  "/hitl_queue"),
    ("Evaluation",    "/evaluation"),
    ("Metrics",       "/metrics"),
    ("Evidence",      "/evidence_explorer"),
]


_CSS_INJECTED = False


def render_topbar(active: str) -> None:
    """
    Render the MedSignal sticky topbar with the correct active tab highlighted.

    Args:
        active: display label of the current page, e.g. "Signal Feed".
                Must match one of the labels in NAVLINKS exactly.
    """
    global _CSS_INJECTED
    if not _CSS_INJECTED:
        st.markdown("""
<style>
/* ── Topbar (injected once by components/nav.py) ─────────────────────────── */
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
</style>
""", unsafe_allow_html=True)
        _CSS_INJECTED = True

    links_html = "\n".join(
        f'        <a class="{"ms-navlink active" if label == active else "ms-navlink"}" '
        f'href="{href}" target="_self">{label}</a>'
        for label, href in NAVLINKS
    )
    st.markdown(f"""
<div class="ms-topbar">
    <div class="ms-brand">Med<span>Signal</span></div>
    <nav class="ms-nav">
{links_html}
    </nav>
    <div class="ms-live">
        <div class="ms-live-dot"></div>
        {datetime.utcnow().strftime("%d %b %Y")}
    </div>
</div>
""", unsafe_allow_html=True)
