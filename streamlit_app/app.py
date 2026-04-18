"""
streamlit_app/app.py — MedSignal Streamlit entry point.

Pages are in streamlit_app/pages/ and load automatically.
Streamlit discovers them alphabetically by filename prefix number.

   

Run:
    poetry run streamlit run streamlit_app/app.py --server.port 8501
"""

import streamlit as st

st.set_page_config(
    page_title="MedSignal",
    page_icon ="⚕",
    layout    ="wide",
    initial_sidebar_state="collapsed",
)

# Redirect to the HITL queue page by default
# when other pages are ready this becomes the landing dashboard

st.markdown("""
<style>
html, body, .stApp { background: #080C14 !important; }
#MainMenu, footer, header { display: none !important; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div style="
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 80vh;
    font-family: 'Inter', sans-serif;
    color: #7B8DB0;
    text-align: center;
">
    <div style="font-size:13px;letter-spacing:2px;text-transform:uppercase;
                margin-bottom:16px;color:#3D4F6E;">
        MedSignal
    </div>
    <div style="font-size:28px;color:#EEF2FF;font-weight:600;
                letter-spacing:-0.5px;margin-bottom:12px;">
        Drug Safety Signal Detection
    </div>
    <div style="font-size:13px;max-width:400px;line-height:1.6;">
        Select a page from the sidebar to begin.
    </div>
</div>
""", unsafe_allow_html=True)