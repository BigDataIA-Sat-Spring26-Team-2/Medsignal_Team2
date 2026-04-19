"""
streamlit_app/app.py — MedSignal Streamlit entry point.

Run:
    poetry run streamlit run streamlit_app/app.py --server.port 8501
"""

import streamlit as st

st.set_page_config(page_title="MedSignal", page_icon="⚕", layout="wide")
st.switch_page("pages/1_signal_feed.py")
