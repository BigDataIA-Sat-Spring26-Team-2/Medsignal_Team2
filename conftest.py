# conftest.py
import sys
import os
import pytest

# Add project root to Python path so 'app' is importable
sys.path.insert(0, os.path.dirname(__file__))


def pytest_configure(config):
    config.addinivalue_line("markers", "unit: pure logic tests, no external dependencies")
    config.addinivalue_line("markers", "integration: requires real API keys and Snowflake")
    config.addinivalue_line("markers", "live: requires ChromaDB loaded with abstracts")