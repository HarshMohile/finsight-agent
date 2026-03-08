# tests/conftest.example.py
# Safe to commit — no real identifiers
# Copy to conftest.py and fill in real values

import pytest


@pytest.fixture
def sample_gstin():
    return "YOUR_TEST_GSTIN"


@pytest.fixture
def sample_vendor_id():
    return "YOUR_VENDOR_ID"


@pytest.fixture
def sample_contract_id():
    return "YOUR_CONTRACT_ID"
