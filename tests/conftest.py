# tests/conftest.py
# Gitignored — real test identifiers live here
# See conftest.example.py for structure
# purpose of conftest is to pass values  sample_gstin func is read like a variable from conftest

import pytest


# metadata tests
@pytest.fixture
def sample_gstin():
    return "29AABCT1332L1ZU"


@pytest.fixture
def sample_vendor_id():
    return "VND-001"


@pytest.fixture
def sample_contract_id():
    return "SOW-2024-003"


# math tests
@pytest.fixture
def clean_line_items():
    return [
        {"total": 120000.0},
        {"total": 25000.0},
        {"total": 40000.0},
        {"total": 15000.0},
    ]


@pytest.fixture
def clean_total():
    return 236000.0


@pytest.fixture
def clean_tax():
    return 36000.0


# DATES CHECKER


@pytest.fixture
def valid_dates():
    return {
        "invoice_date": "2024-11-18",
        "due_date": "2024-12-18",
    }


# FIELD VERIFIER


@pytest.fixture
def sample_raw_text():
    return (
        "INVOICE Invoice Number: INV-2024-00892 "
        "FROM Oroboros Solutions Pvt Ltd "
        "TOTAL DUE 2,36,000.00 "
        "Invoice Date: 18 November 2024"
    )


@pytest.fixture
def sample_extraction():
    return {
        "vendor_name": "Oroboros Solutions Pvt Ltd",
        "invoice_number": "INV-2024-00892",
        "total_amount": "2,36,000.00",
        "invoice_date": "2024-11-18",
    }


@pytest.fixture
def sample_line_items_with_rates():
    return [
        {
            "description": "data_engineering",
            "unit_price": 1500.0,
            "quantity": 80,
            "total": 120000.0,
        },
        {"description": "cloud_setup", "unit_price": 25000.0, "quantity": 1, "total": 25000.0},
    ]


# INVOICE HISTORY CHECKER


@pytest.fixture
def sample_history_vendor_id():
    return "VND-001"
