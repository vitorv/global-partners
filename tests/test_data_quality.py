"""
test_data_quality.py
--------------------
Industry-standard data quality tests for all three Medallion layers.
Uses pytest + pandas to validate Parquet output without Spark overhead.

Run with:  pytest tests/test_data_quality.py -v
"""

import os
import pytest
import pandas as pd
import numpy as np

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE = os.path.join(PROJECT_ROOT, "data", "output", "bronze")
SILVER = os.path.join(PROJECT_ROOT, "data", "output", "silver")
GOLD   = os.path.join(PROJECT_ROOT, "data", "output", "gold")


# ── Fixtures ──────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def bronze_date_dim():
    return pd.read_parquet(os.path.join(BRONZE, "date_dim"))

@pytest.fixture(scope="module")
def bronze_order_items():
    return pd.read_parquet(os.path.join(BRONZE, "order_items"))

@pytest.fixture(scope="module")
def bronze_order_item_options():
    return pd.read_parquet(os.path.join(BRONZE, "order_item_options"))

@pytest.fixture(scope="module")
def silver_date_dim():
    return pd.read_parquet(os.path.join(SILVER, "date_dim"))

@pytest.fixture(scope="module")
def silver_order_items():
    return pd.read_parquet(os.path.join(SILVER, "order_items"))

@pytest.fixture(scope="module")
def silver_order_item_options():
    return pd.read_parquet(os.path.join(SILVER, "order_item_options"))

@pytest.fixture(scope="module")
def gold_dim_date():
    return pd.read_parquet(os.path.join(GOLD, "dim_date"))

@pytest.fixture(scope="module")
def gold_dim_customer():
    return pd.read_parquet(os.path.join(GOLD, "dim_customer"))

@pytest.fixture(scope="module")
def gold_dim_restaurant():
    return pd.read_parquet(os.path.join(GOLD, "dim_restaurant"))

@pytest.fixture(scope="module")
def gold_fct_order_summary():
    return pd.read_parquet(os.path.join(GOLD, "fct_order_summary"))

@pytest.fixture(scope="module")
def gold_fct_daily_sales():
    return pd.read_parquet(os.path.join(GOLD, "fct_daily_sales_summary"))

@pytest.fixture(scope="module")
def gold_fct_customer_daily():
    return pd.read_parquet(os.path.join(GOLD, "fct_customer_daily_snapshot"))

@pytest.fixture(scope="module")
def gold_fct_customer_rfm():
    return pd.read_parquet(os.path.join(GOLD, "fct_customer_rfm"))


# =============================================================================
# BRONZE LAYER TESTS
# =============================================================================

class TestBronzeLayer:
    """Validate raw ingestion: completeness, schema, metadata."""

    def test_date_dim_row_count(self, bronze_date_dim):
        assert len(bronze_date_dim) == 365, f"Expected 365 rows, got {len(bronze_date_dim)}"

    def test_order_items_row_count(self, bronze_order_items):
        assert len(bronze_order_items) == 203519, f"Expected 203519, got {len(bronze_order_items)}"

    def test_order_item_options_row_count(self, bronze_order_item_options):
        assert len(bronze_order_item_options) == 193017, f"Expected 193017, got {len(bronze_order_item_options)}"

    def test_ingestion_timestamp_not_null(self, bronze_date_dim, bronze_order_items, bronze_order_item_options):
        for name, df in [("date_dim", bronze_date_dim), ("order_items", bronze_order_items), ("order_item_options", bronze_order_item_options)]:
            assert df["ingestion_timestamp"].notna().all(), f"Null ingestion_timestamp found in bronze_{name}"

    def test_order_items_pk_not_null(self, bronze_order_items):
        assert bronze_order_items["ORDER_ID"].notna().all(), "Null ORDER_ID in bronze_order_items"
        # LINEITEM_ID has a small number of nulls in source data — verify count is stable
        null_count = bronze_order_items["LINEITEM_ID"].isna().sum()
        assert null_count < 100, f"Too many null LINEITEM_IDs: {null_count} (expected < 100)"

    def test_order_item_options_pk_not_null(self, bronze_order_item_options):
        assert bronze_order_item_options["ORDER_ID"].notna().all(), "Null ORDER_ID in bronze_order_item_options"
        assert bronze_order_item_options["LINEITEM_ID"].notna().all(), "Null LINEITEM_ID in bronze_order_item_options"

    def test_date_dim_pk_not_null(self, bronze_date_dim):
        assert bronze_date_dim["date_key"].notna().all(), "Null date_key in bronze_date_dim"


# =============================================================================
# SILVER LAYER TESTS
# =============================================================================

class TestSilverLayer:
    """Validate type casting, computed columns, and data quality rules."""

    def test_row_counts_match_bronze(self, silver_date_dim, bronze_date_dim,
                                      silver_order_items, bronze_order_items,
                                      silver_order_item_options, bronze_order_item_options):
        assert len(silver_date_dim) == len(bronze_date_dim), "date_dim row count mismatch"
        assert len(silver_order_items) == len(bronze_order_items), "order_items row count mismatch"
        assert len(silver_order_item_options) == len(bronze_order_item_options), "order_item_options row count mismatch"

    def test_date_key_not_null(self, silver_date_dim):
        assert silver_date_dim["date_key"].notna().all(), "Null date_key in silver_date_dim"

    def test_user_id_no_nulls(self, silver_order_items):
        assert silver_order_items["user_id"].notna().all(), "Null user_id found (GUEST backfill failed)"

    def test_guest_backfill_count(self, silver_order_items):
        guest_count = (silver_order_items["user_id"] == "GUEST").sum()
        assert guest_count == 17808, f"Expected 17808 GUEST rows, got {guest_count}"

    def test_is_loyalty_is_boolean(self, silver_order_items):
        assert silver_order_items["is_loyalty"].dtype == bool, f"is_loyalty type: {silver_order_items['is_loyalty'].dtype}"

    def test_is_weekend_is_boolean(self, silver_date_dim):
        assert silver_date_dim["is_weekend"].dtype == bool, f"is_weekend type: {silver_date_dim['is_weekend'].dtype}"

    def test_is_holiday_is_boolean(self, silver_date_dim):
        assert silver_date_dim["is_holiday"].dtype == bool, f"is_holiday type: {silver_date_dim['is_holiday'].dtype}"

    def test_line_item_revenue_not_null(self, silver_order_items):
        assert silver_order_items["line_item_revenue"].notna().all(), "Null line_item_revenue"

    def test_line_item_revenue_computation(self, silver_order_items):
        """Spot-check: line_item_revenue should equal item_price * item_quantity."""
        sample = silver_order_items.head(100).copy()
        expected = sample["item_price"].astype(float) * sample["item_quantity"].astype(float)
        actual = sample["line_item_revenue"].astype(float)
        assert np.allclose(expected, actual, atol=0.01), "line_item_revenue computation mismatch"

    def test_option_revenue_computation(self, silver_order_item_options):
        """Spot-check: option_revenue should equal option_price * option_quantity."""
        sample = silver_order_item_options.head(100).copy()
        expected = sample["option_price"].astype(float) * sample["option_quantity"].astype(float)
        actual = sample["option_revenue"].astype(float)
        assert np.allclose(expected, actual, atol=0.01), "option_revenue computation mismatch"

    def test_is_discount_matches_negative_price(self, silver_order_item_options):
        """is_discount should be True only when option_price < 0."""
        prices = silver_order_item_options["option_price"].astype(float)
        expected = prices < 0
        actual = silver_order_item_options["is_discount"]
        assert (expected == actual).all(), "is_discount does not match option_price < 0"

    def test_no_null_prices(self, silver_order_items, silver_order_item_options):
        assert silver_order_items["item_price"].notna().all(), "Null item_price in silver_order_items"
        assert silver_order_item_options["option_price"].notna().all(), "Null option_price in silver_order_item_options"

    def test_no_null_quantities(self, silver_order_items, silver_order_item_options):
        assert silver_order_items["item_quantity"].notna().all(), "Null item_quantity"
        assert silver_order_item_options["option_quantity"].notna().all(), "Null option_quantity"


# =============================================================================
# GOLD LAYER TESTS
# =============================================================================

class TestGoldDimensions:
    """Validate dimension table grain and completeness."""

    def test_dim_customer_pk_unique(self, gold_dim_customer):
        assert gold_dim_customer["user_id"].is_unique, "Duplicate user_id in dim_customer"

    def test_dim_restaurant_count(self, gold_dim_restaurant):
        assert len(gold_dim_restaurant) == 28, f"Expected 28 restaurants, got {len(gold_dim_restaurant)}"

    def test_dim_restaurant_pk_unique(self, gold_dim_restaurant):
        assert gold_dim_restaurant["restaurant_id"].is_unique, "Duplicate restaurant_id"

    def test_dim_date_covers_order_range(self, gold_dim_date, silver_order_items):
        min_order = silver_order_items["order_date"].min()
        max_order = silver_order_items["order_date"].max()
        min_date = gold_dim_date["date_key"].min()
        max_date = gold_dim_date["date_key"].max()
        assert str(min_date) <= str(min_order), f"dim_date starts at {min_date}, orders start at {min_order}"
        assert str(max_date) >= str(max_order), f"dim_date ends at {max_date}, orders end at {max_order}"

    def test_dim_date_pk_unique(self, gold_dim_date):
        assert gold_dim_date["date_key"].is_unique, "Duplicate date_key in dim_date"

    def test_dim_customer_no_null_dates(self, gold_dim_customer):
        assert gold_dim_customer["first_order_date"].notna().all(), "Null first_order_date"
        assert gold_dim_customer["last_order_date"].notna().all(), "Null last_order_date"


class TestGoldOrderSummary:
    """Validate the atomic order-level fact table."""

    def test_pk_unique(self, gold_fct_order_summary):
        assert gold_fct_order_summary["order_id"].is_unique, "Duplicate order_id in fct_order_summary"

    def test_row_count_matches_unique_orders(self, gold_fct_order_summary, silver_order_items):
        expected = silver_order_items["order_id"].nunique()
        actual = len(gold_fct_order_summary)
        assert actual == expected, f"Expected {expected} orders, got {actual}"

    def test_net_revenue_formula(self, gold_fct_order_summary):
        """net_revenue should equal gross_revenue + option_revenue - discount_amount."""
        sample = gold_fct_order_summary.head(200).copy()
        expected = (sample["gross_revenue"].astype(float)
                    + sample["option_revenue"].astype(float)
                    - sample["discount_amount"].astype(float))
        actual = sample["net_revenue"].astype(float)
        assert np.allclose(expected, actual, atol=0.01), "net_revenue formula mismatch"

    def test_no_null_revenue(self, gold_fct_order_summary):
        for col in ["gross_revenue", "net_revenue", "option_revenue", "discount_amount"]:
            assert gold_fct_order_summary[col].notna().all(), f"Null {col} in fct_order_summary"

    def test_referential_integrity_user(self, gold_fct_order_summary, gold_dim_customer):
        """Every user_id in facts should exist in dim_customer."""
        fact_users = set(gold_fct_order_summary["user_id"].unique())
        dim_users = set(gold_dim_customer["user_id"].unique())
        orphans = fact_users - dim_users
        assert len(orphans) == 0, f"Orphan user_ids in fct_order_summary: {orphans}"

    def test_referential_integrity_restaurant(self, gold_fct_order_summary, gold_dim_restaurant):
        """Every restaurant_id in facts should exist in dim_restaurant."""
        fact_rests = set(gold_fct_order_summary["restaurant_id"].unique())
        dim_rests = set(gold_dim_restaurant["restaurant_id"].unique())
        orphans = fact_rests - dim_rests
        assert len(orphans) == 0, f"Orphan restaurant_ids: {orphans}"


class TestGoldCustomerDaily:
    """Validate the daily LTV snapshot — the PRIMARY deliverable."""

    def test_no_null_cumulative_ltv(self, gold_fct_customer_daily):
        assert gold_fct_customer_daily["cumulative_ltv"].notna().all(), "Null cumulative_ltv"

    def test_clv_segment_domain(self, gold_fct_customer_daily):
        valid = {"High", "Medium", "Low"}
        actual = set(gold_fct_customer_daily["clv_segment"].unique())
        assert actual.issubset(valid), f"Invalid clv_segment values: {actual - valid}"

    def test_churn_risk_flag_domain(self, gold_fct_customer_daily):
        valid = {"Active", "At Risk", "Churned"}
        actual = set(gold_fct_customer_daily["churn_risk_flag"].unique())
        assert actual.issubset(valid), f"Invalid churn_risk_flag values: {actual - valid}"

    def test_cumulative_ltv_non_decreasing(self, gold_fct_customer_daily):
        """For each customer, cumulative_ltv should never decrease over time."""
        # Spot-check 50 random customers
        sample_users = gold_fct_customer_daily["user_id"].drop_duplicates().sample(
            min(50, gold_fct_customer_daily["user_id"].nunique()), random_state=42
        )
        for uid in sample_users:
            user_data = gold_fct_customer_daily[
                gold_fct_customer_daily["user_id"] == uid
            ].sort_values("date_key")
            ltv = user_data["cumulative_ltv"].astype(float).values
            diffs = np.diff(ltv)
            assert (diffs >= -0.01).all(), f"cumulative_ltv decreased for user {uid}"


class TestGoldRFM:
    """Validate RFM segmentation scores and labels."""

    def test_pk_unique(self, gold_fct_customer_rfm):
        assert gold_fct_customer_rfm["user_id"].is_unique, "Duplicate user_id in fct_customer_rfm"

    def test_scores_in_range(self, gold_fct_customer_rfm):
        for col in ["r_score", "f_score", "m_score"]:
            vals = gold_fct_customer_rfm[col]
            assert vals.min() >= 1, f"{col} has values below 1"
            assert vals.max() <= 5, f"{col} has values above 5"

    def test_rfm_segment_domain(self, gold_fct_customer_rfm):
        valid = {"VIP", "New Customer", "Churn Risk", "Regular"}
        actual = set(gold_fct_customer_rfm["rfm_segment"].unique())
        assert actual.issubset(valid), f"Invalid rfm_segment values: {actual - valid}"

    def test_row_count_matches_dim_customer(self, gold_fct_customer_rfm, gold_dim_customer):
        """RFM should have one row per customer (single snapshot)."""
        assert len(gold_fct_customer_rfm) == len(gold_dim_customer), \
            f"RFM rows ({len(gold_fct_customer_rfm)}) != dim_customer rows ({len(gold_dim_customer)})"
