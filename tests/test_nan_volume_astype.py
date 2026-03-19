"""Tests for safe Volume integer conversion when data contains NaN or inf."""

import numpy as np
import pandas as pd

from yfinance_cache.yfc_utils import safe_int


def _build_price_df(volume_values, csf_values=None):
    n = len(volume_values)
    return pd.DataFrame(
        {
            "Open": np.random.uniform(100, 200, n),
            "High": np.random.uniform(200, 300, n),
            "Low": np.random.uniform(50, 100, n),
            "Close": np.random.uniform(100, 200, n),
            "Dividends": np.zeros(n),
            "Volume": pd.Series(volume_values, dtype="float64"),
            "CSF": pd.Series(
                csf_values if csf_values is not None else [2.0] * n,
                dtype="float64",
            ),
        }
    )


class TestSafeInt:
    def test_nan_becomes_nan(self):
        result = safe_int(pd.Series([1_000_000.0, np.nan, 750_000.0]))
        assert result.dtype == np.float64
        assert result[0] == 1_000_000.0
        assert np.isnan(result[1])
        assert result[2] == 750_000.0

    def test_inf_becomes_nan(self):
        result = safe_int(pd.Series([500_000.0, np.inf, 250_000.0]))
        assert result[0] == 500_000.0
        assert np.isnan(result[1])
        assert result[2] == 250_000.0

    def test_neg_inf_becomes_nan(self):
        result = safe_int(pd.Series([500_000.0, -np.inf, 250_000.0]))
        assert np.isnan(result[1])

    def test_clean_data_unchanged(self):
        result = safe_int(pd.Series([1_000_000.0, 500_000.0, 750_000.0]))
        assert list(result) == [1_000_000.0, 500_000.0, 750_000.0]

    def test_values_are_rounded(self):
        result = safe_int(pd.Series([1_000_000.7, 500_000.3]))
        assert result[0] == 1_000_001.0
        assert result[1] == 500_000.0

    def test_result_compatible_with_numpy_isnan(self):
        result = safe_int(pd.Series([500_000.0, np.inf, np.nan]))
        assert np.isnan(result.to_numpy()).sum() == 2


class TestSplitAdjustmentWithNonFiniteVolume:
    """Volume split adjustment should produce NaN for non-finite values, not crash."""

    def _adjust(self, df):
        h = df.copy()
        for c in ["Open", "High", "Low", "Close", "Dividends"]:
            h[c] *= h["CSF"]
        h["Volume"] = safe_int(h["Volume"] / h["CSF"])
        return h

    def test_nan_volume(self):
        h = self._adjust(_build_price_df([1_000_000, 500_000, np.nan, 750_000]))
        assert h["Volume"][0] == 500_000.0
        assert h["Volume"][1] == 250_000.0
        assert np.isnan(h["Volume"][2])
        assert h["Volume"][3] == 375_000.0

    def test_inf_volume(self):
        h = self._adjust(_build_price_df([1_000_000, np.inf, 500_000]))
        assert h["Volume"][0] == 500_000.0
        assert np.isnan(h["Volume"][1])
        assert h["Volume"][2] == 250_000.0

    def test_zero_csf(self):
        h = self._adjust(_build_price_df([1_000_000, 500_000], csf_values=[2.0, 0.0]))
        assert h["Volume"][0] == 500_000.0
        assert np.isnan(h["Volume"][1])
