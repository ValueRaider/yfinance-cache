import unittest
import requests_cache
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import tempfile
from pprint import pprint
from datetime import datetime, date

import yfinance as yf

from .utils import *
from .context import session_gbl
from .context import yfc_ticker as yfc
from .context import yfc_cache_manager as yfcm
from .context import yfc_financials_manager as yfcf
from .context import yfc_dat as yfcd

class Test_Yfc_DateEstimates(unittest.TestCase):
    def test_lt_date(self):
        # High confidence
        d = yfcf.DateEstimate(date(2023, 1, 1), yfcd.Confidence.High)
        ## Far apart
        d1 = date(2024, 1, 1)
        self.assertTrue(d < d1)
        ## Adjacent
        d = date(2023, 2, 1)
        self.assertTrue(d < d1)
        ## False
        d1 = date(2022, 12, 1)
        self.assertFalse(d < d1)

        # Medium confidence
        d = yfcf.DateEstimate(date(2023, 1, 1), yfcd.Confidence.Medium)
        ## Far apart
        d1 = date(2024, 1, 1)
        self.assertTrue(d < d1)
        ## Nearby
        d1 = date(2023, 3, 1)
        self.assertTrue(d < d1)
        ## False
        d1 = date(2022, 12, 1)
        self.assertFalse(d < d1)
        ## Ambiguous
        d1 = date(2023, 1, 2)
        self.assertRaises(Exception, d.__lt__, d1)

    def test_lt_dateEstimate(self):
        # High confidence
        ## Far apart
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        r1 = yfcf.DateEstimate(date(2024, 1, 1), yfcd.Confidence.High)
        self.assertTrue(r0 < r1)
        ## Adjacent
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        r1 = yfcf.DateEstimate(date(2023, 2, 1), yfcd.Confidence.High)
        self.assertTrue(r0 < r1)
        ## False
        self.assertFalse(r1 < r0)

        # Medium confidence
        ## Far apart
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        r1 = yfcf.DateEstimate(date(2024, 1, 1), yfcd.Confidence.Medium)
        self.assertTrue(r0 < r1)
        ## Nearby
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        r1 = yfcf.DateEstimate(date(2023, 3, 1), yfcd.Confidence.Medium)
        self.assertTrue(r0 < r1)
        ## False
        self.assertFalse(r1 < r0)
        # ## Ambiguous because of medium confidence
        # r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        # r1 = yfcf.DateRangeEstimate(date(2023, 2, 1), date(2023, 3, 1), yfcd.Confidence.Medium)
        # self.assertFalse(r1 < r0)

    def test_lt_dateRange(self):
        # High confidence
        ## Far apart
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        r1 = yfcf.DateRangeEstimate(date(2024, 1, 1), date(2024, 2, 1), yfcd.Confidence.High)
        self.assertTrue(r0 < r1)
        ## Adjacent
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        r1 = yfcf.DateRangeEstimate(date(2023, 2, 1), date(2023, 3, 1), yfcd.Confidence.High)
        self.assertTrue(r0 < r1)
        ## False
        self.assertFalse(r1 < r0)

        # Medium confidence
        ## Far apart
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        r1 = yfcf.DateRangeEstimate(date(2024, 1, 1), date(2024, 2, 1), yfcd.Confidence.Medium)
        self.assertTrue(r0 < r1)
        ## Nearby
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        r1 = yfcf.DateRangeEstimate(date(2023, 3, 1), date(2023, 4, 1), yfcd.Confidence.Medium)
        self.assertTrue(r0 < r1)
        ## False
        self.assertFalse(r1 < r0)
        # ## Ambiguous because of medium confidence
        # r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        # r1 = yfcf.DateRangeEstimate(date(2023, 2, 1), date(2023, 3, 1), yfcd.Confidence.Medium)
        # self.assertFalse(r1 < r0)

    def test_le(self):
        # High confidence
        ## Far apart
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        r1 = yfcf.DateRangeEstimate(date(2024, 1, 1), date(2024, 2, 1), yfcd.Confidence.High)
        self.assertTrue(r0 <= r1)
        ## Adjacent
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        r1 = yfcf.DateRangeEstimate(date(2023, 2, 1), date(2023, 3, 1), yfcd.Confidence.High)
        self.assertTrue(r0 <= r1)
        ## Equal
        self.assertTrue(r0 <= r1)
        ## False
        self.assertFalse(r1 <= r0)


class Test_Yfc_DateRangeEstimates(unittest.TestCase):
    def test_lt_date(self):
        # High confidence
        ## Far apart
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        d = date(2024, 1, 1)
        self.assertTrue(r0 < d)
        ## Adjacent
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        d = date(2023, 2, 1)
        self.assertTrue(r0 < d)
        ## False
        d = date(2022, 12, 1)
        self.assertFalse(r0 < d)

        # Medium confidence
        ## Far apart
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        d = date(2024, 1, 1)
        self.assertTrue(r0 < d)
        ## Nearby
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        d = date(2023, 3, 1)
        self.assertTrue(r0 < d)
        ## False
        d = date(2022, 12, 1)
        self.assertFalse(r0 < d)
        # ## Ambiguous because of medium confidence
        # r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        # d = date(2023, 2, 1)
        # self.assertFalse(r0 < d)

    def test_lt_dateEstimate(self):
        # High confidence
        ## Far apart
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        r1 = yfcf.DateEstimate(date(2024, 1, 1), yfcd.Confidence.High)
        self.assertTrue(r0 < r1)
        ## Adjacent
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        r1 = yfcf.DateEstimate(date(2023, 2, 1), yfcd.Confidence.High)
        self.assertTrue(r0 < r1)
        ## False
        self.assertFalse(r1 < r0)

        # Medium confidence
        ## Far apart
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        r1 = yfcf.DateEstimate(date(2024, 1, 1), yfcd.Confidence.Medium)
        self.assertTrue(r0 < r1)
        ## Nearby
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        r1 = yfcf.DateEstimate(date(2023, 3, 1), yfcd.Confidence.Medium)
        self.assertTrue(r0 < r1)
        ## False
        self.assertFalse(r1 < r0)
        # ## Ambiguous because of medium confidence
        # r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        # r1 = yfcf.DateRangeEstimate(date(2023, 2, 1), date(2023, 3, 1), yfcd.Confidence.Medium)
        # self.assertFalse(r1 < r0)

    def test_lt_dateRange(self):
        # High confidence
        ## Far apart
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        r1 = yfcf.DateRangeEstimate(date(2024, 1, 1), date(2024, 2, 1), yfcd.Confidence.High)
        self.assertTrue(r0 < r1)
        ## Adjacent
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        r1 = yfcf.DateRangeEstimate(date(2023, 2, 1), date(2023, 3, 1), yfcd.Confidence.High)
        self.assertTrue(r0 < r1)
        ## False
        self.assertFalse(r1 < r0)

        # Medium confidence
        ## Far apart
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        r1 = yfcf.DateRangeEstimate(date(2024, 1, 1), date(2024, 2, 1), yfcd.Confidence.Medium)
        self.assertTrue(r0 < r1)
        ## Nearby
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        r1 = yfcf.DateRangeEstimate(date(2023, 3, 1), date(2023, 4, 1), yfcd.Confidence.Medium)
        self.assertTrue(r0 < r1)
        ## False
        self.assertFalse(r1 < r0)
        # ## Ambiguous because of medium confidence
        # r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.Medium)
        # r1 = yfcf.DateRangeEstimate(date(2023, 2, 1), date(2023, 3, 1), yfcd.Confidence.Medium)
        # self.assertFalse(r1 < r0)

    def test_le(self):
        # High confidence
        ## Far apart
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        r1 = yfcf.DateRangeEstimate(date(2024, 1, 1), date(2024, 2, 1), yfcd.Confidence.High)
        self.assertTrue(r0 <= r1)
        ## Adjacent
        r0 = yfcf.DateRangeEstimate(date(2023, 1, 1), date(2023, 2, 1), yfcd.Confidence.High)
        r1 = yfcf.DateRangeEstimate(date(2023, 2, 1), date(2023, 3, 1), yfcd.Confidence.High)
        self.assertTrue(r0 <= r1)
        ## Equal
        self.assertTrue(r0 <= r1)
        ## False
        self.assertFalse(r1 <= r0)



# class Test_Yfc_Financials(Test_Base):
class Test_Yfc_Financials(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempCacheDir = tempfile.TemporaryDirectory()
        yfcm.SetCacheDirpath(cls.tempCacheDir.name)
        cls.session = session_gbl
        cls.T = 'INTC'
        cls.dat = yfc.Ticker(cls.T, cls.session)
        cls.exchange, cls.tz_name = cls.dat._getExchangeAndTz()
        cls.fin = yfcf.FinancialsManager(cls.T, cls.exchange, cls.tz_name, session=cls.session)

        cls.dat_yf = yf.Ticker(cls.T, cls.session)

    def tearDown(self):
        self.tempCacheDir.cleanup()
        self.session.close()

    # Calendar, True or False
    def test_calendar_noRefresh(self):
        cal = self.fin.get_calendar(refresh=False)
        self.assertIsNone(cal)

    def test_calendar_withRefresh(self):
        # Value matches Yahoo
        cal = self.fin.get_calendar(refresh=True)
        cal_yf = self.dat_yf.calendar ; cal_yf['FetchDate'] = cal['FetchDate']
        self.assertEqual(cal, cal_yf)

        # Next get doesn't modify cache
        cache_state_before = take_directory_snapshot(self.tempCacheDir.name)
        cal = self.fin.get_calendar(refresh=True)
        cache_state_after = take_directory_snapshot(self.tempCacheDir.name)
        self.assertEqual(cache_state_before, cache_state_after)

        # Only 1 web request - test works, but disable to reduce spam
        # with requests_cache.CachedSession(backend="memory") as session:
        #     fin = yfcf.FinancialsManager(self.T, self.exchange, self.tz_name, session=session)
        #     fin.get_calendar(refresh=True)
        #     actual_urls_called = [r.url for r in session.cache.filter()]
        # # Remove 'crumb' argument
        # for i in range(len(actual_urls_called)):
        #     u = actual_urls_called[i]
        #     parsed_url = urlparse(u)
        #     query_params = parse_qs(parsed_url.query)
        #     query_params.pop('crumb', None)
        #     query_params.pop('cookie', None)
        #     u = urlunparse(parsed_url._replace(query=urlencode(query_params, doseq=True)))
        #     actual_urls_called[i] = u
        # self.assertEqual(len(actual_urls_called), 1)

    # tg_caldts, True or False
    def _test_tagged_calendar_dates_noRefresh(self):
        cal_dts = self.fin._get_tagged_calendar_dates(refresh=False)
        pprint(cal_dts)
        self.assertTrue(cal_dts is None or len(cal_dts)==0)
    def _test_tagged_calendar_dates_withRefresh(self):
        # Preset the cache
        cal = {'Earnings Date': [date(2024, 4, 26), date(2024, 4, 30)], 'FetchDate': pd.Timestamp.utcnow()}
        yfcm.StoreCacheDatum(self.T, "calendar", cal)

        cal_dts = self.fin._get_tagged_calendar_dates(refresh=True)
        for x in cal_dts:
            pprint(x)

    # fetch_dts, True or False

    # edts_F

    # fintbl_F
    # bs_F
    # int_F
    # icstmt_F
    # cf_F

    # reldts_F

    # fintbl_T
    # cf_T
    # bs_T
    # int_T
    # icstmt_T

    # edts_T

    # reldts_T

    # e_int_T

    # e_int_F
