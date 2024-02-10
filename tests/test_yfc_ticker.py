import unittest
import tempfile
from pprint import pprint

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct
from .context import yfc_cache_manager as yfcm
from .context import yfc_utils as yfcu
from .context import yfc_ticker as yfc
from .context import session_gbl
from .utils import Test_Base
import pickle as pkl

import yfinance as yf

import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
import os

class Test_Yfc_Ticker(Test_Base):

    def setUp(self):
        self.tempCacheDir = tempfile.TemporaryDirectory()
        yfcm.SetCacheDirpath(self.tempCacheDir.name)

        self.session = session_gbl

        self.tkrs = ["MEL.NZ", "BHG.JO", "INTC"]
        self.tkrs.append("HLTH") # Listed recently
        self.tkrs.append("GME") # Stock split recently
        self.tkrs.append("BHP.AX") # ASX market has auction
        # self.tkrs.append("ICL.TA") # TLV market has auction and odd times  # disabling until I restore 'yahooWeeklyDef'

        self.usa_tkr = "INTC"
        self.usa_market = "us_market"
        self.usa_exchange = "NMS"
        self.usa_market_tz_name = 'US/Eastern'
        self.usa_market_tz = ZoneInfo(self.usa_market_tz_name)
        self.usa_market_open_time  = time(hour=9, minute=30)
        self.usa_market_close_time = time(hour=16, minute=0)
        self.usa_dat = yfc.Ticker(self.usa_tkr, session=self.session)

        self.nze_tkr = "MEL.NZ"
        self.nze_market = "nz_market"
        self.nze_exchange = "NZE"
        self.nze_market_tz_name = 'Pacific/Auckland'
        self.nze_market_tz = ZoneInfo(self.nze_market_tz_name)
        self.nze_market_open_time  = time(hour=10, minute=0)
        self.nze_market_close_time = time(hour=16, minute=45)
        self.nze_dat = yfc.Ticker(self.nze_tkr, session=self.session)


    def tearDown(self):
        self.tempCacheDir.cleanup()
        self.session.close()


    def test_info(self):
        fp = yfcm.GetFilepath(self.usa_tkr, 'info')
        self.assertIsNone(fp)

        i1 = self.usa_dat.info
        self.assertIsNotNone(i1)

        fp = yfcm.GetFilepath(self.usa_tkr, 'info')
        self.assertIsNotNone(fp)
        self.assertTrue(os.path.isfile(fp))
        mod_dt1 = datetime.fromtimestamp(os.path.getmtime(fp))

        i2 = self.usa_dat.info
        self.assertEqual(i1, i2)
        mod_dt2 = datetime.fromtimestamp(os.path.getmtime(fp))
        self.assertEqual(mod_dt1, mod_dt2)

        # Simulate aging
        yfcm._option_manager.max_ages.info = '1s'
        sleep(2)
        i3 = self.usa_dat.info
        mod_dt3 = datetime.fromtimestamp(os.path.getmtime(fp))
        self.assertNotEqual(mod_dt3, mod_dt1)


    def test_calendar(self):
        fp = yfcm.GetFilepath(self.usa_tkr, 'calendar')
        self.assertIsNone(fp)

        i1 = self.usa_dat.calendar
        self.assertIsNotNone(i1)

        fp = yfcm.GetFilepath(self.usa_tkr, 'calendar')
        self.assertIsNotNone(fp)
        self.assertTrue(os.path.isfile(fp))
        mod_dt1 = datetime.fromtimestamp(os.path.getmtime(fp))

        i2 = self.usa_dat.calendar
        self.assertEqual(i1, i2)
        mod_dt2 = datetime.fromtimestamp(os.path.getmtime(fp))
        self.assertEqual(mod_dt1, mod_dt2)

        # Simulate aging
        yfcm._option_manager.max_ages.calendar = '1s'
        sleep(2)
        i3 = self.usa_dat.calendar
        mod_dt3 = datetime.fromtimestamp(os.path.getmtime(fp))
        self.assertNotEqual(mod_dt3, mod_dt1)


    def test_get_shares(self):
        df1 = self.usa_dat.get_shares('2023-01-01', '2023-06-01')

        df2 = self.usa_dat.get_shares('2023-01-01', '2023-06-01')
        self.assertTrue(df1.equals(df2))

        # Fetch more after
        df3 = self.usa_dat.get_shares('2023-01-01', '2024-01-01')
        # Fetch first range again
        df4 = self.usa_dat.get_shares('2023-01-01', '2023-06-01')
        self.assertTrue(df1.equals(df4))

        # Fetch more before
        df5 = self.usa_dat.get_shares('2022-06-01', '2023-06-01')
        # Fetch first range again
        df6 = self.usa_dat.get_shares('2023-01-01', '2023-06-01')
        self.assertTrue(df1.equals(df6))


if __name__ == '__main__':
    unittest.main()
