import unittest

import sys ; sys.path.insert(0, "/home/gonzo/ReposForks/yfinance-ValueRaider.integrate")
import yfinance as yf
# from .context import yfc_dat as yfcd
# from .context import yfc_time as yfct
# from .context import yfc_cache_manager as yfcm
# from .context import yfc_utils as yfcu
from .context import yfc_ticker as yfc


from .context import yfc_cache_manager as yfcm
from .context import yfc_dat as yfcd
from .context import yfc_utils as yfcu

import tempfile
import pandas as pd
import numpy as np

from time import sleep
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
import os
import requests_cache

from pprint import pprint




class Test_Unadjust(unittest.TestCase):

    def setUp(self):
        self.session = requests_cache.CachedSession(os.path.join(yfcu.GetUserCacheDirpath(),'yfinance.cache'))

        self.tempCacheDir = tempfile.TemporaryDirectory()
        yfcm.SetCacheDirpath(self.tempCacheDir.name)

    def tearDown(self):
        self.tempCacheDir.cleanup()
        self.session.close()


    ## TODO: extend start date to earlier to contain more events


    def test_deAdjust(self):
        tkr = "PNL.L"
        dat = yfc.Ticker(tkr, self.session)
        tz = dat.info["exchangeTimezoneName"]

        answer = pd.read_csv("./tests/Adjustment/Unadjust/pnl-answer.csv",parse_dates=["Date"],index_col="Date")
        answer.index = answer.index.tz_localize(tz)

        df = dat.history(start="2022-05-03", end="2022-08-19", adjust_splits=False, adjust_divs=False)

        dcs = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
        for dc in dcs:
            try:
                self.assertTrue(np.isclose(df[dc].values, answer[dc].values, rtol=1e-5).all())
            except:
                f = ~np.isclose(df[dc].values, answer[dc].values, rtol=1e-5)
                if sum(f) < 5:
                    print("Differences in column {}:".format(dc))
                    print("- answer:")
                    print(answer[f][[dc]])
                    print("- result:")
                    print(df[f][[dc]])
                else:
                    print("{}/{} diffs in column {}".format(sum(f), df.shape[0], dc))
                raise


    def test_adjust_simple(self):
        # YFC's internal adjustment should match YF:

        tkr = "PNL.L"

        dat = yf.Ticker(tkr, self.session)
        answer = dat.history(start="2022-05-03", end="2022-08-19")

        dat = yfc.Ticker(tkr, self.session)
        result = dat.history(start="2022-05-03", end="2022-08-19")

        dcs = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
        for dc in dcs:
            try:
                self.assertTrue(np.isclose(result[dc].values, answer[dc].values, rtol=1e-5).all())
            except:
                f = ~np.isclose(result[dc].values, answer[dc].values, rtol=1e-5)
                if sum(f) < 5:
                    print("Differences here:")
                    print("- answer:")
                    print(answer[f][[dc]])
                    print("- result:")
                    print(result[f][[dc]])
                else:
                    print("{}/{} rows mismatch".format(sum(f), result.shape[0]))
                    print("- answer:")
                    print(answer[f][[dc]])
                    print("- result:")
                    print(result[f][[dc]])
                raise


    def test_adjust_complex1(self):
        # Fetch May->August, then fetch Jan->May.
        # Test that the second fetch is correctly adjusted/deAdjusted
        return


    def test_adjust_complex(self):
        # Have some data cached, e.g. Jan->May. Then a later fetch 
        # should append and back-adjust correctly
        return


if __name__ == '__main__':
    unittest.main()
