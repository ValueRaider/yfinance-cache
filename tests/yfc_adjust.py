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
import pickle as pkl

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

        answer = pd.read_csv("./tests/Adjustment/TestCase_deAdjust/pnl-answer.csv",parse_dates=["Date"],index_col="Date")
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
                    print("{}/{} diffs in column {}".format(sum(f), answer.shape[0], dc))
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


    def test_adjust_append(self):
        # Have some data cached, e.g. Jan->May. Then a later fetch should append and back-adjust correctly

        tkr = "PNL.L"
        dat = yfc.Ticker(tkr, self.session)
        tz = dat.info["exchangeTimezoneName"]

        cached_df = pd.read_csv("./tests/Adjustment/TestCase_append/cached.csv",parse_dates=["Date","FetchDate"],index_col="Date")
        cached_df.index = cached_df.index.tz_convert(tz)
        last_adjust_dt = datetime.combine(date(2022,7,28), time(15,30), ZoneInfo(tz))
        cache_dp = os.path.join(self.tempCacheDir.name, tkr)
        with open(os.path.join(cache_dp, "history-1d.pkl"), 'wb') as f:
            pkl.dump({"data":cached_df, "metadata":{"LastAdjustDt":last_adjust_dt}}, f, 4)

        answer_noadjust = pd.read_csv("./tests/Adjustment/TestCase_append/pnl-answer-noadjust.csv",parse_dates=["Date"],index_col="Date")
        answer_noadjust.index = answer_noadjust.index.tz_localize(tz)

        df = dat.history(start="2022-01-01", end="2022-08-20", adjust_divs=False, adjust_splits=False)
        dcs = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
        for dc in dcs:
            f = np.isclose(df[dc].values, answer_noadjust[dc].values, rtol=1e-10)
            try:
                self.assertTrue(f.all())
            except:
                f = ~f
                print("{}/{} diffs in column {}".format(sum(f), df.shape[0], dc))
                if sum(f) < 10:
                    print("Differences in column {}:".format(dc))
                    print("- answer_noadjust:")
                    print(answer_noadjust[f][[dc]])
                    print("- result:")
                    print(df[f][[dc]])
                    last_diff_idx = np.where(f)[0][-1]
                    last_diff = df[dc][last_diff_idx] - answer_noadjust[dc][last_diff_idx]
                    print("- last_diff = {}".format(last_diff))

                else:
                    last_idx = np.where(f)[0][-1]
                    print("- last diff: {}".format(df.index[last_idx]))
                    x = df[dc][last_idx]
                    y = answer_noadjust[dc][last_idx]
                    print("- response={} - answer={} = {}".format(x, y, x-y))
                raise

        df = dat.history(start="2022-01-01",end="2022-08-20",auto_adjust=False)
        answer_splitAdjusted = yf.Ticker(tkr, self.session).history(start="2022-01-01",end="2022-08-20",adjust_divs=False)
        dcs = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
        for dc in dcs:
            f = np.isclose(df[dc].values, answer_splitAdjusted[dc].values, rtol=5e-6)
            try:
                self.assertTrue(f.all())
            except:
                f = ~f
                print("{}/{} diffs in column {}".format(sum(f), df.shape[0], dc))
                if sum(f) < 10:
                    print("Differences in column {}:".format(dc))
                    print("- answer_splitAdjusted:")
                    print(answer_splitAdjusted[f][[dc]])
                    print("- result:")
                    print(df[f][[dc]])
                    last_diff_idx = np.where(f)[0][-1]
                    last_diff = df[dc][last_diff_idx] - answer_splitAdjusted[dc][last_diff_idx]
                    print("- last_diff = {}".format(last_diff))

                else:
                    last_idx = np.where(f)[0][-1]
                    print("- last diff: {}".format(df.index[last_idx]))
                    x = df[dc][last_idx]
                    y = answer_splitAdjusted[dc][last_idx]
                    print("- response={} - answer={} = {}".format(x, y, x-y))
                    print(df.iloc[last_idx])
                raise

        df = dat.history(start="2022-01-01",end="2022-08-20")
        answer_adjusted = yf.Ticker(tkr, self.session).history(start="2022-01-01",end="2022-08-20")
        dcs = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
        for dc in dcs:
            f = np.isclose(df[dc].values, answer_adjusted[dc].values, rtol=5e-6)
            try:
                self.assertTrue(f.all())
            except:
                f = ~f
                print("{}/{} diffs in column {}".format(sum(f), df.shape[0], dc))
                if sum(f) < 10:
                    print("Differences in column {}:".format(dc))
                    print("- answer_adjusted:")
                    print(answer_adjusted[f][[dc]])
                    print("- result:")
                    print(df[f][[dc]])
                    last_diff_idx = np.where(f)[0][-1]
                    last_diff = df[dc][last_diff_idx] - answer_adjusted[dc][last_diff_idx]
                    print("- last_diff = {}".format(last_diff))

                else:
                    last_idx = np.where(f)[0][-1]
                    print("- last diff: {}".format(df.index[last_idx]))
                    x = df[dc][last_idx]
                    y = answer_adjusted[dc][last_idx]
                    print("- response={} - answer={} = {}".format(x, y, x-y))
                raise


    def test_adjust_prepend1(self):
        # Fetch 1st Aug (split day) -> now, then Jan -> now
        
        tkr = "PNL.L"
        dat = yfc.Ticker(tkr, self.session)
        tz = dat.info["exchangeTimezoneName"]

        df1 = dat.history(start="2022-08-01", end="2022-08-20", adjust_divs=False, adjust_splits=False)
        df = dat.history(start="2022-01-01", end="2022-08-20", adjust_divs=False, adjust_splits=False)

        answer_noadjust = pd.read_csv("./tests/Adjustment/TestCase_prepend/pnl-answer-noadjust.csv",parse_dates=["Date"],index_col="Date")
        answer_noadjust.index = answer_noadjust.index.tz_localize(tz)

        dcs = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
        for dc in dcs:
            f = np.isclose(df[dc].values, answer_noadjust[dc].values, rtol=1e-10)
            try:
                self.assertTrue(f.all())
            except:
                f = ~f
                if sum(f) < 10:
                    print("Differences in column {}:".format(dc))
                    print("- answer_noadjust:")
                    print(answer_noadjust[f][[dc]])
                    print("- result:")
                    print(df[f][[dc]])
                    last_diff_idx = np.where(f)[0][-1]
                    last_diff = df[dc][last_diff_idx] - answer_noadjust[dc][last_diff_idx]
                    print("- last_diff = {}".format(last_diff))

                else:
                    print("{}/{} diffs in column {}".format(sum(f), df.shape[0], dc))
                raise

    def test_adjust_prepend2(self):
        # Fetch 2nd Aug (after split day) -> now, then Jan -> now
        
        tkr = "PNL.L"
        dat = yfc.Ticker(tkr, self.session)
        tz = dat.info["exchangeTimezoneName"]

        df1 = dat.history(start="2022-08-02", end="2022-08-20", adjust_divs=False, adjust_splits=False)
        df = dat.history(start="2022-01-01", end="2022-08-20", adjust_divs=False, adjust_splits=False)

        answer_noadjust = pd.read_csv("./tests/Adjustment/TestCase_prepend/pnl-answer-noadjust.csv",parse_dates=["Date"],index_col="Date")
        answer_noadjust.index = answer_noadjust.index.tz_localize(tz)

        dcs = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
        for dc in dcs:
            f = np.isclose(df[dc].values, answer_noadjust[dc].values, rtol=1e-10)
            try:
                self.assertTrue(f.all())
            except:
                f = ~f
                if sum(f) < 10:
                    print("Differences in column {}:".format(dc))
                    print("- answer_noadjust:")
                    print(answer_noadjust[f][[dc]])
                    print("- result:")
                    print(df[f][[dc]])
                    last_diff_idx = np.where(f)[0][-1]
                    last_diff = df[dc][last_diff_idx] - answer_noadjust[dc][last_diff_idx]
                    print("- last_diff = {}".format(last_diff))

                else:
                    print("{}/{} diffs in column {}".format(sum(f), df.shape[0], dc))
                raise

    def test_adjust_prepend3(self):
        # Fetch 28th July (before split day) -> now, then Jan -> now
        
        tkr = "PNL.L"
        dat = yfc.Ticker(tkr, self.session)
        tz = dat.info["exchangeTimezoneName"]

        df1 = dat.history(start="2022-07-29", end="2022-08-20", adjust_divs=False, adjust_splits=False)
        df = dat.history(start="2022-01-01", end="2022-08-20", adjust_divs=False, adjust_splits=False)

        answer_noadjust = pd.read_csv("./tests/Adjustment/TestCase_prepend/pnl-answer-noadjust.csv",parse_dates=["Date"],index_col="Date")
        answer_noadjust.index = answer_noadjust.index.tz_localize(tz)

        dcs = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
        for dc in dcs:
            f = np.isclose(df[dc].values, answer_noadjust[dc].values, rtol=1e-10)
            try:
                self.assertTrue(f.all())
            except:
                f = ~f
                if sum(f) < 10:
                    print("Differences in column {}:".format(dc))
                    print("- answer_noadjust:")
                    print(answer_noadjust[f][[dc]])
                    print("- result:")
                    print(df[f][[dc]])
                    last_diff_idx = np.where(f)[0][-1]
                    last_diff = df[dc][last_diff_idx] - answer_noadjust[dc][last_diff_idx]
                    print("- last_diff = {}".format(last_diff))

                else:
                    print("{}/{} diffs in column {}".format(sum(f), df.shape[0], dc))
                raise


    def test_adjust_insertion1(self):
        # Test that insertion works
        # I.e. have in cache: Jan->Mar & May->present, 
        # and fetch/insert Mar->May (has split)
        return

    def test_adjust_insertion2(self):
        # Test that insertion works
        # I.e. have in cache: Jan->Mar & May->present (has split), 
        # and fetch/insert Mar->May
        return

    def test_adjust_insertion3(self):
        # Test that insertion works
        # I.e. have in cache: Jan->Mar & May->present (has split), 
        # and fetch/insert Mar->May (has split)
        return


if __name__ == '__main__':
    unittest.main()
