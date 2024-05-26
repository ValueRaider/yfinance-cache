import unittest

import yfinance as yf
from .context import yfc_dat as yfcd
from .context import yfc_time as yfct
from .context import yfc_cache_manager as yfcm
from .context import yfc_utils as yfcu
from .context import yfc_ticker as yfc
from .context import session_gbl
from .utils import Test_Base
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
import appdirs


class Test_Unadjust(Test_Base):
    def setUp(self):
        self.tkrs = ["PNL.L", "I3E.L", "INTC", "GME", "AMC", "ESLT.TA"]

        self.session = session_gbl

        self.tempCacheDir = tempfile.TemporaryDirectory()
        yfcm.SetCacheDirpath(self.tempCacheDir.name)

    def tearDown(self):
        self.tempCacheDir.cleanup()
        self.session.close()

    def test_cleanFetch(self):
        # Fetch with empty cache, no adjustment

        tkr = "PNL.L"
        dat = yfc.Ticker(tkr, self.session)
        tz = dat.fast_info["timezone"]

        answer = pd.read_csv("./tests/Adjustment/pnl-unadjusted.csv",parse_dates=["Date"],index_col="Date")
        if not isinstance(answer.index, pd.DatetimeIndex):
            answer.index = pd.to_datetime(answer.index, utc=True)
            answer.index = answer.index.tz_convert(tz)
        else:
            answer.index = answer.index.tz_localize(tz)

        df = dat.history(start="2022-01-04", end="2022-08-20", adjust_splits=False, adjust_divs=False)
        self.verify_df(df, answer, 5e-5)


    def test_adjust_simple(self):
        # Fetch with empty cache, adjustment should match YF

        tkr = "PNL.L"

        start_d = "2022-05-03"
        end_d = "2022-08-19"

        dat = yf.Ticker(tkr, self.session)
        answer = dat.history(start=start_d, end=end_d)

        dat = yfc.Ticker(tkr, self.session)
        result = dat.history(start=start_d, end=end_d)

        self.verify_df(result, answer, 5e-5)


    def test_adjust_append1(self):
        # Have Jan->Jul cached. Fetch August (stock split), should append and back-adjust correctly

        tkr = "PNL.L"
        dat = yfc.Ticker(tkr, self.session)
        tz = dat.fast_info["timezone"]

        dt_cols = ["Date","FetchDate"]
        dt_cols += ["LastDivAdjustDt", "LastSplitAdjustDt"]
        cached_df = pd.read_csv("./tests/Adjustment/TestCase_append/cached1.csv",parse_dates=dt_cols,index_col="Date")
        if not isinstance(cached_df.index, pd.DatetimeIndex):
            cached_df.index = pd.to_datetime(cached_df.index, utc=True)
        cached_df.index = cached_df.index.tz_convert(tz)
        last_adjust_dt = datetime.combine(date(2022,7,30), time(12,0), ZoneInfo(tz)).astimezone(ZoneInfo('UTC'))
        cache_dp = os.path.join(self.tempCacheDir.name, tkr)
        with open(os.path.join(cache_dp, "history-1d.pkl"), 'wb') as f:
            # md = {"LastDivAdjustDt":last_adjust_dt, "LastSplitAdjustDt":last_adjust_dt}
            md = {}
            cached_df["LastDivAdjustDt"] = last_adjust_dt
            cached_df["LastSplitAdjustDt"] = last_adjust_dt
            pkl.dump({"data":cached_df, "metadata":md}, f, 4)

        answer_noadjust = pd.read_csv("./tests/Adjustment/pnl-unadjusted.csv",parse_dates=["Date"],index_col="Date")
        if not isinstance(answer_noadjust.index, pd.DatetimeIndex):
            answer_noadjust.index = pd.to_datetime(answer_noadjust.index, utc=True)
        answer_noadjust.index = answer_noadjust.index.tz_convert(tz)

        df = dat.history(start="2022-01-01", end="2022-08-20", adjust_divs=False, adjust_splits=False)
        self.verify_df(df, answer_noadjust, 1e-10)

        df = dat.history(start="2022-01-01",end="2022-08-20",adjust_divs=False)
        answer_splitAdjusted = yf.Ticker(tkr, self.session).history(start="2022-01-01",end="2022-08-20",auto_adjust=False)
        self.verify_df(df, answer_splitAdjusted, 5e-5)

        df = dat.history(start="2022-01-01",end="2022-08-20")
        answer_adjusted = yf.Ticker(tkr, self.session).history(start="2022-01-01",end="2022-08-20")
        self.verify_df(df, answer_adjusted, 9e-6)

    def test_adjust_append2(self):
        # Have Jan->May cached. Fetch Jun->August (dividends & stock split), should append and back-adjust correctly

        tkr = "PNL.L"
        dat = yfc.Ticker(tkr, self.session)
        tz = dat.fast_info["timezone"]

        dt_cols = ["Date","FetchDate"]
        dt_cols += ["LastDivAdjustDt", "LastSplitAdjustDt"]
        cached_df = pd.read_csv("./tests/Adjustment/TestCase_append/cached2.csv",parse_dates=dt_cols,index_col="Date")
        if not isinstance(cached_df.index, pd.DatetimeIndex):
            cached_df.index = pd.to_datetime(cached_df.index, utc=True)
        cached_df.index = cached_df.index.tz_convert(tz)
        last_adjust_dt = datetime.combine(date(2022,5,28), time(12,0), ZoneInfo(tz)).astimezone(ZoneInfo('UTC'))
        cache_dp = os.path.join(self.tempCacheDir.name, tkr)
        with open(os.path.join(cache_dp, "history-1d.pkl"), 'wb') as f:
            # md = {"LastDivAdjustDt":last_adjust_dt, "LastSplitAdjustDt":last_adjust_dt}
            md = {}
            cached_df["LastDivAdjustDt"] = last_adjust_dt
            cached_df["LastSplitAdjustDt"] = last_adjust_dt
            pkl.dump({"data":cached_df, "metadata":md}, f, 4)

        answer_noadjust = pd.read_csv("./tests/Adjustment/pnl-unadjusted.csv",parse_dates=["Date"],index_col="Date")
        if not isinstance(answer_noadjust.index, pd.DatetimeIndex):
            answer_noadjust.index = pd.to_datetime(answer_noadjust.index, utc=True)
        answer_noadjust.index = answer_noadjust.index.tz_convert(tz)

        df = dat.history(start="2022-01-01", end="2022-08-20", adjust_divs=False, adjust_splits=False)
        self.verify_df(df, answer_noadjust, 5e-5)

        df = dat.history(start="2022-01-01",end="2022-08-20",adjust_divs=False)
        answer_splitAdjusted = yf.Ticker(tkr, self.session).history(start="2022-01-01",end="2022-08-20",auto_adjust=False)
        self.verify_df(df, answer_splitAdjusted, 5e-6)

        df = dat.history(start="2022-01-01",end="2022-08-20")
        answer_adjusted = yf.Ticker(tkr, self.session).history(start="2022-01-01",end="2022-08-20")
        self.verify_df(df, answer_adjusted, 5e-6)


    def test_adjust_prepend1(self):
        # Fetch 1st Aug (split day) -> now, then Jan -> now
        
        tkr = "PNL.L"
        dat = yfc.Ticker(tkr, self.session)
        tz = dat.fast_info["timezone"]

        df = dat.history(start="2022-01-01", end="2022-08-20", adjust_divs=False, adjust_splits=False)

        answer_noadjust = pd.read_csv("./tests/Adjustment/pnl-unadjusted.csv",parse_dates=["Date"],index_col="Date")
        if not isinstance(answer_noadjust.index, pd.DatetimeIndex):
            answer_noadjust.index = pd.to_datetime(answer_noadjust.index, utc=True)
        answer_noadjust.index = answer_noadjust.index.tz_convert(tz)

        self.verify_df(df, answer_noadjust, 5e-5)

    def test_adjust_prepend2(self):
        # Fetch 2nd Aug (after split day) -> now, then Jan -> now
        
        tkr = "PNL.L"
        dat = yfc.Ticker(tkr, self.session)
        tz = dat.fast_info["timezone"]

        df = dat.history(start="2022-01-01", end="2022-08-20", adjust_divs=False, adjust_splits=False)

        answer_noadjust = pd.read_csv("./tests/Adjustment/pnl-unadjusted.csv",parse_dates=["Date"],index_col="Date")
        if not isinstance(answer_noadjust.index, pd.DatetimeIndex):
            answer_noadjust.index = pd.to_datetime(answer_noadjust.index, utc=True)
        answer_noadjust.index = answer_noadjust.index.tz_convert(tz)

        self.verify_df(df, answer_noadjust, 5e-5)

    def test_adjust_prepend3(self):
        # Fetch 28th July (before split day) -> now, then Jan -> now
        
        tkr = "PNL.L"
        dat = yfc.Ticker(tkr, self.session)
        tz = dat.fast_info["timezone"]

        df = dat.history(start="2022-01-01", end="2022-08-20", adjust_divs=False, adjust_splits=False)

        answer_noadjust = pd.read_csv("./tests/Adjustment/pnl-unadjusted.csv",parse_dates=["Date"],index_col="Date")
        if not isinstance(answer_noadjust.index, pd.DatetimeIndex):
            answer_noadjust.index = pd.to_datetime(answer_noadjust.index, utc=True)
        answer_noadjust.index = answer_noadjust.index.tz_convert(tz)

        self.verify_df(df, answer_noadjust, 5e-5)


    def test_weekly_simple(self):
        start_d = date(2022,1,3)
        end_d = date(2022,8,22)

        for tkr in self.tkrs:
            dat = yfc.Ticker(tkr, session=self.session)
            tz = ZoneInfo(dat.fast_info["timezone"])
            df_yfc = dat.history(start=start_d, end=end_d, interval="1wk")

            dat_yf = yf.Ticker(tkr, session=self.session)

            # First compare against YF daily data (aggregated by week)
            df_yf = dat_yf.history(start=start_d, end=end_d, interval="1d")
            df_yf_weekly = df_yf.copy()
            df_yf_weekly["_weekStart"] = pd.to_datetime(df_yf_weekly.index.tz_localize(None).to_period('W-SUN').start_time).tz_localize(tz)
            df_yf_weekly.loc[df_yf_weekly["Stock Splits"]==0,"Stock Splits"]=1
            df_yf_weekly = df_yf_weekly.groupby("_weekStart").agg(
                Open=("Open", "first"),
                Close=("Close", "last"),
                Low=("Low", "min"),
                High=("High", "max"),
                Volume=("Volume", "sum"),
                Dividends=("Dividends", "sum"),
                StockSplits=("Stock Splits", "prod")).rename(columns={"StockSplits":"Stock Splits"})
            df_yf_weekly.loc[df_yf_weekly["Stock Splits"]==1,"Stock Splits"]=0
            # Loose tolerance because just checking that in same ballpark
            self.verify_df(df_yfc, df_yf_weekly, 1e-1)
            # self.verify_df(df_yfc.drop(['Volume'], axis=1), df_yf_weekly.drop(['Volume'], axis=1), 1e-1)

            # Now compare against YF weekly
            if tkr.endswith(".TA"):
                # YF struggles with TLV exchange
                df_yf = dat_yf.history(start=start_d-timedelta(days=2), end=end_d, interval="1wk", repair=True)
                df_yf = df_yf[df_yf.index.date>=start_d]
            else:
                df_yf = dat_yf.history(start=start_d, end=end_d, interval="1wk", repair=True)
            self.verify_df(df_yfc, df_yf, 5e-6)

    def test_weekly_append(self):
        start1_d = date(2022,1,3)
        end1_d = date(2022,6,27)

        start2_d = date(2022,6,27)
        end2_d = date(2022,8,22)

        for tkr in self.tkrs:
            dat = yfc.Ticker(tkr, session=self.session)
            dat.history(start=start1_d, end=end1_d, interval="1wk")
            dat.history(start=start2_d, end=end2_d, interval="1wk")

            df = dat.history(start=start1_d, end=end2_d, interval="1wk")

            dat_yf = yf.Ticker(tkr, session=self.session)
            if tkr.endswith(".TA"):
                # YF struggles with TLV exchange
                answer = dat_yf.history(start=start1_d-timedelta(days=2), end=end2_d, interval="1wk", repair=True)
                answer = answer[answer.index.date>=start1_d]
            else:
                answer = dat_yf.history(start=start1_d, end=end2_d, interval="1wk", repair=True)

            self.verify_df(df, answer, 5e-6)

    def test_weekly_prepend(self):
        start1_d = date(2022,4,4)
        end1_d = date(2022,8,22)

        start2_d = date(2022,1,3)
        end2_d = date(2022,8,22)

        for tkr in self.tkrs:
            dat = yfc.Ticker(tkr, session=self.session)
            dat.history(start=start1_d, end=end1_d, interval="1wk")
            dat.history(start=start2_d, end=end2_d, interval="1wk")

            df = dat.history(start=start2_d, end=end2_d, interval="1wk")

            dat_yf = yf.Ticker(tkr, session=self.session)
            if tkr.endswith(".TA"):
                # YF struggles with TLV exchange
                answer = dat_yf.history(start=start2_d-timedelta(days=2), end=end2_d, interval="1wk", repair=True)
                answer = answer[answer.index.date>=start2_d]
            else:
                answer = dat_yf.history(start=start2_d, end=end2_d, interval="1wk", repair=True)

            self.verify_df(df, answer, 5e-6)

    def test_weekly_insert1(self):
        start1_d = date(2022,1,3)
        end1_d = date(2022,4,4)

        start2_d = date(2022,4,4)
        end2_d = date(2022,6,27)

        start3_d = date(2022,6,27)
        end3_d = date(2022,8,22)

        for tkr in self.tkrs:
            dat = yfc.Ticker(tkr, session=self.session)
            dat.history(start=start1_d, end=end1_d, interval="1wk")
            dat.history(start=start3_d, end=end3_d, interval="1wk")
            dat.history(start=start2_d, end=end2_d, interval="1wk")

            df = dat.history(start=start1_d, end=end3_d, interval="1wk")

            dat_yf = yf.Ticker(tkr, session=self.session)
            if tkr.endswith(".TA"):
                # YF struggles with TLV exchange
                answer = dat_yf.history(start=start1_d-timedelta(days=2), end=end3_d, interval="1wk", repair=True)
                answer = answer[answer.index.date>=start1_d]
            else:
                answer = dat_yf.history(start=start1_d, end=end3_d, interval="1wk", repair=True)

            self.verify_df(df, answer, 5e-6)

    def test_weekly_insert2(self):
        start1_d = date(2022,1,3)
        end1_d = date(2022,4,4)

        start2_d = date(2022,4,4)
        end2_d = date(2022,6,27)

        start3_d = date(2022,6,27)
        end3_d = date(2022,8,22)

        for tkr in self.tkrs:
            dat = yfc.Ticker(tkr, session=self.session)
            dat.history(start=start3_d, end=end3_d, interval="1wk")
            dat.history(start=start1_d, end=end1_d, interval="1wk")
            dat.history(start=start2_d, end=end2_d, interval="1wk")

            df = dat.history(start=start1_d, end=end3_d, interval="1wk")

            dat_yf = yf.Ticker(tkr, session=self.session)
            if tkr.endswith(".TA"):
                # YF struggles with TLV exchange
                answer = dat_yf.history(start=start1_d-timedelta(days=2), end=end3_d, interval="1wk", repair=True)
                answer = answer[answer.index.date>=start1_d]
            else:
                answer = dat_yf.history(start=start1_d, end=end3_d, interval="1wk", repair=True)

            self.verify_df(df, answer, 5e-6)


    def test_hourly_simple(self):
        end_d = pd.Timestamp.utcnow().date()
        if not end_d.weekday() == 5:
            end_d -= timedelta(days=end_d.weekday()+2)
        start_d = end_d - timedelta(days=5) - timedelta(days=8*7)
        td_1d = timedelta(days=1)
        td_5d = timedelta(days=5)

        for tkr in self.tkrs:
            dat = yfc.Ticker(tkr, session=self.session)
            dat_yf = yf.Ticker(tkr, session=self.session)

            # First compare against YF daily data (aggregate YFC by day)
            df_yfc = dat.history(start=start_d, end=end_d, interval="1h")
            df_yf = dat_yf.history(start=start_d, end=end_d, interval="1d", repair=True)
            df_yfc_daily = df_yfc.copy()
            df_yfc_daily["_day"] = pd.to_datetime(df_yfc_daily.index.date).tz_localize(df_yfc.index.tz)
            df_yfc_daily.loc[df_yfc_daily["Stock Splits"]==0,"Stock Splits"]=1
            df_yfc_daily = df_yfc_daily.groupby("_day").agg(
                Open=("Open", "first"),
                Close=("Close", "last"),
                Low=("Low", "min"),
                High=("High", "max"),
                Volume=("Volume", "sum"),
                Dividends=("Dividends", "sum"),
                StockSplits=("Stock Splits", "prod"))
            df_yfc_daily = df_yfc_daily.rename(columns={"StockSplits":"Stock Splits"})
            df_yfc_daily.loc[df_yfc_daily["Stock Splits"]==1,"Stock Splits"]=0
            # Loose tolerance because just checking that in same ballpark
            # - ignore volume here, and missing hour intervals
            df_yf2 = df_yf[df_yf.index.isin(df_yfc_daily.index)]
            self.verify_df(df_yfc_daily.drop("Volume",axis=1), df_yf2.drop("Volume",axis=1), 1e-1)

            # Now compare against YF hourly
            # Note: Yahoo doesn't dividend-adjust hourly
            df_yfc = dat.history(start=start_d, end=end_d, interval="1h", adjust_divs=False)
            df_yf = dat_yf.history(start=start_d-td_5d, end=end_d+td_5d, interval='1h', repair=True).loc[str(start_d):str(end_d-td_1d)]
            # Discard 0-volume data at market close
            sched = yfct.GetExchangeSchedule(dat.fast_info["exchange"], df_yf.index.date.min(), df_yf.index.date.max()+td_1d)
            sched["_date"] = sched.index.date
            df_yf["_date"] = df_yf.index.date
            answer2 = df_yf.merge(sched, on="_date", how="left", validate="many_to_one")
            answer2.index = df_yf.index ; df_yf = answer2
            f_drop = (df_yf["Volume"]==0).values & (df_yf.index>=df_yf["close"])
            df_yf = df_yf[~f_drop].drop("_date",axis=1)
            # YF hourly volume is not split-adjusted, so adjust:
            ss = df_yf["Stock Splits"].copy()
            ss[ss==0.0] = 1.0
            ss_rcp = 1.0/ss
            csf = ss_rcp.sort_index(ascending=False).cumprod().sort_index(ascending=True).shift(-1, fill_value=1.0)
            df_yf["Volume"] /= csf
            df_yf = df_yf[df_yf.index.date<end_d]
            self.verify_df(df_yfc, df_yf, 1e-7)

    def test_hourly_append(self):
        end2_d = pd.Timestamp.utcnow().date()
        if not end2_d.weekday() == 5:
            end2_d -= timedelta(days=end2_d.weekday()+2)
        start2_d = end2_d - timedelta(days=5) - timedelta(days=4*7)
        end1_d = start2_d - timedelta(days=2)
        start1_d = end1_d - timedelta(days=5) - timedelta(days=4*7)
        td_5d = timedelta(days=5)

        for tkr in self.tkrs:
            dat = yfc.Ticker(tkr, session=self.session)
            dat.history(start=start1_d, end=end1_d, interval="1h")
            # print(f"start2_d={start2_d} end2_d={end2_d}")
            dat.history(start=start2_d, end=end2_d, interval="1h")

            dat_yf = yf.Ticker(tkr, session=self.session)

            # First compare against YF daily data (aggregate YFC by day)
            df_yfc = dat.history(start=start1_d, end=end2_d, interval="1h")
            df_yf = dat_yf.history(start=start1_d, end=end2_d, interval="1d", repair=True)
            df_yfc_daily = df_yfc.copy()
            df_yfc_daily["_day"] = pd.to_datetime(df_yfc_daily.index.date).tz_localize(df_yfc.index.tz)
            df_yfc_daily.loc[df_yfc_daily["Stock Splits"]==0,"Stock Splits"]=1
            df_yfc_daily = df_yfc_daily.groupby("_day").agg(
                Open=("Open", "first"),
                Close=("Close", "last"),
                Low=("Low", "min"),
                High=("High", "max"),
                Volume=("Volume", "sum"),
                Dividends=("Dividends", "sum"),
                StockSplits=("Stock Splits", "prod")).rename(columns={"StockSplits":"Stock Splits"})
            df_yfc_daily.loc[df_yfc_daily["Stock Splits"]==1,"Stock Splits"]=0
            # Loose tolerance because just checking that in same ballpark
            # - ignore volume here, and missing hour intervals
            df_yf2 = df_yf[df_yf.index.isin(df_yfc_daily.index)]
            if tkr == "I3E.L" and start1_d <= date(2022,11,29):
                # Skip test, because there is genuine difference between 1d and 1h on 2022-11-29
                pass
            else:
                self.verify_df(df_yfc_daily.drop("Volume",axis=1), df_yf2.drop("Volume",axis=1), 1e-1)

            # Now compare against YF hourly
            df_yfc = dat.history(start=start1_d, end=end2_d, interval="1h", adjust_divs=False)
            df_yf = dat_yf.history(start=start1_d-td_5d, interval='1h', repair=True).loc[str(start1_d):]
            # Discard 0-volume data at market close
            td_1d = timedelta(days=1)
            sched = yfct.GetExchangeSchedule(dat.fast_info["exchange"], df_yf.index.date.min(), df_yf.index.date.max()+td_1d)
            sched["_date"] = sched.index.date
            df_yf["_date"] = df_yf.index.date
            answer2 = df_yf.merge(sched, on="_date", how="left", validate="many_to_one")
            answer2.index = df_yf.index ; df_yf = answer2
            f_drop = (df_yf["Volume"]==0).values & (df_yf.index>=df_yf["close"])
            df_yf = df_yf[~f_drop].drop(["_date","open","close"],axis=1)
            # YF hourly volume is not split-adjusted, so adjust:
            ss = df_yf["Stock Splits"].copy()
            ss[ss==0.0] = 1.0
            ss_rcp = 1.0/ss
            csf = ss_rcp.sort_index(ascending=False).cumprod().sort_index(ascending=True).shift(-1, fill_value=1.0)
            df_yf["Volume"] /= csf
            df_yf = df_yf[df_yf.index.date<end2_d]
            self.verify_df(df_yfc, df_yf, 1e-7)

    def test_hourly_prepend(self):
        end2_d = pd.Timestamp.utcnow().date()
        if not end2_d.weekday() == 5:
            end2_d -= timedelta(days=end2_d.weekday()+2)
        start2_d = end2_d - timedelta(days=5) - timedelta(days=4*7)
        end1_d = start2_d - timedelta(days=2)
        start1_d = end1_d - timedelta(days=5) - timedelta(days=4*7)
        td_5d = timedelta(days=5)

        for tkr in self.tkrs:
            dat = yfc.Ticker(tkr, session=self.session)
            dat.history(start=start2_d, end=end2_d, interval="1h")
            dat.history(start=start1_d, end=end1_d, interval="1h")

            dat_yf = yf.Ticker(tkr, session=self.session)

            # First compare against YF daily data (aggregate YFC by day)
            df_yfc = dat.history(start=start1_d, end=end2_d, interval="1h")
            df_yf = dat_yf.history(start=start1_d, end=end2_d, interval="1d", repair=True)
            df_yfc_daily = df_yfc.copy()
            df_yfc_daily["_day"] = pd.to_datetime(df_yfc_daily.index.date).tz_localize(df_yfc.index.tz)
            df_yfc_daily.loc[df_yfc_daily["Stock Splits"]==0,"Stock Splits"]=1
            df_yfc_daily = df_yfc_daily.groupby("_day").agg(
                Open=("Open", "first"),
                Close=("Close", "last"),
                Low=("Low", "min"),
                High=("High", "max"),
                Volume=("Volume", "sum"),
                Dividends=("Dividends", "sum"),
                StockSplits=("Stock Splits", "prod")).rename(columns={"StockSplits":"Stock Splits"})
            df_yfc_daily.loc[df_yfc_daily["Stock Splits"]==1,"Stock Splits"]=0
            # Loose tolerance because just checking that in same ballpark
            # - ignore volume here, and missing hour intervals
            df_yf2 = df_yf[df_yf.index.isin(df_yfc_daily.index)]
            df_yf2 = df_yf[df_yf.index.isin(df_yfc_daily.index)]
            if tkr == "I3E.L" and start1_d <= date(2022,11,29):
                # Skip test, because there is genuine difference between 1d and 1h on 2022-11-29
                pass
            else:
                self.verify_df(df_yfc_daily.drop("Volume",axis=1), df_yf2.drop("Volume",axis=1), 1e-1)

            # Now compare against YF hourly
            df_yfc = dat.history(start=start1_d, end=end2_d, interval="1h", adjust_divs=False)
            df_yf = dat_yf.history(start=start1_d-td_5d, interval='1h', repair=True).loc[str(start1_d):]
            # Discard 0-volume data at market close
            td_1d = timedelta(days=1)
            sched = yfct.GetExchangeSchedule(dat.fast_info["exchange"], df_yf.index.date.min(), df_yf.index.date.max()+td_1d)
            sched["_date"] = sched.index.date
            df_yf["_date"] = df_yf.index.date
            answer2 = df_yf.merge(sched, on="_date", how="left", validate="many_to_one")
            answer2.index = df_yf.index ; df_yf = answer2
            f_drop = (df_yf["Volume"]==0).values & (df_yf.index>=df_yf["close"])
            df_yf = df_yf[~f_drop].drop("_date",axis=1)
            # YF hourly volume is not split-adjusted, so adjust:
            ss = df_yf["Stock Splits"].copy()
            ss[ss==0.0] = 1.0
            ss_rcp = 1.0/ss
            csf = ss_rcp.sort_index(ascending=False).cumprod().sort_index(ascending=True).shift(-1, fill_value=1.0)
            df_yf["Volume"] /= csf
            df_yf = df_yf[df_yf.index.date<end2_d]
            self.verify_df(df_yfc, df_yf, 1e-7)

    def test_hourly_insert1(self):
        end3_d = pd.Timestamp.utcnow().date()
        if not end3_d.weekday() == 5:
            end3_d -= timedelta(days=end3_d.weekday()+2)
        start3_d = end3_d - timedelta(days=5) - timedelta(days=4*7)
        end2_d = start3_d - timedelta(days=2)
        start2_d = end2_d - timedelta(days=5) - timedelta(days=4*7)
        end1_d = start2_d - timedelta(days=2)
        start1_d = end1_d - timedelta(days=5) - timedelta(days=4*7)
        td_5d = timedelta(days=5)

        # print("Fetching ranges: {}->{} then {}->{} then {}->{}".format(start3_d,end3_d , start1_d,end1_d , start2_d,end2_d))
        for tkr in self.tkrs:
            dat = yfc.Ticker(tkr, session=self.session)
            dat.history(start=start3_d, end=end3_d, interval="1h")
            dat.history(start=start1_d, end=end1_d, interval="1h")
            dat.history(start=start2_d, end=end2_d, interval="1h")

            dat_yf = yf.Ticker(tkr, session=self.session)

            # First compare against YF daily data (aggregate YFC by day)
            df_yfc = dat.history(start=start1_d, end=end3_d, interval="1h")
            df_yf = dat_yf.history(start=start1_d, end=end3_d, interval="1d", repair=True)
            df_yfc_daily = df_yfc.copy()
            df_yfc_daily["_day"] = pd.to_datetime(df_yfc_daily.index.date).tz_localize(df_yfc.index.tz)
            df_yfc_daily.loc[df_yfc_daily["Stock Splits"]==0,"Stock Splits"]=1
            df_yfc_daily = df_yfc_daily.groupby("_day").agg(
                Open=("Open", "first"),
                Close=("Close", "last"),
                Low=("Low", "min"),
                High=("High", "max"),
                Volume=("Volume", "sum"),
                Dividends=("Dividends", "sum"),
                StockSplits=("Stock Splits", "prod")).rename(columns={"StockSplits":"Stock Splits"})
            df_yfc_daily.loc[df_yfc_daily["Stock Splits"]==1,"Stock Splits"]=0
            # Loose tolerance because just checking that in same ballpark
            # - ignore volume here, and missing hour intervals
            df_yf2 = df_yf[df_yf.index.isin(df_yfc_daily.index)]
            if tkr == "I3E.L" and start1_d <= date(2022,11,29):
                # Skip test, because there is genuine difference between 1d and 1h on 2022-11-29
                pass
            else:
                self.verify_df(df_yfc_daily.drop("Volume",axis=1), df_yf2.drop("Volume",axis=1), 1e-1)

            # Now compare against YF hourly
            df_yfc = dat.history(start=start1_d, end=end3_d, interval="1h", adjust_divs=False)
            df_yf = dat_yf.history(start=start1_d-td_5d, interval='1h', repair=True).loc[str(start1_d):]
            # Discard 0-volume data at market close
            td_1d = timedelta(days=1)
            sched = yfct.GetExchangeSchedule(dat.fast_info["exchange"], df_yf.index.date.min(), df_yf.index.date.max()+td_1d)
            sched["_date"] = sched.index.date
            df_yf["_date"] = df_yf.index.date
            answer2 = df_yf.merge(sched, on="_date", how="left", validate="many_to_one")
            answer2.index = df_yf.index ; df_yf = answer2
            f_drop = (df_yf["Volume"]==0).values & (df_yf.index>=df_yf["close"])
            df_yf = df_yf[~f_drop].drop(["_date","close","auction"],errors="ignore",axis=1)
            # YF hourly volume is not split-adjusted, so adjust:
            ss = df_yf["Stock Splits"].copy()
            ss[ss==0.0] = 1.0
            ss_rcp = 1.0/ss
            csf = ss_rcp.sort_index(ascending=False).cumprod().sort_index(ascending=True).shift(-1, fill_value=1.0)
            df_yf["Volume"] /= csf
            df_yf = df_yf[df_yf.index.date<end3_d]
            self.verify_df(df_yfc, df_yf, 1e-7)

    def test_hourly_insert2(self):
        end3_d = pd.Timestamp.utcnow().date()
        if not end3_d.weekday() == 5:
            end3_d -= timedelta(days=end3_d.weekday()+2)
        start3_d = end3_d - timedelta(days=5) - timedelta(days=4*7)
        end2_d = start3_d - timedelta(days=2)
        start2_d = end2_d - timedelta(days=5) - timedelta(days=4*7)
        end1_d = start2_d - timedelta(days=2)
        start1_d = end1_d - timedelta(days=5) - timedelta(days=4*7)
        td_5d = timedelta(days=5)

        # print("Fetching ranges: {}->{} then {}->{} then {}->{}".format(start1_d,end1_d , start3_d,end3_d , start2_d,end2_d))
        for tkr in self.tkrs:
            dat = yfc.Ticker(tkr, session=self.session)
            dat.history(start=start1_d, end=end1_d, interval="1h")
            dat.history(start=start3_d, end=end3_d, interval="1h")
            dat.history(start=start2_d, end=end2_d, interval="1h")

            dat_yf = yf.Ticker(tkr, session=self.session)

            # First compare against YF daily data (aggregate YFC by day)
            df_yfc = dat.history(start=start1_d, end=end3_d, interval="1h")
            df_yf = dat_yf.history(start=start1_d, end=end3_d, interval="1d", repair=True)
            df_yfc_daily = df_yfc.copy()
            df_yfc_daily["_day"] = pd.to_datetime(df_yfc_daily.index.date).tz_localize(df_yfc.index.tz)
            df_yfc_daily.loc[df_yfc_daily["Stock Splits"]==0,"Stock Splits"]=1
            df_yfc_daily = df_yfc_daily.groupby("_day").agg(
                Open=("Open", "first"),
                Close=("Close", "last"),
                Low=("Low", "min"),
                High=("High", "max"),
                Volume=("Volume", "sum"),
                Dividends=("Dividends", "sum"),
                StockSplits=("Stock Splits", "prod")).rename(columns={"StockSplits":"Stock Splits"})
            df_yfc_daily.loc[df_yfc_daily["Stock Splits"]==1,"Stock Splits"]=0
            # Loose tolerance because just checking that in same ballpark
            df_yf2 = df_yf[df_yf.index.isin(df_yfc_daily.index)]
            if tkr == "I3E.L" and start1_d <= date(2022,11,29):
                # Skip test, because there is genuine difference between 1d and 1h on 2022-11-29
                pass
            else:
                self.verify_df(df_yfc_daily.drop("Volume",axis=1), df_yf2.drop("Volume",axis=1), 1e-1)

            # Now compare against YF hourly
            df_yfc = dat.history(start=start1_d, end=end3_d, interval="1h", adjust_divs=False)
            df_yf = dat_yf.history(start=start1_d-td_5d, interval='1h', repair=True).loc[str(start1_d):]
            # Discard 0-volume data at market close
            td_1d = timedelta(days=1)
            sched = yfct.GetExchangeSchedule(dat.fast_info["exchange"], df_yf.index.date.min(), df_yf.index.date.max()+td_1d)
            sched["_date"] = sched.index.date
            df_yf["_date"] = df_yf.index.date
            answer2 = df_yf.merge(sched, on="_date", how="left", validate="many_to_one")
            answer2.index = df_yf.index ; df_yf = answer2
            f_drop = (df_yf["Volume"]==0).values & (df_yf.index>=df_yf["close"])
            df_yf = df_yf[~f_drop].drop("_date",axis=1)
            # YF hourly volume is not split-adjusted, so adjust:
            ss = df_yf["Stock Splits"].copy()
            ss[ss==0.0] = 1.0
            ss_rcp = 1.0/ss
            csf = ss_rcp.sort_index(ascending=False).cumprod().sort_index(ascending=True).shift(-1, fill_value=1.0)
            df_yf["Volume"] /= csf
            df_yf = df_yf[df_yf.index.date<end3_d]
            self.verify_df(df_yfc, df_yf, 1e-7)

if __name__ == '__main__':
    unittest.main()

    # # Run tests sequentially:
    # import inspect
    # test_src = inspect.getsource(Test_Unadjust)
    # unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: (
    #     test_src.index(f"def {x}") - test_src.index(f"def {y}")
    # )
    # unittest.main(verbosity=2)
