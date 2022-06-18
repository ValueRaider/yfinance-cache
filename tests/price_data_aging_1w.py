import unittest

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from pprint import pprint

class Test_PriceDataAging_1W(unittest.TestCase):
    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.market_open_time  = time(hour=9, minute=30)
        self.market_close_time = time(hour=16, minute=0)

        self.monday  = date(year=2022, month=2, day=7)
        self.tuesday = date(year=2022, month=2, day=8)
        self.wednday = date(year=2022, month=2, day=9)
        self.friday  = date(year=2022, month=2, day=11)
        self.saturday= date(year=2022, month=2, day=12)

    ## Test same day same week
    def test_sameWeek_sameDay(self):
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)
        intervals = [yfcd.Interval.Week, yfcd.Interval.Days5]

        for interval in intervals:
            max_ages = []
            fetch_dts = []
            dt_nows = []
            expire_on_candle_closes = []
            yf_lags = []
            answers = []

            max_ages.append(timedelta(hours=2))
            fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=5), self.market_tz))
            dt_nows.append(  datetime.combine(self.monday, time(hour=14, minute=4), self.market_tz))
            expire_on_candle_closes.append(False) ; yf_lags.append(None)
            answers.append(False)

            max_ages.append(timedelta(hours=2))
            fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=5), self.market_tz))
            dt_nows.append(  datetime.combine(self.monday, time(hour=14, minute=5), self.market_tz))
            expire_on_candle_closes.append(False) ; yf_lags.append(None)
            answers.append(True)

            for i in range(len(fetch_dts)):
                fetch_dt = fetch_dts[i]
                max_age = max_ages[i]
                dt_now = dt_nows[i]
                expire_on_candle_close = expire_on_candle_closes[i]
                yf_lag = yf_lags[i]
                answer = answers[i]
                response = yfct.IsPriceDatapointExpired(interval_start_dt, fetch_dt, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
                try:
                    self.assertEqual(response, answer)
                except:
                    print("fetch_dt = {0}".format(fetch_dt))
                    print("max_age = {0}".format(max_age))
                    print("dt_now = {0}".format(dt_now))
                    print("answer:")
                    pprint(answer)
                    print("response:")
                    pprint(response)
                    raise

    ## Test different day same week
    def test_sameWeek_diffDay(self):
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)
        intervals = [yfcd.Interval.Week, yfcd.Interval.Days5]

        for interval in intervals:
            max_ages = []
            fetch_dts = []
            dt_nows = []
            expire_on_candle_closes = []
            yf_lags = []
            answers = []

            max_ages.append(timedelta(hours=2))
            fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
            dt_nows.append(  datetime.combine(self.tuesday, time(hour=12, minute=5), self.market_tz))
            expire_on_candle_closes.append(False) ; yf_lags.append(None)
            answers.append(True)

            max_ages.append(timedelta(hours=24))
            fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
            dt_nows.append(  datetime.combine(self.tuesday, time(hour=12, minute=4), self.market_tz))
            expire_on_candle_closes.append(False) ; yf_lags.append(None)
            answers.append(False)
            max_ages.append(timedelta(hours=24))
            fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
            dt_nows.append(  datetime.combine(self.tuesday, time(hour=12, minute=5), self.market_tz))
            expire_on_candle_closes.append(False) ; yf_lags.append(None)
            answers.append(True)

            max_ages.append(timedelta(days=1))
            fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
            dt_nows.append(  datetime.combine(self.tuesday, time(hour=11, minute=4), self.market_tz))
            expire_on_candle_closes.append(False) ; yf_lags.append(None)
            answers.append(False)
            max_ages.append(timedelta(days=1))
            fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
            dt_nows.append(  datetime.combine(self.tuesday, time(hour=12, minute=5), self.market_tz))
            expire_on_candle_closes.append(False) ; yf_lags.append(None)
            answers.append(True)

            max_ages.append(timedelta(days=2))
            fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
            dt_nows.append(  datetime.combine(self.tuesday, time(hour=12, minute=5), self.market_tz))
            expire_on_candle_closes.append(False) ; yf_lags.append(None)
            answers.append(False)

            max_ages.append(timedelta(days=2))
            fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
            dt_nows.append(  datetime.combine(self.wednday, time(hour=12, minute=5), self.market_tz))
            expire_on_candle_closes.append(False) ; yf_lags.append(None)
            answers.append(True)

            for i in range(len(fetch_dts)):
                fetch_dt = fetch_dts[i]
                max_age = max_ages[i]
                dt_now = dt_nows[i]
                expire_on_candle_close = expire_on_candle_closes[i]
                yf_lag = yf_lags[i]
                answer = answers[i]
                response = yfct.IsPriceDatapointExpired(interval_start_dt, fetch_dt, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
                try:
                    self.assertEqual(response, answer)
                except:
                    print("fetch_dt = {0}".format(fetch_dt))
                    print("max_age = {0}".format(max_age))
                    print("dt_now = {0}".format(dt_now))
                    print("answer:")
                    pprint(answer)
                    print("response:")
                    pprint(response)
                    raise

    ## Test weekend following week
    def test_sameWeek_weekend(self):
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)
        intervals = [yfcd.Interval.Week, yfcd.Interval.Days5]

        dt_now = datetime.combine(self.saturday, time(hour=9, minute=0), self.market_tz)
        for interval in intervals:
            max_ages = []
            fetch_dts = []
            expire_on_candle_closes = []
            yf_lags = []
            answers = []

            max_ages.append(timedelta(hours=2))
            fetch_dts.append(datetime.combine(self.monday, time(hour=13, minute=44), self.market_tz))
            expire_on_candle_closes.append(False) ; yf_lags.append(None)
            answers.append(True)

            max_ages.append(timedelta(hours=2))
            fetch_dts.append(datetime.combine(self.monday, time(hour=13, minute=45), self.market_tz))
            expire_on_candle_closes.append(False) ; yf_lags.append(None)
            answers.append(True)

            for i in range(len(fetch_dts)):
                fetch_dt = fetch_dts[i]
                max_age = max_ages[i]
                expire_on_candle_close = expire_on_candle_closes[i]
                yf_lag = yf_lags[i]
                answer = answers[i]
                response = yfct.IsPriceDatapointExpired(interval_start_dt, fetch_dt, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
                try:
                    self.assertEqual(response, answer)
                except:
                    print("fetch_dt = {0}".format(fetch_dt))
                    print("max_age = {0}".format(max_age))
                    print("dt_now = {0}".format(dt_now))
                    print("answer:")
                    pprint(answer)
                    print("response:")
                    pprint(response)
                    raise

    ## Test next week
    def test_nextWeek(self):
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)
        intervals = [yfcd.Interval.Week, yfcd.Interval.Days5]

        dt_now = datetime.combine(self.monday+timedelta(days=7), time(hour=13, minute=45), self.market_tz)
        for interval in intervals:
            max_ages = []
            fetch_dts = []
            expire_on_candle_closes = []
            yf_lags = []
            answers = []

            max_ages.append(timedelta(hours=2))
            fetch_dts.append(datetime.combine(self.monday, time(hour=13, minute=59), self.market_tz))
            expire_on_candle_closes.append(False) ; yf_lags.append(None)
            answers.append(True)

            max_ages.append(timedelta(hours=2))
            fetch_dts.append(datetime.combine(self.monday, time(hour=14, minute=0), self.market_tz))
            expire_on_candle_closes.append(False) ; yf_lags.append(None)
            answers.append(True)

            for i in range(len(fetch_dts)):
                fetch_dt = fetch_dts[i]
                max_age = max_ages[i]
                expire_on_candle_close = expire_on_candle_closes[i]
                yf_lag = yf_lags[i]
                answer = answers[i]
                response = yfct.IsPriceDatapointExpired(interval_start_dt, fetch_dt, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
                try:
                    self.assertEqual(response, answer)
                except:
                    print("fetch_dt = {0}".format(fetch_dt))
                    print("max_age = {0}".format(max_age))
                    print("dt_now = {0}".format(dt_now))
                    print("answer:")
                    pprint(answer)
                    print("response:")
                    pprint(response)
                    raise

if __name__ == '__main__':
    unittest.main()
