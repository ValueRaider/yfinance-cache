import unittest

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from pprint import pprint

class Test_PriceDataAging_1H(unittest.TestCase):

    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.market_close_time = time(hour=16, minute=0)

        self.monday  = date(year=2022, month=2, day=7)
        self.tuesday = date(year=2022, month=2, day=8)
        self.friday  = date(year=2022, month=2, day=11)
        self.saturday= date(year=2022, month=2, day=12)

    ## 1h interval, fetched during or v.soon after, tested during or v.soon after
    def test_duringDay(self):
        interval = yfcd.Interval.Hours1
        interval_start_dt = datetime.combine(self.monday, time(hour=12, minute=30), self.market_tz)

        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=4),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=5),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=30), self.market_tz))
        expire_on_candle_closes.append(False)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=30), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=31), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(hours=1))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=30), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(hours=1))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=31), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
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
                print("expire_on_candle_close = {0}".format(expire_on_candle_close))
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(response)
                raise

    def test_duringDay_delay(self):
        exchange = "LSE" # 20min delay
        yf_lag = timedelta(minutes=20)
        market_tz = ZoneInfo("Europe/London")
        interval = yfcd.Interval.Hours1
        market_open = time(8)
        market_close = time(16, 30)

        ## Just before interval close, but with data delay was fetched after close:
        fetch_dt = datetime.combine(self.monday, time(15), market_tz)+timedelta(minutes=1)
        interval_dt = fetch_dt-yf_lag
        max_age = timedelta(minutes=10)
        dt_now = datetime.combine(self.monday, time(15), market_tz)+timedelta(minutes=14)
        expire_on_candle_close = False
        answer = True
        result = yfct.IsPriceDatapointExpired(interval_dt, fetch_dt, max_age, exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        try:
            self.assertEqual(result, answer)
        except:
            print("result:")
            pprint(result)
            print("answer:")
            pprint(answer)
            raise
        #
        dt_now = datetime.combine(self.monday, time(15), market_tz)+timedelta(minutes=2)
        answer = False
        result = yfct.IsPriceDatapointExpired(interval_dt, fetch_dt, max_age, exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        try:
            self.assertEqual(result, answer)
        except:
            print("result:")
            pprint(result)
            print("answer:")
            pprint(answer)
            raise

        ## Check that 'expire-on-candle-close' still works:
        dt_now = datetime.combine(self.monday, time(16,1), market_tz)+yf_lag
        max_age = timedelta(hours=1)
        expire_on_candle_close = True
        answer = True
        result = yfct.IsPriceDatapointExpired(interval_dt, fetch_dt, max_age, exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        try:
            self.assertEqual(result, answer)
        except:
            print("result:")
            pprint(result)
            print("answer:")
            pprint(answer)
            raise

        ## Just after interval close + data delay:
        fetch_dt = datetime.combine(self.monday, time(16,1), market_tz)+yf_lag
        interval_dt = datetime.combine(self.monday, time(15), market_tz)
        max_age = timedelta(minutes=10)
        dt_now = datetime.combine(self.monday, time(16), market_tz)+yf_lag+timedelta(minutes=14)
        expire_on_candle_close = False
        answer = False
        result = yfct.IsPriceDatapointExpired(interval_dt, fetch_dt, max_age, exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        try:
            self.assertEqual(result, answer)
        except:
            print("result:")
            pprint(result)
            print("answer:")
            pprint(answer)
            raise

    ## 1h interval end-of-day, fetched during interval, tested during | soon after | next morning
    def test_endOfDay(self):
        interval = yfcd.Interval.Hours1
        interval_start_dt = datetime.combine(self.monday, time(hour=15, minute=30), self.market_tz)
        
        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=20))
        dt_nows.append(  datetime.combine(self.monday, time(hour=15, minute=54), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=20))
        dt_nows.append(  datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=15, minute=59), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)
        
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=0), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=0), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=1), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
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
                print("expire_on_candle_close = {}".format(expire_on_candle_close))
                print("yf_lag = {}".format(yf_lag))
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(response)
                raise

    ## 1h interval end-of-Friday, fetched during interval, tested after market close | next Saturday
    def test_endOfFriday(self):
        interval = yfcd.Interval.Hours1
        interval_start_dt = datetime.combine(self.friday, time(hour=15, minute=30), self.market_tz)
        
        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.friday, time(hour=16, minute=1),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.friday, time(hour=16, minute=0),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.friday, time(hour=16, minute=0),  self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.friday, time(hour=16, minute=1),  self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
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

if __name__ == '__main__':
    unittest.main()
