import unittest

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from pprint import pprint

class Test_PriceDataAging_1D(unittest.TestCase):

    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.market_open_time  = time(hour=9, minute=30)
        self.market_close_time = time(hour=16, minute=0)

        self.monday  = date(year=2022, month=2, day=7)
        self.tuesday = date(year=2022, month=2, day=8)
        self.friday  = date(year=2022, month=2, day=11)
        self.saturday= date(year=2022, month=2, day=12)

    ## Test day interval fetched same day
    def test_sameDay(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)

        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=5),  self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=12, minute=34), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=5),  self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)



        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=5),  self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=4), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=5),  self.market_tz))
        max_ages.append(timedelta(hours=1))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)



        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=0),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=0),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=0))
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=0),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=1),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(True)



        fetch_dts.append(datetime.combine(self.monday,  time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(hour=9,  minute=0),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday,  time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(hour=9, minute=30),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday,  time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(hour=9, minute=30),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(None)
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

    ## Test day interval fetched same day, on exchange with data delay
    def test_sameDay_delay(self):
        exchange = "LSE" # 20min delay
        yf_lag = timedelta(minutes=20)
        market_tz = ZoneInfo("Europe/London")
        interval = yfcd.Interval.Days1
        market_open = time(8)
        market_close = time(16, 30)

        ## Just before market close, but with data delay was fetched after close:
        fetch_dt = datetime.combine(self.monday, market_close, market_tz)+timedelta(minutes=1)
        interval_dt = fetch_dt-yf_lag
        max_age = timedelta(minutes=10)
        dt_now = datetime.combine(self.monday, market_close, market_tz)+timedelta(minutes=14)
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
        dt_now = datetime.combine(self.monday, market_close, market_tz)+timedelta(minutes=2)
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
        dt_now = datetime.combine(self.monday, market_close, market_tz)+yf_lag+timedelta(minutes=1)
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

        ## Just after market close + data delay:
        fetch_dt = datetime.combine(self.monday, market_close, market_tz)+yf_lag+timedelta(minutes=1)
        interval_dt = datetime.combine(self.monday, market_open, market_tz)
        max_age = timedelta(minutes=10)
        dt_now = datetime.combine(self.monday, market_close, market_tz)+yf_lag+timedelta(minutes=14)
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

    ## Test Friday interval fetched same day when dt_now is next day (weekend)
    def test_nextDayWeekend(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.friday, self.market_open_time, self.market_tz)

        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        max_ages.append(timedelta(minutes=30))
        fetch_dts.append(datetime.combine(self.friday,   time(hour=14, minute=55), self.market_tz))
        dt_nows.append(  datetime.combine(self.saturday, time(hour=9, minute=10), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        max_ages.append(timedelta(minutes=30))
        fetch_dts.append(datetime.combine(self.friday,   time(hour=15, minute=55), self.market_tz))
        dt_nows.append(  datetime.combine(self.saturday, time(hour=9, minute=10), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        max_ages.append(timedelta(minutes=30))
        fetch_dts.append(datetime.combine(self.friday,   time(hour=15, minute=55), self.market_tz))
        dt_nows.append(  datetime.combine(self.saturday, time(hour=9, minute=10), self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(None)
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

    ## Batch test day interval during same day
    def test_intraDay_batch(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)

        max_age = timedelta(minutes=30)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.monday, time(hour=13, minute=10), self.market_tz)

        interval_start_dts = []
        fetch_dts = []

        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=9, minute=35), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=30), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=13, minute=0), self.market_tz))

        responses = yfct.IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = yfct.IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise

    ## Batch test day interval during same day, on exchange with data delay
    def test_intraDay_delay_batch(self):
        exchange = "LSE" # 20min delay
        yf_lag = timedelta(minutes=20)
        market_tz = ZoneInfo("Europe/London")
        interval = yfcd.Interval.Days1
        market_open = time(8)
        market_close = time(16, 30)

        interval = yfcd.Interval.Days1

        max_age = timedelta(minutes=10)
        expire_on_candle_close = False
        dt_now = datetime.combine(self.monday, market_close, market_tz)+timedelta(minutes=14)

        interval_dts = []
        fetch_dts = []
        answers = []
        #
        fetch_dt = datetime.combine(self.monday, market_close, market_tz)+timedelta(minutes=1)
        interval_dt = fetch_dt-yf_lag
        interval_dts.append(interval_dt) ; fetch_dts.append(fetch_dt)
        answers.append(True)
        #
        fetch_dt = datetime.combine(self.monday, market_close, market_tz)+timedelta(minutes=12)
        interval_dt = fetch_dt-yf_lag
        interval_dts.append(interval_dt) ; fetch_dts.append(fetch_dt)
        answers.append(False)

        responses = yfct.IsPriceDatapointExpired_batch(interval_dts, fetch_dts, max_age, exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = yfct.IsPriceDatapointExpired(interval_dts[i], fetch_dts[i], max_age, exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            # answer = answers[i]
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_dt = {0}".format(interval_dts[i]))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt: {}".format(fetch_dts[i]))
                print("answer:{}".format(answer))
                print("response:{}".format(responses[i]))
                raise

    ## Batch test day interval just after market close
    def test_lateDay_batch(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)

        max_age = timedelta(minutes=30)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.monday, time(hour=16, minute=15), self.market_tz)

        interval_start_dts = []
        fetch_dts = []

        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=14, minute=0), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=40), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))

        responses = yfct.IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = yfct.IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise

    ## Batch test day interval next day during market open
    def test_nextDay_batch1(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)

        max_age = timedelta(minutes=30)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.tuesday, time(hour=9, minute=45), self.market_tz)

        interval_start_dts = []
        fetch_dts = []

        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=5), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=17, minute=0), self.market_tz))

        responses = yfct.IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = yfct.IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise

    ## Batch test day interval next day during market open with 24-hour age
    def test_nextDay_batch2(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)

        max_age = timedelta(days=1)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.tuesday, time(hour=9, minute=45), self.market_tz)

        interval_start_dts = []
        fetch_dts = []

        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=9, minute=30), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=10, minute=0), self.market_tz))

        responses = yfct.IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = yfct.IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise

    ## Batch test Friday interval next day Saturday
    def test_nextDay_batch3(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.friday, self.market_open_time, self.market_tz)

        max_age = timedelta(minutes=30)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.saturday, time(hour=10, minute=45), self.market_tz)

        interval_start_dts = []
        fetch_dts = []

        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=5), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=55), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.friday, time(hour=17, minute=0), self.market_tz))

        responses = yfct.IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = yfct.IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise

    ## Reproduce a runtime bug:
    def test_batch_bug1(self):
        interval_dts = [datetime(2022, 6, 13, 9,   0,             tzinfo=ZoneInfo(key='Africa/Johannesburg')),
                        datetime(2022, 6, 14, 9,   0,             tzinfo=ZoneInfo(key='Africa/Johannesburg')),
                        datetime(2022, 6, 15, 9,   0,             tzinfo=ZoneInfo(key='Africa/Johannesburg')),
                        datetime(2022, 6, 17, 9,   0,             tzinfo=ZoneInfo(key='Africa/Johannesburg')),
                        datetime(2022, 6, 20, 17, 35, 15,         tzinfo=ZoneInfo(key='Africa/Johannesburg'))]
        fetch_dts = [   datetime(2022, 6, 20, 13, 18, 13, 123612, tzinfo=ZoneInfo(key='Africa/Johannesburg')),
                        datetime(2022, 6, 20, 13, 18, 13, 123612, tzinfo=ZoneInfo(key='Africa/Johannesburg')),
                        datetime(2022, 6, 20, 13, 18, 13, 123612, tzinfo=ZoneInfo(key='Africa/Johannesburg')),
                        datetime(2022, 6, 20, 13, 18, 13, 123612, tzinfo=ZoneInfo(key='Africa/Johannesburg')),
                        datetime(2022, 6, 20, 23, 56, 26, 474974, tzinfo=ZoneInfo(key='Africa/Johannesburg'))]
        max_age = timedelta(minutes=30)
        exchange = "JNB"
        interval = yfcd.Interval.Days1
        triggerExpiryOnClose = True
        yf_lag = timedelta(minutes=15)
        dt_now = datetime.combine(date(2022, 6, 20), time(23, 6), tzinfo=ZoneInfo("Europe/London"))

        responses = yfct.IsPriceDatapointExpired_batch(interval_dts, fetch_dts, max_age, exchange, interval, triggerExpiryOnClose, yf_lag, dt_now)
        for i in range(len(interval_dts)):
            answer = yfct.IsPriceDatapointExpired(interval_dts[i], fetch_dts[i], max_age, exchange, interval, triggerExpiryOnClose, yf_lag, dt_now)
            self.assertEqual(responses[i], answer)

if __name__ == '__main__':
    unittest.main()
