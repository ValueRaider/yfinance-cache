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
        self.market_close = time(16)

        self.nze_exchange = "NZE"
        self.nze_market_tz_name = 'Pacific/Auckland'
        self.nze_market_tz = ZoneInfo('Pacific/Auckland')
        self.nze_market_open_time  = time(10)
        self.nze_market_close_time = time(16)

        self.monday = date(2022, 2, 7)
        self.tuesday = date(2022, 2, 8)
        self.friday = date(2022, 2, 11)
        self.saturday = date(2022, 2, 12)

        self.td1h = timedelta(hours=1)
        self.td1d = timedelta(days=1)

    def test_CalcIntervalLastDataDt_USA_hourly(self):
        interval = yfcd.Interval.Hours1
        day = date(2022, 2, 7)
        market_close_dt = datetime.combine(day, self.market_close, self.market_tz)

        lag = timedelta(0)
        dts = []
        answers = []
        for h in range(9, 16):
            dt = datetime.combine(day, time(h, 30), self.market_tz)
            dts.append(dt)
            if h == 15:
                answers.append(datetime.combine(day + self.td1d, time(9, 30), self.market_tz))
            else:
                answers.append(dt + self.td1h)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(self.exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

        lag = timedelta(minutes=15)
        dts = []
        answers = []
        for h in range(9, 16):
            dt = datetime.combine(day, time(h, 30), self.market_tz)
            dts.append(dt)
            if h == 15:
                answers.append(datetime.combine(day + self.td1d, time(9, 30), self.market_tz) + lag)
            else:
                answers.append(dt + self.td1h + lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(self.exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

    def test_CalcIntervalLastDataDt_USA_hourly_batch(self):
        interval = yfcd.Interval.Hours1

        lags = [timedelta(0), timedelta(minutes=15)]

        start_d = date.today()
        week_start_d = start_d - self.td1d*start_d.weekday()
        week2_start_d = week_start_d -7*self.td1d
        week1_start_d = week2_start_d -7*self.td1d
        days  = [week1_start_d+x*self.td1d for x in [0, 1, 2, 3, 4]]
        days += [week2_start_d+x*self.td1d for x in [0, 1, 2, 3, 4]]

        times = [time(h, 30) for h in range(9, 16)]
        dts = []

        for d in days:
            if yfct.ExchangeOpenOnDay(self.exchange, d):
                for t in times:
                    dts.append(datetime.combine(d, t, self.market_tz))

        for lag in lags:
            responses = yfct.CalcIntervalLastDataDt_batch(self.exchange, dts, interval, yf_lag=lag)
            return
            for i in range(len(dts)):
                answer = yfct.CalcIntervalLastDataDt(self.exchange, dts[i], interval, yf_lag=lag)
                try:
                    self.assertEqual(responses[i], answer)
                except:
                    print("dt = {}".format(dts[i]))
                    print("response = {}".format(responses[i]))
                    print("answer = {}".format(answer))
                    raise

    def test_CalcIntervalLastDataDt_NZE_hourly(self):
        interval = yfcd.Interval.Hours1

        day = date(2022, 4, 4)

        exchange = self.nze_exchange
        tz = self.nze_market_tz
        yfct.SetExchangeTzName(exchange, self.nze_market_tz_name)
        market_close_dt = datetime.combine(day, self.nze_market_close_time, tz)

        lag = timedelta(0)
        dts = []
        answers = []
        for h in range(10, 17):
            dt = datetime.combine(day, time(h), tz)
            dts.append(dt)
            if h == 16:
                answers.append(datetime.combine(day + self.td1d, time(10), tz))
            else:
                answers.append(min(dt+self.td1h, market_close_dt))
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

        lag = timedelta(minutes=15)
        dts = []
        answers = []
        for h in range(10, 17):
            dt = datetime.combine(day, time(h), tz)
            dts.append(dt)
            if h == 16:
                answers.append(datetime.combine(day + self.td1d, time(10), tz) + lag)
            else:
                answers.append(min(dt+self.td1h, market_close_dt) + lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

    # 1h interval, fetched during or v.soon after, tested during or v.soon after
    def test_duringDay(self):
        interval = yfcd.Interval.Hours1
        interval_start_dt = datetime.combine(self.monday, time(12, 30), self.market_tz)
        repaired = False

        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []


        # Simple aging of a mid-day interval
        fetch_dts.append(datetime.combine(self.monday, time(12, 35), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(13, 4),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(12, 35), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(13, 5),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday, time(12, 35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(13, 30), self.market_tz))
        expire_on_candle_closes.append(False)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(12, 35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(13, 30), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(12, 35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(13, 31), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday, time(12, 35), self.market_tz))
        max_ages.append(timedelta(hours=1))
        dt_nows.append(  datetime.combine(self.monday, time(13, 30), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(12, 35), self.market_tz))
        max_ages.append(timedelta(hours=1))
        dt_nows.append(  datetime.combine(self.monday, time(13, 31), self.market_tz))
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
            response = yfct.IsPriceDatapointExpired(interval_start_dt, fetch_dt, repaired, max_age, self.exchange, interval, triggerExpiryOnClose=expire_on_candle_close, yf_lag=yf_lag, dt_now=dt_now)
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

    # 1h interval end-of-day, fetched during interval, tested during | soon after | next morning
    def test_endOfDay(self):
        interval = yfcd.Interval.Hours1
        interval_start_dt = datetime.combine(self.monday, time(15, 30), self.market_tz)
        repaired = False
        
        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        fetch_dts.append(datetime.combine(self.monday, time(15, 35), self.market_tz))
        max_ages.append(timedelta(minutes=20))
        dt_nows.append(  datetime.combine(self.monday, time(15, 54), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(15, 35), self.market_tz))
        max_ages.append(timedelta(minutes=20))
        dt_nows.append(  datetime.combine(self.monday, time(15, 55), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday, time(15, 35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(15, 59), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)
        
        fetch_dts.append(datetime.combine(self.monday, time(15, 35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(16, 0), self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(15, 35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(16, 0), self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(15, 35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(16, 1), self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(15, 35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.tuesday, time(9, 31), self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(True)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            dt_now = dt_nows[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(interval_start_dt, fetch_dt, repaired, max_age, self.exchange, interval, triggerExpiryOnClose=expire_on_candle_close, yf_lag=yf_lag, dt_now=dt_now)
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

    # 1h interval end-of-Friday, fetched during interval, tested after market close | next Saturday
    def test_endOfFriday(self):
        interval = yfcd.Interval.Hours1
        interval_start_dt = datetime.combine(self.friday, time(15, 30), self.market_tz)
        repaired = False
        
        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        fetch_dts.append(datetime.combine(self.friday, time(15, 31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.friday, time(16, 1),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.friday, time(15, 31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.friday, time(16, 0),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.friday, time(15, 31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.friday, time(16, 0),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.friday, time(15, 31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.friday, time(16, 1),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.friday, time(15, 31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.saturday, time(12),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.friday, time(15, 31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday+7*self.td1d, time(9, 31),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(True)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            dt_now = dt_nows[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(interval_start_dt, fetch_dt, repaired, max_age, self.exchange, interval, triggerExpiryOnClose=expire_on_candle_close, yf_lag=yf_lag, dt_now=dt_now)
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
