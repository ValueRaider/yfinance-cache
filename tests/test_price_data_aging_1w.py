import unittest

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from pprint import pprint

## 2022 calendar:
## X* = day X is USA public holiday that closed NYSE
##  -- February --
##  Mo   Tu   We   Th   Fr   Sa   Su
##  7    8    9    10   11   12   13
##  14   15   16   17   18   19   20
##  21*  22   23   24   25   26   27
##  28

class Test_PriceDataAging_1W(unittest.TestCase):
    def setUp(self):
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.market_open  = time(9, 30)
        self.market_close = time(16, 0)

        self.nze_exchange = "NZE"
        self.nze_market_tz_name = 'Pacific/Auckland'
        self.nze_market_tz = ZoneInfo('Pacific/Auckland')
        self.nze_market_open_time  = time(10)

        self.uk_exchange = "LSE"
        self.uk_market_tz_name = "Europe/London"
        self.uk_market_tz = ZoneInfo(self.uk_market_tz_name)
        self.uk_market_open_time = time(8)

        self.monday = date(2022, 2, 14)
        self.tuesday = date(2022, 2, 15)
        self.wednday = date(2022, 2, 16)
        self.friday = date(2022, 2, 18)
        self.saturday = date(2022, 2, 19)

        self.td1h = timedelta(hours=1)
        self.td4h = timedelta(hours=4)
        self.td1d = timedelta(days=1)

    def test_CalcIntervalLastDataDt_USA_weekly(self):
        interval = yfcd.Interval.Week

        lag = timedelta(minutes=15)
        dts = []
        answers = []
        week_start_day = date(2022, 2, 7)
        answer = datetime.combine(week_start_day + 7*self.td1d, self.market_open, self.market_tz) + self.td4h+self.td1d
        for d in range(7, 12):
            day = date(2022, 2,d)
            dt = datetime.combine(day, time(14, 30), self.market_tz)
            dts.append(dt)
            answers.append(answer+lag)

        week_start_day = date(2022, 2, 14)
        answer = datetime.combine(week_start_day + 7*self.td1d, self.market_open, self.market_tz) + self.td4h+self.td1d + self.td1d  # Monday holiday
        for d in range(14, 19):
            day = date(2022, 2,d)
            dt = datetime.combine(day, time(14, 30), self.market_tz)
            dts.append(dt)
            answers.append(answer + lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(self.exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

    def test_CalcIntervalLastDataDt_USA_weekly_batch(self):
        interval = yfcd.Interval.Week

        lags = [timedelta(0), timedelta(minutes=15)]

        start_d = date.today()
        week_start_d = start_d - self.td1d*start_d.weekday()
        week2_start_d = week_start_d -7*self.td1d
        week1_start_d = week2_start_d -7*self.td1d
        days  = [week1_start_d+x*self.td1d for x in [0, 1, 2, 3, 4]]
        days += [week2_start_d+x*self.td1d for x in [0, 1, 2, 3, 4]]

        times = [time(10), time(15)]
        dts = []

        for d in days:
            if yfct.ExchangeOpenOnDay(self.exchange, d):
                for t in times:
                    dts.append(datetime.combine(d, t, self.market_tz))

        # debugging:
        # dts = [datetime.combine(date(2023, 2, 13), time(10), self.market_tz)]

        for lag in lags:
            responses = yfct.CalcIntervalLastDataDt_batch(self.exchange, dts, interval, yf_lag=lag)
            for i in range(len(dts)):
                answer = yfct.CalcIntervalLastDataDt(self.exchange, dts[i], interval, yf_lag=lag)
                try:
                    # self.assertEqual(responses[i], answer)
                    if answer is None:
                        self.assertTrue(pd.isna(responses[i]))
                    else:
                        self.assertEqual(responses[i], answer)
                except:
                    print(f"dt={dts[i]} lag={lag}")
                    print("response = {}".format(responses[i]))
                    print("answer = {}".format(answer))
                    raise

    def test_CalcIntervalLastDataDt_NZE_weekly(self):
        interval = yfcd.Interval.Week

        exchange = self.nze_exchange
        tz = self.nze_market_tz
        yfct.SetExchangeTzName(exchange, self.nze_market_tz_name)

        lag = timedelta(minutes=15)
        dts = []
        answers = []
        week_start_day = date(2022, 4, 4)
        answer = datetime.combine(date(2022, 4, 11), self.nze_market_open_time, tz) + self.td4h+self.td1d
        for d in range(4, 9):
            day = date(2022, 4, d)

            dts.append(datetime.combine(day, time(0), tz))
            answers.append(answer+lag)
            dts.append(datetime.combine(day, time(12), tz))
            answers.append(answer+lag)
            dts.append(datetime.combine(day, time(20), tz))
            answers.append(answer+lag)

        week_start_day = date(2022, 4, 11)
        answer = datetime.combine(date(2022, 4, 19), time(10), tz) + self.td4h+self.td1d
        for d in range(11, 16):
            day = date(2022, 4, d)
            
            dts.append(datetime.combine(day, time(0), tz))
            answers.append(answer+lag)
            dts.append(datetime.combine(day, time(12), tz))
            answers.append(answer+lag)
            dts.append(datetime.combine(day, time(20), tz))
            answers.append(answer+lag)


        response_batch = yfct.CalcIntervalLastDataDt_batch(exchange, dts, interval, yf_lag=lag)

        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

            try:
                self.assertEqual(response_batch[i], answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response_batch[i] = {}".format(response_batch[i]))
                print("answer = {}".format(answers[i]))
                raise

    def test_CalcIntervalLastDataDt_UK_weekly(self):
        interval = yfcd.Interval.Week

        exchange = self.uk_exchange
        tz = self.uk_market_tz
        yfct.SetExchangeTzName(exchange, self.uk_market_tz_name)

        lag = timedelta(0)
        dts = []
        answers = []
        answer = datetime.combine(date(2022, 5, 9), time(8), tz) + self.td4h+self.td1d
        for d in range(3, 7):  # 2nd is holiday
            day = date(2022, 5, d)
            dt = datetime.combine(day, time(14, 30), tz)
            dts.append(dt)
            answers.append(answer+lag)

        response_batch = yfct.CalcIntervalLastDataDt_batch(exchange, dts, interval, yf_lag=lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

            try:
                self.assertEqual(response, response_batch[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response_batch[i]))
                print("answer = {}".format(response))
                raise

    # Test same day same week
    def test_sameWeek_sameDay(self):
        interval_start = self.monday
        interval = yfcd.Interval.Week
        repaired = False

        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        max_ages.append(timedelta(hours=2))
        fetch_dts.append(datetime.combine(self.monday, time(12, 5), self.market_tz))
        dt_nows.append(  datetime.combine(self.monday, time(14, 4), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        max_ages.append(timedelta(hours=2))
        fetch_dts.append(datetime.combine(self.monday, time(12, 5), self.market_tz))
        dt_nows.append(  datetime.combine(self.monday, time(14, 5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        max_ages.append(timedelta(hours=1))
        fetch_dts.append(datetime.combine(self.monday, time(15, 30), self.market_tz))
        dt_nows.append(  datetime.combine(self.monday, time(17), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            dt_now = dt_nows[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(interval_start, fetch_dt, repaired, max_age, self.exchange, interval, triggerExpiryOnClose=expire_on_candle_close, yf_lag=yf_lag, dt_now=dt_now)
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

    # Test different day same week
    def test_sameWeek_diffDay(self):
        interval_start = self.monday
        interval = yfcd.Interval.Week
        repaired = False

        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        max_ages.append(timedelta(hours=1))
        fetch_dts.append(datetime.combine(self.monday, time(15, 30), self.market_tz))
        dt_nows.append(  datetime.combine(self.tuesday, time(9, 30), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)


        max_ages.append(timedelta(hours=24))
        fetch_dts.append(datetime.combine(self.monday,  time(12, 5), self.market_tz))
        dt_nows.append(  datetime.combine(self.tuesday, time(12, 4), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)
        max_ages.append(timedelta(hours=24))
        fetch_dts.append(datetime.combine(self.monday,  time(12, 5), self.market_tz))
        dt_nows.append(  datetime.combine(self.tuesday, time(12, 5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)
        max_ages.append(timedelta(hours=24))
        fetch_dts.append(datetime.combine(self.monday,  time(12, 5), self.market_tz))
        dt_nows.append(  datetime.combine(self.tuesday, time(12, 5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(minutes=1))
        answers.append(True)

        max_ages.append(timedelta(days=1))
        fetch_dts.append(datetime.combine(self.monday,  time(12, 5), self.market_tz))
        dt_nows.append(  datetime.combine(self.tuesday, time(11, 4), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)
        max_ages.append(timedelta(days=1))
        fetch_dts.append(datetime.combine(self.monday,  time(12, 5), self.market_tz))
        dt_nows.append(  datetime.combine(self.tuesday, time(12, 5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)


        max_ages.append(timedelta(days=2))
        fetch_dts.append(datetime.combine(self.monday,  time(12, 5), self.market_tz))
        dt_nows.append(  datetime.combine(self.tuesday, time(12, 5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        max_ages.append(timedelta(days=2))
        fetch_dts.append(datetime.combine(self.monday,  time(12, 5), self.market_tz))
        dt_nows.append(  datetime.combine(self.wednday, time(12, 5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)


        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            dt_now = dt_nows[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(interval_start, fetch_dt, repaired, max_age, self.exchange, interval, triggerExpiryOnClose=expire_on_candle_close, yf_lag=yf_lag, dt_now=dt_now)
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

    # Test weekend following week
    def test_sameWeek_weekend(self):
        interval_start = self.monday
        interval = yfcd.Interval.Week
        repaired = False

        dt_now = datetime.combine(self.saturday, time(9, 0), self.market_tz)
        max_ages = []
        fetch_dts = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        max_ages.append(timedelta(hours=2))
        fetch_dts.append(datetime.combine(self.monday, time(13, 44), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        max_ages.append(timedelta(hours=2))
        fetch_dts.append(datetime.combine(self.monday, time(13, 45), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(interval_start, fetch_dt, repaired, max_age, self.exchange, interval, triggerExpiryOnClose=expire_on_candle_close, yf_lag=yf_lag, dt_now=dt_now)
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

    # Test next week
    def test_nextWeek(self):
        interval_start = self.monday
        interval = yfcd.Interval.Week
        repaired = False

        dt_now = datetime.combine(self.monday + timedelta(days=8), time(13, 45), self.market_tz)  # next Monday is holiday
        max_ages = []
        fetch_dts = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        max_ages.append(timedelta(days=8))
        fetch_dts.append(datetime.combine(self.monday, time(13, 30), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        max_ages.append(timedelta(days=8))
        fetch_dts.append(datetime.combine(self.monday, time(14), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(interval_start, fetch_dt, repaired, max_age, self.exchange, interval, triggerExpiryOnClose=expire_on_candle_close, yf_lag=yf_lag, dt_now=dt_now)
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
