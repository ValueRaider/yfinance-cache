import unittest

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

import pandas as pd

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

class Test_PriceDataAging_1D(unittest.TestCase):

    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz_name = 'US/Eastern'
        self.market_tz = ZoneInfo(self.market_tz_name)
        self.market_open = time(9, 30)
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
        self.friday = date(2022, 2, 18)
        self.saturday = date(2022, 2, 19)

        self.td1h = timedelta(hours=1)
        self.td4h = timedelta(hours=4)
        self.td1d = timedelta(days=1)

        yfct.SetExchangeTzName(self.exchange, self.market_tz_name)


    def test_CalcIntervalLastDataDt_USA_daily_datetime(self):
        interval = yfcd.Interval.Days1

        # Is next-next working day afternoon
        lag = timedelta(0)
        days = []
        dts = []
        answers = []
        for d in range(7, 12):
            day = date(2022, 2, d)
            days.append(day)
            dt = datetime.combine(day, time(14, 30), self.market_tz)
            dts.append(dt)
            if d >= 10:
                answers.append(datetime.combine(day + 4*self.td1d, self.market_open, self.market_tz) + self.td4h)
            else:
                answers.append(datetime.combine(day + 2*self.td1d, self.market_open, self.market_tz) + self.td4h)
        for i in range(len(days)):
            response = yfct.CalcIntervalLastDataDt(self.exchange, days[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("day = {}".format(days[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(self.exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

        # Is next-next working day afternoon
        lag = timedelta(minutes=15)
        days = []
        dts = []
        answers = []
        for d in range(7, 12):
            day = date(2022, 2, d)
            days.append(day)
            dt = datetime.combine(day, time(14, 30), self.market_tz)
            dts.append(dt)
            if d >= 10:
                answers.append(datetime.combine(day + 4*self.td1d, self.market_open, self.market_tz) + self.td4h + lag)
            else:
                answers.append(datetime.combine(day + 2*self.td1d, self.market_open, self.market_tz) + self.td4h + lag)
        for i in range(len(days)):
            response = yfct.CalcIntervalLastDataDt(self.exchange, days[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("day = {}".format(days[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(self.exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

        # Is next Tuesday open, after Monday holiday
        day = date(2022, 2, 17)
        dt = datetime.combine(day, time(12), self.market_tz)
        lag = timedelta(0)
        answer = datetime.combine(date(2022, 2, 22), self.market_open, self.market_tz) + self.td4h + lag
        response = yfct.CalcIntervalLastDataDt(self.exchange, day, interval, yf_lag=lag)
        try:
            self.assertEqual(response, answer)
        except:
            print("day = {}".format(day))
            print("response = {}".format(response))
            print("answer = {}".format(answer))
            raise
        response = yfct.CalcIntervalLastDataDt(self.exchange, dt, interval, yf_lag=lag)
        try:
            self.assertEqual(response, answer)
        except:
            print("dt = {}".format(dt))
            print("response = {}".format(response))
            print("answer = {}".format(answer))
            raise

    def test_CalcIntervalLastDataDt_USA_daily_date(self):
        interval = yfcd.Interval.Days1

        lag = timedelta(0)
        days = []
        answers = []
        for d in range(7, 12):
            day = date(2022, 2, d)
            days.append(day)
            if d >= 10:
                answers.append(datetime.combine(day + 4*self.td1d, self.market_open, self.market_tz) + self.td4h)
            else:
                answers.append(datetime.combine(day + 2*self.td1d, self.market_open, self.market_tz) + self.td4h)
        for i in range(len(days)):
            response = yfct.CalcIntervalLastDataDt(self.exchange, days[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(days[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

        lag = timedelta(minutes=15)
        days = []
        answers = []
        for d in range(7, 12):
            day = date(2022, 2, d)
            days.append(day)
            if d >= 10:
                answers.append(datetime.combine(day + 4*self.td1d, self.market_open, self.market_tz) + self.td4h + lag)
            else:
                answers.append(datetime.combine(day + 2*self.td1d, self.market_open, self.market_tz) + self.td4h + lag)
        for i in range(len(days)):
            response = yfct.CalcIntervalLastDataDt(self.exchange, days[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

    def test_CalcIntervalLastDataDt_USA_daily_batch(self):
        interval = yfcd.Interval.Days1

        lags = [timedelta(0), timedelta(minutes=15)]

        start_d = date.today()
        week_start_d = start_d - self.td1d*start_d.weekday()
        week2_start_d = week_start_d -7*self.td1d
        week1_start_d = week2_start_d -7*self.td1d
        days = []
        for d in [week1_start_d+x*self.td1d for x in [0, 1, 2, 3, 4]]:
            if yfct.ExchangeOpenOnDay(self.exchange, d):
                days.append(d)
        for d in [week2_start_d+x*self.td1d for x in [0, 1, 2, 3, 4]]:
            if yfct.ExchangeOpenOnDay(self.exchange, d):
                days.append(d)

        for lag in lags:
            responses = yfct.CalcIntervalLastDataDt_batch(self.exchange, days, interval, yf_lag=lag)
            for i in range(len(days)):
                answer = yfct.CalcIntervalLastDataDt(self.exchange, days[i], interval, yf_lag=lag)
                try:
                    if answer is None:
                        self.assertTrue(pd.isna(responses[i]))
                    else:
                        self.assertEqual(responses[i], answer)
                except:
                    print(f"day={days[i]} lag={lag}")
                    print("response = {}".format(responses[i]))
                    print("answer = {}".format(answer))
                    raise


        times = [time(10), time(15)]
        dts = []
        for d in days:
            if yfct.ExchangeOpenOnDay(self.exchange, d):
                for t in times:
                    dts.append(datetime.combine(d, t, self.market_tz))
        for lag in lags:
            responses = yfct.CalcIntervalLastDataDt_batch(self.exchange, dts, interval, yf_lag=lag)
            for i in range(len(dts)):
                answer = yfct.CalcIntervalLastDataDt(self.exchange, dts[i], interval, yf_lag=lag)
                try:
                    if answer is None:
                        self.assertTrue(pd.isna(responses[i]))
                    else:
                        self.assertEqual(responses[i], answer)
                except:
                    print(f"dt={dts[i]} lag={lag}")
                    print("response = {}".format(responses[i]))
                    print("answer = {}".format(answer))
                    raise

    def test_CalcIntervalLastDataDt_FX_daily_batch(self):
        fx_exchange = 'CCY'

        interval = yfcd.Interval.Days1

        lags = [timedelta(0), timedelta(minutes=15)]

        start_d = date.today()
        week_start_d = start_d - self.td1d*start_d.weekday()
        week2_start_d = week_start_d -7*self.td1d
        week1_start_d = week2_start_d -7*self.td1d
        days = []
        for d in [week1_start_d+x*self.td1d for x in [0, 1, 2, 3, 4]]:
            if yfct.ExchangeOpenOnDay(self.exchange, d):
                days.append(d)
        # for d in [week2_start_d+x*self.td1d for x in [0, 1, 2, 3, 4]]:
        #     if yfct.ExchangeOpenOnDay(self.exchange, d):
        #         days.append(d)

        for lag in lags:
            responses = yfct.CalcIntervalLastDataDt_batch(fx_exchange, days, interval, yf_lag=lag)
            for i in range(len(days)):
                answer = yfct.CalcIntervalLastDataDt(fx_exchange, days[i], interval, yf_lag=lag)
                try:
                    if answer is None:
                        self.assertTrue(pd.isna(responses[i]))
                    else:
                        self.assertEqual(responses[i], answer)
                except:
                    print(f"day={days[i]} lag={lag}")
                    print("response = {}".format(responses[i]))
                    print("answer = {}".format(answer))
                    raise


        times = [time(10), time(15)]
        dts = []
        for d in days:
            if yfct.ExchangeOpenOnDay(fx_exchange, d):
                for t in times:
                    dts.append(datetime.combine(d, t, self.market_tz))
        for lag in lags:
            responses = yfct.CalcIntervalLastDataDt_batch(fx_exchange, dts, interval, yf_lag=lag)
            for i in range(len(dts)):
                answer = yfct.CalcIntervalLastDataDt(fx_exchange, dts[i], interval, yf_lag=lag)
                try:
                    if answer is None:
                        self.assertTrue(pd.isna(responses[i]))
                    else:
                        self.assertEqual(responses[i], answer)
                except:
                    print(f"dt={dts[i]} lag={lag}")
                    print("response = {}".format(responses[i]))
                    print("answer = {}".format(answer))
                    raise

    def test_CalcIntervalLastDataDt_NZE_daily(self):
        interval = yfcd.Interval.Days1

        exchange = self.nze_exchange
        tz = self.nze_market_tz
        yfct.SetExchangeTzName(exchange, self.nze_market_tz_name)

        lag = timedelta(0)
        dts = []
        answers = []
        for d in range(4, 9):
            day = date(2022, 4, d)
            dt = datetime.combine(day, time(14), tz)
            dts.append(dt)
            if d >= 7:
                answers.append(datetime.combine(day+4*self.td1d, time(10), tz) + self.td4h)
            else:
                answers.append(datetime.combine(day+2*self.td1d, time(10), tz) + self.td4h)
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

        lag = timedelta(minutes=15)
        dts = []
        answers = []
        for d in range(4, 9):
            day = date(2022, 4, d)
            dt = datetime.combine(day, time(14), tz)
            dts.append(dt)
            if d >= 7:
                answers.append(datetime.combine(day+4*self.td1d, time(10), tz) + self.td4h + lag)
            else:
                answers.append(datetime.combine(day+2*self.td1d, time(10), tz) + self.td4h + lag)
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

    def test_CalcIntervalLastDataDt_UK_daily(self):
        interval = yfcd.Interval.Days1

        exchange = self.uk_exchange
        tz = self.uk_market_tz
        yfct.SetExchangeTzName(exchange, self.uk_market_tz_name)

        lag = timedelta(0)
        dts = []
        answers = []
        for d in range(4, 9):
            day = date(2022, 4, d)
            dt = datetime.combine(day, time(14), tz)
            dts.append(dt)
            if d >= 7:
                answers.append(datetime.combine(day+4*self.td1d, self.uk_market_open_time, tz) + self.td4h)
            else:
                answers.append(datetime.combine(day+2*self.td1d, self.uk_market_open_time, tz) + self.td4h)
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

        lag = timedelta(minutes=15)
        dts = []
        answers = []
        for d in range(4, 9):
            day = date(2022, 4, d)
            dt = datetime.combine(day, time(14), tz)
            dts.append(dt)
            if d >= 7:
                answers.append(datetime.combine(day+4*self.td1d, self.uk_market_open_time, tz) + self.td4h + lag)
            else:
                answers.append(datetime.combine(day+2*self.td1d, self.uk_market_open_time, tz) + self.td4h + lag)
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

    # Test day interval fetched same day
    def test_sameDay(self):
        interval = yfcd.Interval.Days1

        intervalStartD = self.monday
        repaired = False

        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        # Simple aging of a mid-day interval
        fetch_dts.append(datetime.combine(self.monday, time(12, 5),  self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(12, 34), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(12, 5),  self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(12, 35), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday, time(12, 5),  self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(13, 4), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(12, 5),  self.market_tz))
        max_ages.append(timedelta(hours=1))
        dt_nows.append(  datetime.combine(self.monday, time(13, 5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(True)


        # Test end-of-session behaviour
        fetch_dts.append(datetime.combine(self.monday, time(15, 30), self.market_tz))
        max_ages.append(timedelta(minutes=29))
        dt_nows.append(  datetime.combine(self.monday, time(16),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday, time(15, 30), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(17),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(0))
        answers.append(False)


        # Test midnight behaviour
        fetch_dts.append(datetime.combine(self.monday, time(23, 55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(0, 0),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(23, 55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(0, 0),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(23, 55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(0, 0),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(23, 55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(0, 1),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(False)


        # Test expiry when next day trading begins
        # - fetched last midnight
        fetch_dts.append(datetime.combine(self.monday,  time(23, 55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(9, 0),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday,  time(23, 55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(9, 30),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday,  time(23, 55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(9, 30),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday,  time(23, 55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(9, 31),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(True)

        # - fetched just before next day trading
        fetch_dts.append(datetime.combine(self.tuesday,  time(9, 15), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(9, 30),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.tuesday,  time(9, 15), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(9, 31),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.tuesday,  time(9, 15), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(9, 45),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(True)

        # Fetch after market open, but within 4 hours
        fetch_dts.append(datetime.combine(self.monday,  time(10, 0), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(11, 0),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=0))
        answers.append(True)

        # Fetched before candle close (4 hours after open)
        fetch_dts.append(datetime.combine(self.tuesday,  time(13), self.market_tz))
        max_ages.append(timedelta(minutes=120))
        dt_nows.append(  datetime.combine(self.tuesday, time(13, 30),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=0))
        answers.append(False)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            dt_now = dt_nows[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(intervalStartD, fetch_dt, repaired, max_age, self.exchange, interval, triggerExpiryOnClose=expire_on_candle_close, yf_lag=yf_lag, dt_now=dt_now)
            try:
                self.assertEqual(response, answer)
            except:
                print("fetch_dt = {0}".format(fetch_dt))
                print("expire_on_candle_close = {}".format(expire_on_candle_close))
                print("yf_lag = {}".format(yf_lag))
                print("max_age = {0}".format(max_age))
                print("dt_now = {0}".format(dt_now))
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(response)
                raise

    # Test Friday interval fetched same day when dt_now is next day (weekend)
    def test_nextDayWeekend(self):
        interval = yfcd.Interval.Days1
        intervalStart = self.friday
        repaired = False

        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        max_ages.append(timedelta(minutes=30))
        fetch_dts.append(datetime.combine(self.friday,   time(14, 55), self.market_tz))
        dt_nows.append(  datetime.combine(self.saturday, time(9, 10), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(True)

        max_ages.append(timedelta(minutes=30))
        fetch_dts.append(datetime.combine(self.friday,   time(23, 55), self.market_tz))
        dt_nows.append(  datetime.combine(self.saturday, time(9, 10), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(timedelta(0))
        answers.append(False)

        max_ages.append(timedelta(minutes=30))
        fetch_dts.append(datetime.combine(self.friday,   time(23, 55), self.market_tz))
        dt_nows.append(  datetime.combine(self.saturday, time(9, 10), self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(0))
        answers.append(False)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            dt_now = dt_nows[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(intervalStart, fetch_dt, repaired, max_age, self.exchange, interval, triggerExpiryOnClose=expire_on_candle_close, yf_lag=yf_lag, dt_now=dt_now)
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
