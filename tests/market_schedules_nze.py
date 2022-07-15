import unittest
from pprint import pprint

from .context import yfc_time as yfct
from .context import yfc_dat as yfcd

from datetime import datetime,date,time,timedelta
from zoneinfo import ZoneInfo

## 2022 calendar:
## X* = day X is public holiday that closed exchange
##  -- April --
##  Mo   Tu   We   Th   Fr   Sa   Su
##  4    5    6    7    8    9    10
##  11   12   13   14   15*  16   17
##  18*  19*  20   21   22   23   24
##  25*

class Test_Market_Schedules_NZE(unittest.TestCase):

    def setUp(self):
        self.market = "nz_market"
        self.exchange = "NZE"
        self.tz = 'Pacific/Auckland'
        self.market_tz = ZoneInfo(self.tz)
        yfct.SetExchangeTzName(self.exchange, self.tz)

    def test_ExchangeOpenOnDay(self):
        # Weekdays
        for d in [4, 5, 6, 7, 8]:
            dt = date(year=2022, month=4, day=d)
            response = yfct.ExchangeOpenOnDay(self.exchange, dt)
            answer = True
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise

        ## Weekend
        for d in [9, 10]:
            dt = date(year=2022, month=4, day=d)
            response = yfct.ExchangeOpenOnDay(self.exchange, dt)
            answer = False
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise

        ## Public holiday
        dt = date(year=2022, month=4, day=18)
        response = yfct.ExchangeOpenOnDay(self.exchange, dt)
        answer = False
        try:
            self.assertEqual(response, answer)
        except:
            print("dt = {0}".format(dt))
            print("response = {0}".format(response))
            print("answer = {0}".format(answer))
            raise

    def test_ExchangeTimezone(self):
        tz2 = yfct.GetExchangeTzName("NZE")
        self.assertEqual(tz2, "Pacific/Auckland")

    def test_GetScheduleIntervals_hourly(self):
        interval = yfcd.Interval.Hours1

        for d in [4,5,6,7,8]:
            start_d = date(2022,4,d)
            end_d   = date(2022,4,d+1)
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
            answer_times = [(time(10,0), time(11,0)), 
                            (time(11,0), time(12,0)), 
                            (time(12,0), time(13,0)), 
                            (time(13,0), time(14,0)), 
                            (time(14,0), time(15,0)), 
                            (time(15,0), time(16,0)), 
                            (time(16,0), time(16,45))]
            answer = [(datetime.combine(start_d, t1, self.market_tz), datetime.combine(start_d, t2, self.market_tz)) for (t1,t2) in answer_times]
            try:
                self.assertEqual(response, answer)
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise

            start_d = date(2022,4,d)
            start_dt = datetime.combine(start_d, time(11,0), tzinfo=self.market_tz)
            end_dt = datetime.combine(start_d, time(14,0), tzinfo=self.market_tz)
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt, weeklyUseYahooDef=False)
            answer_times = [(time(11,0), time(12,0)),
                            (time(12,0), time(13,0)),
                            (time(13,0), time(14,0))]
            answer = [(datetime.combine(start_d, t1, self.market_tz), datetime.combine(start_d, t2, self.market_tz)) for (t1,t2) in answer_times]
            try:
                self.assertEqual(response, answer)
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise

            start_d = date(2022,4,d)
            start_dt = datetime.combine(start_d, time(10,30), tzinfo=self.market_tz)
            end_dt = datetime.combine(start_d, time(13,30), tzinfo=self.market_tz)
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt, weeklyUseYahooDef=False)
            answer_times = [(time(11,0), time(12,0)),
                            (time(12,0), time(13,0))]
            answer = [(datetime.combine(start_d, t1, self.market_tz), datetime.combine(start_d, t2, self.market_tz)) for (t1,t2) in answer_times]
            try:
                self.assertEqual(response, answer)
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise

    def test_GetScheduleIntervals_dayWeek(self):
        interval = yfcd.Interval.Days1
        start_d = date(2022,4,4) # Monday
        end_d = date(2022,4,16) # next Saturday
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
        answer_days = [date(2022,4,4), date(2022,4,5), date(2022,4,6), date(2022,4,7), date(2022,4,8), 
                        date(2022,4,11), date(2022,4,12), date(2022,4,13), date(2022,4,14)]
        answer = [(answer_days[i], answer_days[i]+timedelta(days=1)) for i in range(len(answer_days))]
        try:
            self.assertEqual(response, answer)
        except:
            print("response:")
            pprint(response)
            print("answer:")
            pprint(answer)
            raise

        intervals = [yfcd.Interval.Days5, yfcd.Interval.Week]
        start_d = date(2022,4,4) # Monday
        end_d = date(2022,4,16) # next Saturday
        answer = [(date(2022,4,4),date(2022,4,9)) , (date(2022,4,11),date(2022,4,15))]
        for interval in intervals:
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
            try:
                self.assertEqual(response, answer)
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise
        # Start/end dates are in middle of week:
        start_d = date(2022,4,6) # Wednesday
        end_d = date(2022,4,20) # Wednesday
        answer = [(date(2022,4,11),date(2022,4,15))]
        for interval in intervals:
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
            try:
                self.assertEqual(response, answer)
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise

    def test_IsTimestampInActiveSession_marketTz(self):
        times = [] ; answers = []
        times.append(time(9))     ; answers.append(False) # Before open
        times.append(time(10))    ; answers.append(True)  # During session
        times.append(time(16,45)) ; answers.append(False) # Right on close
        times.append(time(18))    ; answers.append(False) # After close

        for d in [4,5,6,7,8]:
            for i in range(len(times)):
                t = times[i]
                answer = answers[i]

                dt = datetime.combine(date(2022,4,d), t, self.market_tz)
                response = yfct.IsTimestampInActiveSession(self.exchange, dt)
                try:
                    self.assertEqual(response, answer)
                except:
                    print("dt = {0}".format(dt))
                    print("response = {0}".format(response))
                    print("answer = {0}".format(answer))
                    raise

        # Weekend:
        for d in [9,10]:
            dt = datetime(year=2022, month=4, day=d, hour=11, tzinfo=self.market_tz)
            response = yfct.IsTimestampInActiveSession(self.exchange, dt)
            answer = False
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise

    def test_IsTimestampInActiveSession_utcTz(self):
        times = [] ; answers = []
        times.append(time(9))     ; answers.append(False) # Before open
        times.append(time(10))    ; answers.append(True)  # During session
        times.append(time(16,45)) ; answers.append(False) # Right on close
        times.append(time(18))    ; answers.append(False) # After close

        utc_tz = ZoneInfo("UTC")

        for d in [4,5,6,7,8]:
            for i in range(len(times)):
                t = times[i]
                answer = answers[i]

                dt = datetime.combine(date(2022,4,d), t, utc_tz)
                response = yfct.IsTimestampInActiveSession(self.exchange, dt)
                answer = False
                try:
                    self.assertEqual(response, answer)
                except:
                    print("dt = {0}".format(dt))
                    print("response = {0}".format(response))
                    print("answer = {0}".format(answer))
                    raise

        # Weekend:
        for d in [9,10]:
            dt = datetime(year=2022, month=4, day=d, hour=1, tzinfo=utc_tz)
            response = yfct.IsTimestampInActiveSession(self.exchange, dt)
            answer = False
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise


    def test_GetTimestampCurrentSession(self):
        for d in [4,5,6,7,8]:
            # During session
            dt = datetime(year=2022, month=4, day=d, hour=10, tzinfo=self.market_tz)
            response = yfct.GetTimestampCurrentSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=4, day=d, hour=10, minute=0,  tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=4, day=d, hour=16, minute=45, tzinfo=self.market_tz)
            try:
                self.assertEqual(response["market_open"], answer_open)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_open"]))
                print("answer = {0}".format(answer_open))
                raise
            try:
                self.assertEqual(response["market_close"], answer_close)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_close"]))
                print("answer = {0}".format(answer_close))
                raise

            # Before session
            dt = datetime(year=2022, month=4, day=d, hour=8, tzinfo=self.market_tz)
            response = yfct.GetTimestampCurrentSession(self.exchange, dt)
            answer = None
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise

            # After session
            dt = datetime(year=2022, month=4, day=d, hour=20, tzinfo=self.market_tz)
            response = yfct.GetTimestampCurrentSession(self.exchange, dt)
            answer = None
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise

    def test_GetTimestampMostRecentSession(self):
        for d in [4,5,6,7,8]:
            if d > 4:
                # Before open
                dt = datetime(year=2022, month=4, day=d, hour=7, tzinfo=self.market_tz)
                response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
                answer_open  = datetime(year=2022, month=4, day=d-1, hour=10, minute=0,  tzinfo=self.market_tz)
                answer_close = datetime(year=2022, month=4, day=d-1, hour=16, minute=45, tzinfo=self.market_tz)
                try:
                    self.assertEqual(response["market_open"], answer_open)
                except:
                    print("dt = {0}".format(dt))
                    print("response = {0}".format(response["market_open"]))
                    print("answer = {0}".format(answer_open))
                    raise
                try:
                    self.assertEqual(response["market_close"], answer_close)
                except:
                    print("dt = {0}".format(dt))
                    print("response = {0}".format(response["market_close"]))
                    print("answer = {0}".format(answer_close))
                    raise

            # During session
            dt = datetime(year=2022, month=4, day=d, hour=10, tzinfo=self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=4, day=d, hour=10, minute=0,  tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=4, day=d, hour=16, minute=45, tzinfo=self.market_tz)
            try:
                self.assertEqual(response["market_open"], answer_open)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_open"]))
                print("answer = {0}".format(answer_open))
                raise
            try:
                self.assertEqual(response["market_close"], answer_close)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_close"]))
                print("answer = {0}".format(answer_close))
                raise

            # After session
            dt = datetime(year=2022, month=4, day=d, hour=20, tzinfo=self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=4, day=d, hour=10, minute=0,  tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=4, day=d, hour=16, minute=45, tzinfo=self.market_tz)
            try:
                self.assertEqual(response["market_open"], answer_open)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_open"]))
                print("answer = {0}".format(answer_open))
                raise
            try:
                self.assertEqual(response["market_close"], answer_close)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_close"]))
                print("answer = {0}".format(answer_close))
                raise

        # Weekend
        for d in [9,10]:
            dt = datetime(year=2022, month=4, day=d, hour=10, tzinfo=self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=4, day=8, hour=10, minute=0,  tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=4, day=8, hour=16, minute=45, tzinfo=self.market_tz)
            try:
                self.assertEqual(response["market_open"], answer_open)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_open"]))
                print("answer = {0}".format(answer_open))
                raise
            try:
                self.assertEqual(response["market_close"], answer_close)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_close"]))
                print("answer = {0}".format(answer_close))
                raise

    def test_GetTimestampNextSession(self):
        for d in [4,5,6,7,8]:
            # Before open
            dt = datetime(year=2022, month=4, day=d, hour=7, tzinfo=self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=4, day=d, hour=10, minute=0,  tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=4, day=d, hour=16, minute=45, tzinfo=self.market_tz)
            try:
                self.assertEqual(response["market_open"], answer_open)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_open"]))
                print("answer = {0}".format(answer_open))
                raise
            try:
                self.assertEqual(response["market_close"], answer_close)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_close"]))
                print("answer = {0}".format(answer_close))
                raise

            if d == 8:
                next_d = 11
            else:
                next_d = d+1

            # During session
            dt = datetime(year=2022, month=4, day=d, hour=10, tzinfo=self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=4, day=next_d, hour=10, minute=0,  tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=4, day=next_d, hour=16, minute=45, tzinfo=self.market_tz)
            try:
                self.assertEqual(response["market_open"], answer_open)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_open"]))
                print("answer = {0}".format(answer_open))
                raise
            try:
                self.assertEqual(response["market_close"], answer_close)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_close"]))
                print("answer = {0}".format(answer_close))
                raise

            # After session
            dt = datetime(year=2022, month=4, day=d, hour=20, tzinfo=self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=4, day=next_d, hour=10, minute=0,  tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=4, day=next_d, hour=16, minute=45, tzinfo=self.market_tz)
            try:
                self.assertEqual(response["market_open"], answer_open)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_open"]))
                print("answer = {0}".format(answer_open))
                raise
            try:
                self.assertEqual(response["market_close"], answer_close)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_close"]))
                print("answer = {0}".format(answer_close))
                raise

        # On Weekend
        for d in [9,10]:
            dt = datetime(year=2022, month=4, day=d, hour=20, tzinfo=self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            # Monday 17th is holiday
            answer_open  = datetime(year=2022, month=4, day=11, hour=10, minute=0,  tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=4, day=11, hour=16, minute=45, tzinfo=self.market_tz)
            try:
                self.assertEqual(response["market_open"], answer_open)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_open"]))
                print("answer = {0}".format(answer_open))
                raise
            try:
                self.assertEqual(response["market_close"], answer_close)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response["market_close"]))
                print("answer = {0}".format(answer_close))
                raise


if __name__ == '__main__':
    unittest.main()
