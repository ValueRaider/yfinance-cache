import unittest
from pprint import pprint

from .context import yfc_time as yfct
from .context import yfc_dat as yfcd

from datetime import datetime,date,time,timedelta
from zoneinfo import ZoneInfo

## 2022 calendar:
## X* = day X is USA public holiday that closed NYSE
##  -- February --
##  Mo   Tu   We   Th   Fr   Sa   Su
##  7    8    9    10   11   12   13
##  14   15   16   17   18   19   20
##  21*  22   23   24   25   26   27
##  28

class Test_Market_Schedules_USA(unittest.TestCase):

    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.tz = 'US/Eastern'
        self.market_tz = ZoneInfo(self.tz)
        yfct.SetExchangeTzName(self.exchange, self.tz)

    def test_ExchangeOpenOnDay(self):
        # Weekdays
        for d in [14, 15, 16, 17, 18]:
            dt = date(year=2022, month=2, day=d)
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
        for d in [19, 20]:
            dt = date(year=2022, month=2, day=d)
            response = yfct.ExchangeOpenOnDay(self.exchange, dt)
            answer = False
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise

        ## Thanksgiving Day
        dt = date(year=2022, month=11, day=24)
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
        tz2 = yfct.GetExchangeTzName("NMS")
        self.assertEqual(tz2, "US/Eastern")

    def test_GetScheduleIntervals_hourly(self):
        interval = yfcd.Interval.Hours1

        for d in [14,15,16,17,18]:
            start_d = date(2022,2,d)
            end_d   = date(2022,2,d+1)

            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
            answer_times = [(time( 9,30), time(10,30)),
                            (time(10,30), time(11,30)), 
                            (time(11,30), time(12,30)), 
                            (time(12,30), time(13,30)), 
                            (time(13,30), time(14,30)), 
                            (time(14,30), time(15,30)), 
                            (time(15,30), time(16,0))]
            answer = [(datetime.combine(start_d, t1, self.market_tz), datetime.combine(start_d, t2, self.market_tz)) for (t1,t2) in answer_times]
            try:
                self.assertEqual(response, answer)
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise

            start_dt = datetime.combine(start_d, time(11,30), tzinfo=self.market_tz)
            end_dt   = datetime.combine(start_d, time(14,30), tzinfo=self.market_tz)
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt, weeklyUseYahooDef=False)
            answer_times = [(time(11,30), time(12,30)),
                            (time(12,30), time(13,30)),
                            (time(13,30), time(14,30))]
            answer = [(datetime.combine(start_d, t1, self.market_tz), datetime.combine(start_d, t2, self.market_tz)) for (t1,t2) in answer_times]
            try:
                self.assertEqual(response, answer)
            except:
                print("response:")
                pprint(response)
                print("answer:")
                pprint(answer)
                raise

            start_dt = datetime.combine(start_d, time(11,0), tzinfo=self.market_tz)
            end_dt   = datetime.combine(start_d, time(14,0), tzinfo=self.market_tz)
            response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_dt, end_dt, weeklyUseYahooDef=False)
            answer_times = [(time(11,30), time(12,30)),
                            (time(12,30), time(13,30))]
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
        start_d = date(2022,2,14) # Monday
        end_d   = date(2022,2,26) # next Saturday
        response = yfct.GetExchangeScheduleIntervals(self.exchange, interval, start_d, end_d, weeklyUseYahooDef=False)
        answer_days = [date(2022,2,14), date(2022,2,15), date(2022,2,16), date(2022,2,17), date(2022,2,18), 
                                        date(2022,2,22), date(2022,2,23), date(2022,2,24), date(2022,2,25)]
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
        start_d = date(2022,2,14) # Monday
        end_d   = date(2022,2,26) # next Saturday
        answer = [(date(2022,2,14),date(2022,2,19)) , (date(2022,2,22),date(2022,2,26))]
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
        start_d = date(2022,2,9) # Wednesday
        end_d   = date(2022,2,23) # Wednesday
        answer = [(date(2022,2,14),date(2022,2,19))]
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
        return

    def test_IsTimestampInActiveSession_marketTz(self):
        hours = [] ; answers = []
        hours.append( 9) ; answers.append(False)
        hours.append(10) ; answers.append(True)
        hours.append(16) ; answers.append(False)
        hours.append(17) ; answers.append(False)

        for d in [14,15,16,17,18]:
            for i in range(len(hours)):
                h = hours[i]
                answer = answers[i]

                dt = datetime(year=2022, month=2, day=d, hour=h, tzinfo=self.market_tz)
                response = yfct.IsTimestampInActiveSession(self.exchange, dt)
                try:
                    self.assertEqual(response, answer)
                except:
                    print("dt = {0}".format(dt))
                    print("response = {0}".format(response))
                    print("answer = {0}".format(answer))
                    raise

        # Weekend:
        for d in [19,20]:
            dt = datetime(year=2022, month=2, day=d, hour=11, tzinfo=self.market_tz)
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
        hours = [] ; answers = []
        hours.append(14) ; answers.append(False) # Before open
        hours.append(15) ; answers.append(True)  # During session
        hours.append(21) ; answers.append(False) # Right on close
        hours.append(22) ; answers.append(False) # After close

        utc_tz = ZoneInfo("UTC")

        for d in [14,15,16,17,18]:
            for i in range(len(hours)):
                h = hours[i]
                answer = answers[i]

                dt = datetime(year=2022, month=2, day=d, hour=h, tzinfo=utc_tz)
                response = yfct.IsTimestampInActiveSession(self.exchange, dt)
                try:
                    self.assertEqual(response, answer)
                except:
                    print("dt = {0}".format(dt))
                    print("response = {0}".format(response))
                    print("answer = {0}".format(answer))
                    raise

        # Weekend:
        for d in [19,20]:
            dt = datetime(year=2022, month=2, day=d, hour=10, tzinfo=utc_tz)
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
        for d in [14,15,16,17,18]:
            # During session
            dt = datetime(year=2022, month=2, day=d, hour=10, tzinfo=self.market_tz)
            response = yfct.GetTimestampCurrentSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=2, day=d, hour=9, minute=30, tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=2, day=d, hour=16, minute=0, tzinfo=self.market_tz)
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
            dt = datetime(year=2022, month=2, day=d, hour=8, tzinfo=self.market_tz)
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
            dt = datetime(year=2022, month=2, day=d, hour=20, tzinfo=self.market_tz)
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
        for d in [14,15,16,17,18]:
            if d > 14:
                # Before open
                dt = datetime(year=2022, month=2, day=d, hour=7, tzinfo=self.market_tz)
                response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
                answer_open  = datetime(year=2022, month=2, day=d-1, hour=9, minute=30, tzinfo=self.market_tz)
                answer_close = datetime(year=2022, month=2, day=d-1, hour=16, minute=0, tzinfo=self.market_tz)
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
            dt = datetime(year=2022, month=2, day=d, hour=10, tzinfo=self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=2, day=d, hour=9, minute=30, tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=2, day=d, hour=16, minute=0, tzinfo=self.market_tz)
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
            dt = datetime(year=2022, month=2, day=d, hour=20, tzinfo=self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=2, day=d, hour=9, minute=30, tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=2, day=d, hour=16, minute=0, tzinfo=self.market_tz)
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
        for d in [19,20]:
            dt = datetime(year=2022, month=2, day=d, hour=10, tzinfo=self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=2, day=18, hour=9, minute=30, tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=2, day=18, hour=16, minute=0, tzinfo=self.market_tz)
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
        for d in [14,15,16,17,18]:
            # Before open
            dt = datetime(year=2022, month=2, day=d, hour=7, tzinfo=self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=2, day=d, hour=9, minute=30, tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=2, day=d, hour=16, minute=0, tzinfo=self.market_tz)
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

            if d == 18:
                next_d = 22
            else:
                next_d = d+1

            # During session
            dt = datetime(year=2022, month=2, day=d, hour=10, tzinfo=self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=2, day=next_d, hour=9, minute=30, tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=2, day=next_d, hour=16, minute=0, tzinfo=self.market_tz)
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
            dt = datetime(year=2022, month=2, day=d, hour=20, tzinfo=self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open  = datetime(year=2022, month=2, day=next_d, hour=9, minute=30, tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=2, day=next_d, hour=16, minute=0, tzinfo=self.market_tz)
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
            dt = datetime(year=2022, month=2, day=13, hour=20, tzinfo=self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            # Monday 17th is holiday
            answer_open  = datetime(year=2022, month=2, day=14, hour=9, minute=30, tzinfo=self.market_tz)
            answer_close = datetime(year=2022, month=2, day=14, hour=16, minute=0, tzinfo=self.market_tz)
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
