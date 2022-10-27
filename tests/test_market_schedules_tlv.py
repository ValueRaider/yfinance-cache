import unittest
from pprint import pprint

# import sys ; sys.path.insert(0, "/home/gonzo/ReposForks/exchange_calendars.dev")

from .context import yfc_time as yfct
from .context import yfc_dat as yfcd

from datetime import datetime,date,time,timedelta
from zoneinfo import ZoneInfo

## 2022 calendar:
## X* = public holiday
##  -- March --
##  Su   Mo   Tu   We   Th   Fr   Sa
##  -    -    1    2    3    4    5
##  6    7    8    9    10   11   12
##  13   14   15   16   17*  18*  19
##  20   21   22   23   24   25   26
##  27   28   29   30   31

class Test_Market_Schedules_TLV(unittest.TestCase):

    def setUp(self):
        self.exchange = "TLV"
        self.tz = 'Asia/Jerusalem'
        self.market_tz = ZoneInfo(self.tz)
        yfct.SetExchangeTzName(self.exchange, self.tz)

        self.market_open = time(9,59)
        self.market_close = time(17,26)
        self.market_close_sunday = time(15,51)


    def test_ExchangeOpenOnDay(self):
        # Weekdays
        for d in [6, 7, 8, 9, 10]:
            dt = date(year=2022, month=3, day=d)
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
        for d in [11, 12]:
            dt = date(year=2022, month=3, day=d)
            response = yfct.ExchangeOpenOnDay(self.exchange, dt)
            answer = False
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise

        ## Purim
        dt = date(year=2022, month=3, day=17)
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
        tz2 = yfct.GetExchangeTzName("TLV")
        self.assertEqual(tz2, "Asia/Jerusalem")


    def test_IsTimestampInActiveSession_marketTz(self):
        times = [] ; answers = []
        times.append(time(9,58)) ; answers.append(False)
        times.append(time(9,59)) ; answers.append(True)
        times.append(time(17,25)) ; answers.append(True)
        times.append(time(17,26)) ; answers.append(False)
        for d in [7,8,9,10]:
            for i in range(len(times)):
                t = times[i]
                answer = answers[i]

                dt = datetime.combine(date(2022,3,d), t, tzinfo=self.market_tz)
                response = yfct.IsTimestampInActiveSession(self.exchange, dt)
                try:
                    self.assertEqual(response, answer)
                except:
                    print("dt = {0}".format(dt))
                    print("response = {0}".format(response))
                    print("answer = {0}".format(answer))
                    raise

        # Sunday closes early:
        d=6
        times = [] ; answers = []
        times.append(time(9,58)) ; answers.append(False)
        times.append(time(9,59)) ; answers.append(True)
        times.append(time(15,49)) ; answers.append(True)
        times.append(time(15,51)) ; answers.append(False)
        for i in range(len(times)):
            t = times[i]
            answer = answers[i]

            dt = datetime.combine(date(2022,3,d), t, tzinfo=self.market_tz)
            response = yfct.IsTimestampInActiveSession(self.exchange, dt)
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise

        # Weekend:
        for d in [11,12]:
            dt = datetime(year=2022, month=3, day=d, hour=11, tzinfo=self.market_tz)
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
        for d in [6,7,8,9,10]:
            # During session
            dt = datetime(year=2022, month=3, day=d, hour=11, tzinfo=self.market_tz)
            response = yfct.GetTimestampCurrentSession(self.exchange, dt)
            answer_open  = datetime.combine(date(2022, 3, d), self.market_open, tzinfo=self.market_tz)
            if d==6:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close_sunday, tzinfo=self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close, tzinfo=self.market_tz)
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
            dt = datetime(year=2022, month=3, day=d, hour=8, tzinfo=self.market_tz)
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
            dt = datetime(year=2022, month=3, day=d, hour=20, tzinfo=self.market_tz)
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
        for d in [6,7,8,9,10]:
            if d==6:
                last_d = 3
            else:
                last_d = d-1

            # Before open
            dt = datetime(year=2022, month=3, day=d, hour=7, tzinfo=self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open  = datetime.combine(date(2022, 3, last_d), self.market_open, tzinfo=self.market_tz)
            if d==7:
                answer_close = datetime.combine(date(2022, 3, last_d), self.market_close_sunday, tzinfo=self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, last_d), self.market_close, tzinfo=self.market_tz)
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
            dt = datetime(year=2022, month=3, day=d, hour=10, tzinfo=self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open = datetime.combine(date(2022, 3, d), self.market_open, tzinfo=self.market_tz)
            if d==6:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close_sunday, tzinfo=self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close, tzinfo=self.market_tz)
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
            dt = datetime(year=2022, month=3, day=d, hour=20, tzinfo=self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open  = datetime.combine(date(2022, 3, d), self.market_open, tzinfo=self.market_tz)
            if d==6:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close_sunday, tzinfo=self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close, tzinfo=self.market_tz)
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
        for d in [11,12]:
            dt = datetime(year=2022, month=3, day=d, hour=10, tzinfo=self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open  = datetime.combine(date(2022, 3, 10), self.market_open, tzinfo=self.market_tz)
            answer_close = datetime.combine(date(2022, 3, 10), self.market_close, tzinfo=self.market_tz)
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
        for d in [6,7,8,9,10]:
            # Before open
            dt = datetime(year=2022, month=3, day=d, hour=7, tzinfo=self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open  = datetime.combine(date(2022, 3, d), self.market_open, tzinfo=self.market_tz)
            if d==6:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close_sunday, tzinfo=self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close, tzinfo=self.market_tz)
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

            if d==10:
                next_d = 13
            else:
                next_d = d+1

            # During session
            dt = datetime(year=2022, month=3, day=d, hour=10, tzinfo=self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open  = datetime.combine(date(2022, 3, next_d), self.market_open, tzinfo=self.market_tz)
            if next_d==13:
                answer_close = datetime.combine(date(2022, 3, next_d), self.market_close_sunday, tzinfo=self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, next_d), self.market_close, tzinfo=self.market_tz)
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
            dt = datetime(year=2022, month=3, day=d, hour=20, tzinfo=self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            if next_d==13:
                answer_close = datetime.combine(date(2022, 3, next_d), self.market_close_sunday, tzinfo=self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, next_d), self.market_close, tzinfo=self.market_tz)
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
            dt = datetime(year=2022, month=3, day=11, hour=15, tzinfo=self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open  = datetime.combine(date(2022, 3, 13), self.market_open, tzinfo=self.market_tz)
            answer_close = datetime.combine(date(2022, 3, 13), self.market_close_sunday, tzinfo=self.market_tz)
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

    # # Run tests sequentially:
    # import inspect
    # test_src = inspect.getsource(Test_Market_Schedules_TLV)
    # unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: (
    #     test_src.index(f"def {x}") - test_src.index(f"def {y}")
    # )
    # unittest.main(verbosity=2)
