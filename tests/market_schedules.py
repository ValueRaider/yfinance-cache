import unittest

from .context import yfc_time as yfct

import datetime
from zoneinfo import ZoneInfo

class Test_USMarket_Schedules(unittest.TestCase):

    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')

    def test_ExchangeOpenOnDay(self):
        # Weekdays
        for d in [10, 11, 12, 13, 14]:
            dt = datetime.date(year=2022, month=1, day=d)
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
        for d in [15, 16]:
            dt = datetime.date(year=2022, month=1, day=d)
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
        dt = datetime.date(year=2022, month=11, day=24)
        response = yfct.ExchangeOpenOnDay(self.exchange, dt)
        answer = False
        try:
            self.assertEqual(response, answer)
        except:
            print("dt = {0}".format(dt))
            print("response = {0}".format(response))
            print("answer = {0}".format(answer))
            raise

    def test_IsTimestampInActiveSession_marketTz(self):
        # Before open
        dt = datetime.datetime(year=2022, month=1, day=10, hour=7, tzinfo=self.market_tz)
        response = yfct.IsTimestampInActiveSession(self.exchange, dt)
        answer = False
        try:
            self.assertEqual(response, answer)
        except:
            print("dt = {0}".format(dt))
            print("response = {0}".format(response))
            print("answer = {0}".format(answer))
            raise

        # During session
        dt = datetime.datetime(year=2022, month=1, day=10, hour=10, tzinfo=self.market_tz)
        response = yfct.IsTimestampInActiveSession(self.exchange, dt)
        answer = True
        try:
            self.assertEqual(response, answer)
        except:
            print("dt = {0}".format(dt))
            print("response = {0}".format(response))
            print("answer = {0}".format(answer))
            raise

        # Right on close
        dt = datetime.datetime(year=2022, month=1, day=10, hour=17, tzinfo=self.market_tz)
        response = yfct.IsTimestampInActiveSession(self.exchange, dt)
        answer = False
        try:
            self.assertEqual(response, answer)
        except:
            print("dt = {0}".format(dt))
            print("response = {0}".format(response))
            print("answer = {0}".format(answer))
            raise

        # After close
        dt = datetime.datetime(year=2022, month=1, day=10, hour=22, tzinfo=self.market_tz)
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
        dt = datetime.datetime(year=2022, month=2, day=7, hour=7, tzinfo=self.market_tz)
        response = yfct.IsTimestampInActiveSession(self.exchange, dt)
        answer = False
        try:
            self.assertEqual(response, answer)
        except:
            print("dt = {0}".format(dt))
            print("response = {0}".format(response))
            print("answer = {0}".format(answer))
            raise

    def test_IsTimestampInActiveSession_localTz(self):
        local_tz = yfct.GetSystemTz()
        # Before open
        dt = datetime.datetime(year=2022, month=1, day=10, hour=13, tzinfo=local_tz)
        response = yfct.IsTimestampInActiveSession(self.exchange, dt)
        answer = False
        try:
            self.assertEqual(response, answer)
        except:
            print("dt = {0}".format(dt))
            print("response = {0}".format(response))
            print("answer = {0}".format(answer))
            raise

        # During session
        dt = datetime.datetime(year=2022, month=1, day=10, hour=15, tzinfo=local_tz)
        response = yfct.IsTimestampInActiveSession(self.exchange, dt)
        answer = True
        try:
            self.assertEqual(response, answer)
        except:
            print("dt = {0}".format(dt))
            print("response = {0}".format(response))
            print("answer = {0}".format(answer))
            raise

        # Right on close
        dt = datetime.datetime(year=2022, month=1, day=10, hour=21, tzinfo=local_tz)
        response = yfct.IsTimestampInActiveSession(self.exchange, dt)
        answer = False
        try:
            self.assertEqual(response, answer)
        except:
            print("dt = {0}".format(dt))
            print("response = {0}".format(response))
            print("answer = {0}".format(answer))
            raise

        # After close
        dt = datetime.datetime(year=2022, month=1, day=10, hour=22, tzinfo=local_tz)
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
        dt = datetime.datetime(year=2022, month=2, day=7, hour=10, tzinfo=local_tz)
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
        # During session
        dt = datetime.datetime(year=2022, month=1, day=10, hour=10, tzinfo=self.market_tz)
        response = yfct.GetTimestampCurrentSession(self.exchange, dt)
        answer_open  = datetime.datetime(year=2022, month=1, day=10, hour=9, minute=30, tzinfo=self.market_tz)
        answer_close = datetime.datetime(year=2022, month=1, day=10, hour=16, minute=0, tzinfo=self.market_tz)
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
        dt = datetime.datetime(year=2022, month=1, day=10, hour=8, tzinfo=self.market_tz)
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
        dt = datetime.datetime(year=2022, month=1, day=10, hour=20, tzinfo=self.market_tz)
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
        # Before open
        dt = datetime.datetime(year=2022, month=1, day=10, hour=7, tzinfo=self.market_tz)
        response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
        answer_open  = datetime.datetime(year=2022, month=1, day=7, hour=9, minute=30, tzinfo=self.market_tz)
        answer_close = datetime.datetime(year=2022, month=1, day=7, hour=16, minute=0, tzinfo=self.market_tz)
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
        dt = datetime.datetime(year=2022, month=1, day=10, hour=10, tzinfo=self.market_tz)
        response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
        answer_open  = datetime.datetime(year=2022, month=1, day=10, hour=9, minute=30, tzinfo=self.market_tz)
        answer_close = datetime.datetime(year=2022, month=1, day=10, hour=16, minute=0, tzinfo=self.market_tz)
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
        dt = datetime.datetime(year=2022, month=1, day=10, hour=20, tzinfo=self.market_tz)
        response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
        answer_open  = datetime.datetime(year=2022, month=1, day=10, hour=9, minute=30, tzinfo=self.market_tz)
        answer_close = datetime.datetime(year=2022, month=1, day=10, hour=16, minute=0, tzinfo=self.market_tz)
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
        dt = datetime.datetime(year=2022, month=1, day=15, hour=20, tzinfo=self.market_tz)
        response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
        answer_open  = datetime.datetime(year=2022, month=1, day=14, hour=9, minute=30, tzinfo=self.market_tz)
        answer_close = datetime.datetime(year=2022, month=1, day=14, hour=16, minute=0, tzinfo=self.market_tz)
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
        # Before open
        dt = datetime.datetime(year=2022, month=1, day=10, hour=7, tzinfo=self.market_tz)
        response = yfct.GetTimestampNextSession(self.exchange, dt)
        answer_open  = datetime.datetime(year=2022, month=1, day=10, hour=9, minute=30, tzinfo=self.market_tz)
        answer_close = datetime.datetime(year=2022, month=1, day=10, hour=16, minute=0, tzinfo=self.market_tz)
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
        dt = datetime.datetime(year=2022, month=1, day=10, hour=10, tzinfo=self.market_tz)
        response = yfct.GetTimestampNextSession(self.exchange, dt)
        answer_open  = datetime.datetime(year=2022, month=1, day=11, hour=9, minute=30, tzinfo=self.market_tz)
        answer_close = datetime.datetime(year=2022, month=1, day=11, hour=16, minute=0, tzinfo=self.market_tz)
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
        dt = datetime.datetime(year=2022, month=1, day=10, hour=20, tzinfo=self.market_tz)
        response = yfct.GetTimestampNextSession(self.exchange, dt)
        answer_open  = datetime.datetime(year=2022, month=1, day=11, hour=9, minute=30, tzinfo=self.market_tz)
        answer_close = datetime.datetime(year=2022, month=1, day=11, hour=16, minute=0, tzinfo=self.market_tz)
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
        dt = datetime.datetime(year=2022, month=1, day=15, hour=20, tzinfo=self.market_tz)
        response = yfct.GetTimestampNextSession(self.exchange, dt)
        # Monday 17th is holiday
        answer_open  = datetime.datetime(year=2022, month=1, day=18, hour=9, minute=30, tzinfo=self.market_tz)
        answer_close = datetime.datetime(year=2022, month=1, day=18, hour=16, minute=0, tzinfo=self.market_tz)
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
