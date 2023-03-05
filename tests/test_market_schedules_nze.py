import unittest

from .context import yfc_time as yfct

import pandas as pd

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

# 2022 calendar:
# X* = day X is public holiday that closed exchange
#  -- April --
#  Mo   Tu   We   Th   Fr   Sa   Su
#  4    5    6    7    8    9    10
#  11   12   13   14   15*  16   17
#  18*  19   20   21   22   23   24
#  25*


class Test_Market_Schedules_NZE(unittest.TestCase):

    def setUp(self):
        self.exchange = "NZE"
        self.tz = 'Pacific/Auckland'
        self.market_tz = ZoneInfo(self.tz)
        yfct.SetExchangeTzName(self.exchange, self.tz)

        self.market_open = time(10)
        self.market_close = time(16, 45)

        self.td7d = timedelta(days=7)

    def test_ExchangeOpenOnDay(self):
        for d in range(4, 9):  # Weekdays
            dt = date(2022, 4, d)
            response = yfct.ExchangeOpenOnDay(self.exchange, dt)
            answer = True
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise

        # Weekend
        for d in [9, 10]:  # weekend
            dt = date(2022, 4, d)
            response = yfct.ExchangeOpenOnDay(self.exchange, dt)
            answer = False
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise

        dt = date(2022, 4, 18)  # Public holiday
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

    def test_GetExchangeWeekSchedule(self):
        args = {"exchange": self.exchange}

        # Test simple case - 3 full weeks
        args["start"] = date(2022, 4, 4)  # Monday
        args["end"] = date(2022, 4, 25)  # Monday + 3 weeks
        week_starts = [datetime.combine(date(2022, 4, d), time(0), self.market_tz) for d in [4, 11, 18]]  # Mondays
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 4, d), self.market_open, self.market_tz) for d in [4, 11, 19]]  # Monday opens - 18th holiday
        closes = [datetime.combine(date(2022, 4, d), self.market_close, self.market_tz) for d in [8, 14, 22]]  # Friday closes - 15th holiday
        answer = pd.DataFrame(data={"open": opens, "close": closes}, index=pd.IntervalIndex.from_arrays(week_starts, week_ends, closed="left"))
        for ignoreHolidays in [False, True]:
            for ignoreWeekends in [False, True]:
                args["ignoreHolidays"] = ignoreHolidays
                args["ignoreWeekends"] = ignoreWeekends
                response = yfct.GetExchangeWeekSchedule(**args)
                try:
                    self.assertTrue(response.index.equals(answer.index))
                except:
                    print("- response.index:") ; print(response.index)
                    print("- answer.index:") ; print(answer.index)
                    raise
                try:
                    self.assertTrue(response.equals(answer))
                except:
                    print("- response:") ; print(response)
                    print("- answer:") ; print(answer)
                    raise

        # Test mid-week behaviour
        args["start"] = date(2022, 4, 6)  # Wednesday
        args["end"] = date(2022, 4, 18)  # next next Monday
        week_starts = [datetime.combine(date(2022, 4, 11), time(0), self.market_tz)]  # next Monday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 4, 11), self.market_open, self.market_tz)]  # Monday open
        closes = [datetime.combine(date(2022, 4, 14), self.market_close, self.market_tz)]  # Thursday close (Friday holiday)
        answer = pd.DataFrame(data={"open": opens, "close": closes}, index=pd.IntervalIndex.from_arrays(week_starts, week_ends, closed="left"))
        for ignoreWeekends in [False, True]:
            for ignoreHolidays in [False, True]:
                args["ignoreWeekends"] = ignoreWeekends
                args["ignoreHolidays"] = ignoreHolidays
                response = yfct.GetExchangeWeekSchedule(**args)
                try:
                    self.assertTrue(response.index.equals(answer.index))
                except:
                    print("- response.index:") ; print(response.index)
                    print("- answer.index:") ; print(answer.index)
                    raise
                try:
                    self.assertTrue(response.equals(answer))
                except:
                    print("- response:") ; print(response)
                    print("- answer:") ; print(answer)
                    raise
        args["start"] = date(2022, 4, 4)  # Monday
        args["end"] = date(2022, 4, 13)  # next Wednesday
        week_starts = [datetime.combine(date(2022, 4, 4), time(0), self.market_tz)]  # Monday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 4, 4), self.market_open, self.market_tz)]  # Monday open
        closes = [datetime.combine(date(2022, 4, 8), self.market_close, self.market_tz)]  # Friday close
        answer = pd.DataFrame(data={"open": opens, "close": closes}, index=pd.IntervalIndex.from_arrays(week_starts, week_ends, closed="left"))
        for ignoreWeekends in [False, True]:
            for ignoreHolidays in [False, True]:
                args["ignoreWeekends"] = ignoreWeekends
                args["ignoreHolidays"] = ignoreHolidays
                response = yfct.GetExchangeWeekSchedule(**args)
                try:
                    self.assertTrue(response.index.equals(answer.index))
                except:
                    print("- response.index:") ; print(response.index)
                    print("- answer.index:") ; print(answer.index)
                    raise
                try:
                    self.assertTrue(response.equals(answer))
                except:
                    print("- response:") ; print(response)
                    print("- answer:") ; print(answer)
                    raise

        # Test weekend behaviour
        args["start"] = date(2022, 4, 4)  # Monday
        args["end"] = date(2022, 4, 10)  # Sunday
        args["ignoreWeekends"] = True
        week_starts = [datetime.combine(date(2022, 4, 4), time(0), self.market_tz)]  # Monday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 4, 4), self.market_open, self.market_tz)]  # Monday open
        closes = [datetime.combine(date(2022, 4, 8), self.market_close, self.market_tz)]  # Friday close
        answer = pd.DataFrame(data={"open": opens, "close": closes}, index=pd.IntervalIndex.from_arrays(week_starts, week_ends, closed="left"))
        for ignoreHolidays in [False, True]:
            args["ignoreHolidays"] = ignoreHolidays
            response = yfct.GetExchangeWeekSchedule(**args)
            try:
                self.assertTrue(response.index.equals(answer.index))
            except:
                print("- response.index:") ; print(response.index)
                print("- answer.index:") ; print(answer.index)
                raise
            try:
                self.assertTrue(response.equals(answer))
            except:
                print("- response:") ; print(response)
                print("- answer:") ; print(answer)
                raise
        #
        args["ignoreWeekends"] = False
        answer = None
        for ignoreHolidays in [False, True]:
            args["ignoreHolidays"] = ignoreHolidays
            response = yfct.GetExchangeWeekSchedule(**args)
            try:
                self.assertEqual(response, answer)
            except:
                print("- response:") ; print(response)
                print("- answer:") ; print(answer)
                raise

        # Test Monday holiday behaviour
        args["start"] = date(2022, 4, 19)  # Monday 18th holiday
        args["end"] = date(2022, 4, 25)  # Monday
        args["ignoreHolidays"] = True
        week_starts = [datetime.combine(date(2022, 4, 18), time(0), self.market_tz)]  # Monday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 4, 19), self.market_open, self.market_tz)]  # Tuesday open - Monday is holiday
        closes = [datetime.combine(date(2022, 4, 22), self.market_close, self.market_tz)]  # Friday close
        answer = pd.DataFrame(data={"open": opens, "close": closes}, index=pd.IntervalIndex.from_arrays(week_starts, week_ends, closed="left"))
        for ignoreWeekends in [False, True]:
            args["ignoreWeekends"] = ignoreWeekends
            response = yfct.GetExchangeWeekSchedule(**args)
            try:
                self.assertTrue(response.index.equals(answer.index))
            except:
                print("- response.index:") ; print(response.index)
                print("- answer.index:") ; print(answer.index)
                raise
            try:
                self.assertTrue(response.equals(answer))
            except:
                print("- response:") ; print(response)
                print("- answer:") ; print(answer)
                raise
        #
        args["ignoreHolidays"] = False
        answer = None
        for ignoreWeekends in [False, True]:
            args["ignoreWeekends"] = ignoreWeekends
            response = yfct.GetExchangeWeekSchedule(**args)
            try:
                self.assertEqual(response, answer)
            except:
                print("- response:") ; print(response)
                print("- answer:") ; print(answer)
                raise

        # Test weekend + Friday holiday behaviour
        args["start"] = date(2022, 4, 11)  # Monday
        args["end"] = date(2022, 4, 15)  # Friday holiday
        args["ignoreWeekends"] = True
        args["ignoreHolidays"] = True
        week_starts = [datetime.combine(date(2022, 4, 11), time(0), self.market_tz)]  # Monday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 4, 11), self.market_open, self.market_tz)]  # Monday open
        closes = [datetime.combine(date(2022, 4, 14), self.market_close, self.market_tz)]  # Thursday close - Friday is holiday
        answer = pd.DataFrame(data={"open": opens, "close": closes}, index=pd.IntervalIndex.from_arrays(week_starts, week_ends, closed="left"))
        response = yfct.GetExchangeWeekSchedule(**args)
        try:
            self.assertTrue(response.index.equals(answer.index))
        except:
            print("- response.index:") ; print(response.index)
            print("- answer.index:") ; print(answer.index)
            raise
        try:
            self.assertTrue(response.equals(answer))
        except:
            print("- response:") ; print(response)
            print("- answer:") ; print(answer)
            raise
        args["ignoreHolidays"] = False
        answer = None
        response = yfct.GetExchangeWeekSchedule(**args)
        try:
            self.assertEqual(response, answer)
        except:
            print("- response:") ; print(response)
            print("- answer:") ; print(answer)
            raise

    def test_IsTimestampInActiveSession_marketTz(self):
        times = [] ; answers = []
        times.append(time(8)) ; answers.append(False)
        times.append(self.market_open) ; answers.append(True)
        times.append(self.market_close) ; answers.append(False)
        times.append(time(18)) ; answers.append(False)

        for d in range(4, 9):
            for i in range(len(times)):
                t = times[i]
                answer = answers[i]

                dt = datetime.combine(date(2022, 4, d), t, self.market_tz)
                response = yfct.IsTimestampInActiveSession(self.exchange, dt)
                try:
                    self.assertEqual(response, answer)
                except:
                    print("dt = {0}".format(dt))
                    print("response = {0}".format(response))
                    print("answer = {0}".format(answer))
                    raise

        # Weekend:
        for d in [9, 10]:
            dt = datetime.combine(date(2022, 4, d), time(12), self.market_tz)
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
        times = []
        times.append(time(8))
        times.append(self.market_open)
        times.append(self.market_close)
        times.append(time(18))
        answer = False

        utc_tz = ZoneInfo("UTC")

        for d in [4, 5, 6, 7, 8]:
            for i in range(len(times)):
                t = times[i]

                dt = datetime.combine(date(2022, 4, d), t, utc_tz)
                response = yfct.IsTimestampInActiveSession(self.exchange, dt)
                try:
                    self.assertEqual(response, answer)
                except:
                    print("dt = {0}".format(dt))
                    print("response = {0}".format(response))
                    print("answer = {0}".format(answer))
                    raise

        # Weekend:
        for d in [9, 10]:
            dt = datetime.combine(date(2022, 4, d), time(12), utc_tz)
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
        for d in range(4, 9):
            d = date(2022, 4, d)

            # During session
            dt = datetime.combine(d, self.market_open, self.market_tz)
            response = yfct.GetTimestampCurrentSession(self.exchange, dt)
            answer_open = datetime.combine(d, self.market_open, self.market_tz)
            answer_close = datetime.combine(d, self.market_close, self.market_tz)
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
            dt = datetime.combine(d, time(8), self.market_tz)
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
            dt = datetime.combine(d, time(20), self.market_tz)
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
        for d in range(4, 9):
            if d == 4:
                last_d = 1
            else:
                last_d = d-1
            d = date(2022, 4, d)
            last_d = date(2022, 4, last_d)

            # Before open
            dt = datetime.combine(d, time(7), self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open = datetime.combine(last_d, self.market_open, self.market_tz)
            answer_close = datetime.combine(last_d, self.market_close, self.market_tz)
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
            dt = datetime.combine(d, self.market_open, self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open = datetime.combine(d, self.market_open, self.market_tz)
            answer_close = datetime.combine(d, self.market_close, self.market_tz)
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
            dt = datetime.combine(d, time(20), self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open = datetime.combine(d, self.market_open, self.market_tz)
            answer_close = datetime.combine(d, self.market_close, self.market_tz)
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
        for d in [9, 10]:
            dt = datetime.combine(date(2022, 4, d), self.market_open, self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open = datetime.combine(date(2022, 4, 8), self.market_open, self.market_tz)
            answer_close = datetime.combine(date(2022, 4, 8), self.market_close, self.market_tz)
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
        for d in range(4, 9):
            if d == 8:
                next_d = 11
            else:
                next_d = d+1
            d = date(2022, 4, d)
            next_d = date(2022, 4, next_d)

            # Before open
            dt = datetime.combine(d, time(7), self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open = datetime.combine(d, self.market_open, self.market_tz)
            answer_close = datetime.combine(d, self.market_close, self.market_tz)
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
            dt = datetime.combine(d, self.market_open, self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open = datetime.combine(next_d, self.market_open, self.market_tz)
            answer_close = datetime.combine(next_d, self.market_close, self.market_tz)
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
            dt = datetime.combine(d, time(20), self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open = datetime.combine(next_d, self.market_open, self.market_tz)
            answer_close = datetime.combine(next_d, self.market_close, self.market_tz)
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
        for d in [16, 17]:
            dt = datetime.combine(date(2022, 4, d), time(20), self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            # Monday 18th is holiday
            answer_open = datetime.combine(date(2022, 4, 19), self.market_open, self.market_tz)
            answer_close = datetime.combine(date(2022, 4, 19), self.market_close, self.market_tz)
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
