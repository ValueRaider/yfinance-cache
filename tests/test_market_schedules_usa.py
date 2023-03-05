import unittest

from .context import yfc_time as yfct

import pandas as pd

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

# 2022 calendar:
# X* = day X is USA public holiday that closed NYSE
#  -- February --
#  Mo   Tu   We   Th   Fr   Sa   Su
#  7    8    9    10   11   12   13
#  14   15   16   17   18   19   20
#  21*  22   23   24   25   26   27
#  28


class Test_Market_Schedules_USA(unittest.TestCase):

    def setUp(self):
        self.exchange = "NMS"
        self.tz = 'America/New_York'
        self.market_tz = ZoneInfo(self.tz)
        yfct.SetExchangeTzName(self.exchange, self.tz)

        self.market_open = time(9, 30)
        self.market_close = time(16)

        self.td7d = timedelta(days=7)

    def test_ExchangeOpenOnDay(self):
        for d in range(14, 19):  # Weekdays
            dt = date(2022, 2, d)
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
        for d in [19, 20]:  # weekend
            dt = date(2022, 2, d)
            response = yfct.ExchangeOpenOnDay(self.exchange, dt)
            answer = False
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise

        dt = date(2022, 11, 24)  # Thanksgiving Day
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
        self.assertEqual(tz2, "America/New_York")

    def test_GetExchangeWeekSchedule(self):
        args = {"exchange": self.exchange}

        # Test simple case - 3 full weeks
        args["start"] = date(2022, 2, 7)  # Monday
        args["end"] = date(2022, 2, 28)  # Monday + 3 weeks
        week_starts = [datetime.combine(date(2022, 2, d), time(0), self.market_tz) for d in [7, 14, 21]]  # Mondays
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 2, d), self.market_open, self.market_tz) for d in [7, 14, 22]]  # Monday opens - 21st holiday
        closes = [datetime.combine(date(2022, 2, d), self.market_close, self.market_tz) for d in [11, 18, 25]]  # Friday closes
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
        args["start"] = date(2022, 2, 9)  # Wednesday
        args["end"] = date(2022, 2, 21)  # next next Monday
        week_starts = [datetime.combine(date(2022, 2, 14), time(0), self.market_tz)]  # next Monday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 2, 14), self.market_open, self.market_tz)]  # Monday open
        closes = [datetime.combine(date(2022, 2, 18), self.market_close, self.market_tz)]  # Friday close
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
        args["start"] = date(2022, 2, 7)  # Monday
        args["end"] = date(2022, 2, 14)  # next Wednesday
        week_starts = [datetime.combine(date(2022, 2, 7), time(0), self.market_tz)]  # Monday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 2, 7), self.market_open, self.market_tz)]  # Monday open
        closes = [datetime.combine(date(2022, 2, 11), self.market_close, self.market_tz)]  # Friday close
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
        args["start"] = date(2022, 2, 7)  # Monday
        args["end"] = date(2022, 2, 13)  # Sunday
        args["ignoreWeekends"] = True
        week_starts = [datetime.combine(date(2022, 2, 7), time(0), self.market_tz)]  # Monday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 2, 7), self.market_open, self.market_tz)]  # Monday open
        closes = [datetime.combine(date(2022, 2, 11), self.market_close, self.market_tz)]  # Friday close
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
        args["start"] = date(2022, 2, 22)  # Monday 21st holiday
        args["end"] = date(2022, 2, 28)  # Monday
        args["ignoreHolidays"] = True
        week_starts = [datetime.combine(date(2022, 2, 21), time(0), self.market_tz)]  # Monday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 2, 22), self.market_open, self.market_tz)]  # Tuesday open - Monday is holiday
        closes = [datetime.combine(date(2022, 2, 25), self.market_close, self.market_tz)]  # Friday close
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
        # Can't test because doesn't happen in February

    def test_IsTimestampInActiveSession_marketTz(self):
        hours = [] ; answers = []
        hours.append(9) ; answers.append(False)
        hours.append(10) ; answers.append(True)
        hours.append(16) ; answers.append(False)
        hours.append(17) ; answers.append(False)

        for d in range(14, 19):
            for i in range(len(hours)):
                h = hours[i]
                answer = answers[i]

                dt = datetime.combine(date(2022, 2, d), time(h), self.market_tz)
                response = yfct.IsTimestampInActiveSession(self.exchange, dt)
                try:
                    self.assertEqual(response, answer)
                except:
                    print("dt = {0}".format(dt))
                    print("response = {0}".format(response))
                    print("answer = {0}".format(answer))
                    raise

        # Weekend:
        for d in [19, 20]:
            dt = datetime.combine(date(2022, 2, d), time(11), self.market_tz)
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
        hours.append(14) ; answers.append(False)  # Before open
        hours.append(15) ; answers.append(True)  # During session
        hours.append(21) ; answers.append(False)  # Right on close
        hours.append(22) ; answers.append(False)  # After close

        utc_tz = ZoneInfo("UTC")

        for d in range(14, 19):
            for i in range(len(hours)):
                h = hours[i]
                answer = answers[i]

                dt = datetime.combine(date(2022, 2, d), time(h), utc_tz)
                response = yfct.IsTimestampInActiveSession(self.exchange, dt)
                try:
                    self.assertEqual(response, answer)
                except:
                    print("dt = {0}".format(dt))
                    print("response = {0}".format(response))
                    print("answer = {0}".format(answer))
                    raise

        # Weekend:
        for d in [19, 20]:
            dt = datetime.combine(date(2022, 2, d), time(10), utc_tz)
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
        for d in range(14, 19):
            d = date(2022, 2, d)

            # During session
            dt = datetime.combine(d, time(10), self.market_tz)
            response = yfct.GetTimestampCurrentSession(self.exchange, dt)
            answer_open = datetime.combine(d, time(9, 30), self.market_tz)
            answer_close = datetime.combine(d, time(16, 0), self.market_tz)
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
        for d in range(14, 19):
            if d == 14:
                last_d = 11
            else:
                last_d = d-1
            d = date(2022, 2, d)
            last_d = date(2022, 2, last_d)

            # Before open
            dt = datetime.combine(d, time(7), self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open = datetime.combine(last_d, time(9, 30), self.market_tz)
            answer_close = datetime.combine(last_d, time(16, 0), self.market_tz)
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
            dt = datetime.combine(d, time(10), self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open = datetime.combine(d, time(9, 30), self.market_tz)
            answer_close = datetime.combine(d, time(16, 0), self.market_tz)
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
            answer_open = datetime.combine(d, time(9, 30), self.market_tz)
            answer_close = datetime.combine(d, time(16, 0), self.market_tz)
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
        for d in [19, 20]:
            dt = datetime.combine(date(2022, 2, d), time(10), self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open = datetime.combine(date(2022, 2, 18), time(9, 30), self.market_tz)
            answer_close = datetime.combine(date(2022, 2, 18), time(16, 0), self.market_tz)
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
        for d in range(14, 19):
            if d == 18:
                next_d = 22
            else:
                next_d = d+1
            d = date(2022, 2, d)
            next_d = date(2022, 2, next_d)

            # Before open
            dt = datetime.combine(d, time(7), self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open = datetime.combine(d, time(9, 30), self.market_tz)
            answer_close = datetime.combine(d, time(16, 0), self.market_tz)
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
            dt = datetime.combine(d, time(10), self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open = datetime.combine(next_d, time(9, 30), self.market_tz)
            answer_close = datetime.combine(next_d, time(16, 0), self.market_tz)
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
            answer_open = datetime.combine(next_d, time(9, 30), self.market_tz)
            answer_close = datetime.combine(next_d, time(16, 0), self.market_tz)
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
            dt = datetime.combine(date(2022, 2, 13), time(20), self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            # Monday 17th is holiday
            answer_open = datetime.combine(date(2022, 2, 14), time(9, 30), self.market_tz)
            answer_close = datetime.combine(date(2022, 2, 14), time(16, 0), self.market_tz)
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
