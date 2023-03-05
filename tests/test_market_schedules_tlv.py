import unittest

from .context import yfc_time as yfct
from .context import yfc_dat as yfcd

import pandas as pd

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

# 2022 calendar:
# X* = public holiday
#  -- March --
#  Mo   Tu   We   Th   Fr   Sa   Su
#  -    1    2    3    4    5    6
#  7    8    9    10   11   12   13
#  14   15   16   17*  18*  19   20
#  21   22   23   24   25   26   27
#  28   29   30   31
#  -- September --
#  Mo   Tu   We   Th   Fr   Sa   Su
#  -    -    -    1    2    3    4
#  5    6    7    8    9    10   11
#  12   13   14   15   16   17   18
#  19   20   21   22   23   24   25*
#  26*  27*  28   29   30
#
#  -- March --
#  Su   Mo   Tu   We   Th   Fr   Sa
#  -    -    1    2    3    4    5
#  6    7    8    9    10   11   12
#  13   14   15   16   17*  18*  19
#  20   21   22   23   24   25   26
#  27   28   29   30   31
#  -- September --
#  Su   Mo   Tu   We   Th   Fr   Sa
#  -    -    -    -    1    2    3
#  4    5    6    7    8    9    10
#  11   12   13   14   15   16   17
#  18   19   20   21   22   23   24
#  25*  26*  27*  28   29   30


class Test_Market_Schedules_TLV(unittest.TestCase):

    def setUp(self):
        self.exchange = "TLV"
        self.tz = 'Asia/Jerusalem'
        self.market_tz = ZoneInfo(self.tz)
        yfct.SetExchangeTzName(self.exchange, self.tz)

        self.market_open = time(9, 59)
        self.market_close = time(17, 26)
        self.market_close_sunday = time(15, 51)

        self.td7d = timedelta(days=7)

    def test_ExchangeOpenOnDay(self):
        # Weekdays
        for d in [6, 7, 8, 9, 10]:
            dt = date(2022, 3, d)
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
        for d in [11, 12]:
            dt = date(2022, 3, d)
            response = yfct.ExchangeOpenOnDay(self.exchange, dt)
            answer = False
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise

        # Purim
        dt = date(2022, 3, 17)
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

    def test_GetExchangeWeekSchedule(self):
        args = {"exchange": self.exchange}

        # Test simple case - 3 full weeks
        args["start"] = date(2022, 3, 7)  # Monday
        args["end"] = date(2022, 3, 28)  # Monday + 3 weeks
        week_starts = [datetime.combine(date(2022, 3, d), time(0), self.market_tz) for d in [7, 14, 21]]  # Mondays
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 3, d), self.market_open, self.market_tz) for d in [7, 14, 21]]  # Monday opens
        closes = [datetime.combine(date(2022, 3, d), self.market_close_sunday, self.market_tz) for d in [13, 20, 27]]  # Sunday closes
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
        args["start"] = date(2022, 3, 9)  # Wednesday
        args["end"] = date(2022, 3, 21)  # next next Monday
        week_starts = [datetime.combine(date(2022, 3, 14), time(0), self.market_tz)]  # next Monday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 3, 14), self.market_open, self.market_tz)]  # Monday open
        closes = [datetime.combine(date(2022, 3, 20), self.market_close_sunday, self.market_tz)]  # Sunday close
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
        args["start"] = date(2022, 3, 7)  # Monday
        args["end"] = date(2022, 3, 16)  # next Wednesday
        week_starts = [datetime.combine(date(2022, 3, 7), time(0), self.market_tz)]  # Monday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 3, 7), self.market_open, self.market_tz)]  # Monday open
        closes = [datetime.combine(date(2022, 3, 13), self.market_close_sunday, self.market_tz)]  # Sunday close
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

        # Test Monday holiday behaviour
        args["start"] = date(2022, 9, 27)  # Monday-Tuesday 25th-26th holidays
        args["end"] = date(2022, 10, 3)  # Monday
        args["ignoreHolidays"] = True
        week_starts = [datetime.combine(date(2022, 9, 26), time(0), self.market_tz)]  # Monday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 9, 28), self.market_open, self.market_tz)]  # Monday open
        closes = [datetime.combine(date(2022, 10, 2), self.market_close_sunday, self.market_tz)]  # Sunday close
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

    def test_GetExchangeWeekSchedule_startSunday(self):
        args = {"exchange": self.exchange, "forceStartMonday": False}

        # Test simple case - 3 full weeks
        args["start"] = date(2022, 3, 6)  # Sunday
        args["end"] = date(2022, 3, 27)  # Sunday + 3 weeks
        week_starts = [datetime.combine(date(2022, 3, d), time(0), self.market_tz) for d in [6, 13, 20]]  # Sundays
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 3, d), self.market_open, self.market_tz) for d in [6, 13, 20]]  # Sunday opens
        closes = [datetime.combine(date(2022, 3, d), self.market_close, self.market_tz) for d in [10, 16, 24]]  # Thursday closes  - 17th holiday
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
        args["start"] = date(2022, 3, 8)  # Tuesday
        args["end"] = date(2022, 3, 20)  # next next Sunday
        week_starts = [datetime.combine(date(2022, 3, 13), time(0), self.market_tz)]  # next Sunday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 3, 13), self.market_open, self.market_tz)]  # Monday open
        closes = [datetime.combine(date(2022, 3, 16), self.market_close, self.market_tz)]  # Wednesday close (Thursday holiday)
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
        args["start"] = date(2022, 3, 6)  # Sunday
        args["end"] = date(2022, 3, 15)  # next Tuesday
        week_starts = [datetime.combine(date(2022, 3, 6), time(0), self.market_tz)]  # Sunday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 3, 6), self.market_open, self.market_tz)]  # Sunday open
        closes = [datetime.combine(date(2022, 3, 10), self.market_close, self.market_tz)]  # Thursday close
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
        args["start"] = date(2022, 3, 6)  # Sunday
        args["end"] = date(2022, 3, 12)  # Saturday
        args["ignoreWeekends"] = True
        week_starts = [datetime.combine(date(2022, 3, 6), time(0), self.market_tz)]  # Sunday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 3, 6), self.market_open, self.market_tz)]  # Sunday open
        closes = [datetime.combine(date(2022, 3, 10), self.market_close, self.market_tz)]  # Thursday close
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

        # Test Sunday holiday behaviour
        args["start"] = date(2022, 9, 27)  # Sunday-Tuesday 24th-26th holidays
        args["end"] = date(2022, 10, 2)  # next Sunday
        args["ignoreHolidays"] = True
        week_starts = [datetime.combine(date(2022, 9, 25), time(0), self.market_tz)]  # Sunday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 9, 28), self.market_open, self.market_tz)]  # Wednesday open
        closes = [datetime.combine(date(2022, 9, 29), self.market_close, self.market_tz)]  # Thursday close
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
        args["start"] = date(2022, 3, 13)  # Sunday
        args["end"] = date(2022, 3, 17)  # Thursday-Friday holidays
        args["ignoreWeekends"] = True
        args["ignoreHolidays"] = True
        week_starts = [datetime.combine(date(2022, 3, 13), time(0), self.market_tz)]  # Sunday
        week_ends = [d + self.td7d for d in week_starts]
        opens = [datetime.combine(date(2022, 3, 13), self.market_open, self.market_tz)]  # Sunday open
        closes = [datetime.combine(date(2022, 3, 16), self.market_close, self.market_tz)]  # Wednesday close
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
        times.append(time(9, 58)) ; answers.append(False)
        times.append(time(9, 59)) ; answers.append(True)
        times.append(time(17, 25)) ; answers.append(True)
        times.append(time(17, 26)) ; answers.append(False)
        for d in [7, 8, 9, 10]:
            for i in range(len(times)):
                t = times[i]
                answer = answers[i]

                dt = datetime.combine(date(2022, 3, d), t, self.market_tz)
                response = yfct.IsTimestampInActiveSession(self.exchange, dt)
                try:
                    self.assertEqual(response, answer)
                except:
                    print("dt = {0}".format(dt))
                    print("response = {0}".format(response))
                    print("answer = {0}".format(answer))
                    raise

        # Sunday closes early:
        d = 6
        times = [] ; answers = []
        times.append(time(9, 58)) ; answers.append(False)
        times.append(time(9, 59)) ; answers.append(True)
        times.append(time(15, 49)) ; answers.append(True)
        times.append(time(15, 51)) ; answers.append(False)
        for i in range(len(times)):
            t = times[i]
            answer = answers[i]

            dt = datetime.combine(date(2022, 3, d), t, self.market_tz)
            response = yfct.IsTimestampInActiveSession(self.exchange, dt)
            try:
                self.assertEqual(response, answer)
            except:
                print("dt = {0}".format(dt))
                print("response = {0}".format(response))
                print("answer = {0}".format(answer))
                raise

        # Weekend:
        for d in [11, 12]:
            dt = datetime.combine(date(2022, 3, d), time(11), self.market_tz)
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
        for d in [6, 7, 8, 9, 10]:
            # During session
            dt = datetime.combine(date(2022, 3, d), time(11), self.market_tz)
            response = yfct.GetTimestampCurrentSession(self.exchange, dt)
            answer_open = datetime.combine(date(2022, 3, d), self.market_open, self.market_tz)
            if d == 6:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close_sunday, self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close, self.market_tz)
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
            dt = datetime.combine(date(2022, 3, d), time(8), self.market_tz)
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
            dt = datetime.combine(date(2022, 3, d), time(20), self.market_tz)
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
        for d in [6, 7, 8, 9, 10]:
            if d == 6:
                last_d = 3
            else:
                last_d = d-1

            # Before open
            dt = datetime.combine(date(2022, 3, d), time(7), self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open = datetime.combine(date(2022, 3, last_d), self.market_open, self.market_tz)
            if d == 7:
                answer_close = datetime.combine(date(2022, 3, last_d), self.market_close_sunday, self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, last_d), self.market_close, self.market_tz)
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
            dt = datetime.combine(date(2022, 3, d), time(10), self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open = datetime.combine(date(2022, 3, d), self.market_open, self.market_tz)
            if d == 6:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close_sunday, self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close, self.market_tz)
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
            dt = datetime.combine(date(2022, 3, d), time(20), self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open = datetime.combine(date(2022, 3, d), self.market_open, self.market_tz)
            if d == 6:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close_sunday, self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close, self.market_tz)
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
        for d in [11, 12]:
            dt = datetime.combine(date(2022, 3, d), time(10), self.market_tz)
            response = yfct.GetTimestampMostRecentSession(self.exchange, dt)
            answer_open = datetime.combine(date(2022, 3, 10), self.market_open, self.market_tz)
            answer_close = datetime.combine(date(2022, 3, 10), self.market_close, self.market_tz)
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
        for d in [6, 7, 8, 9, 10]:
            # Before open
            dt = datetime.combine(date(2022, 3, d), time(7), self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open = datetime.combine(date(2022, 3, d), self.market_open, self.market_tz)
            if d == 6:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close_sunday, self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, d), self.market_close, self.market_tz)
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

            if d == 10:
                next_d = 13
            else:
                next_d = d + 1

            # During session
            dt = datetime.combine(date(2022, 3, d), time(10), self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open = datetime.combine(date(2022, 3, next_d), self.market_open, self.market_tz)
            if next_d == 13:
                answer_close = datetime.combine(date(2022, 3, next_d), self.market_close_sunday, self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, next_d), self.market_close, self.market_tz)
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
            dt = datetime.combine(date(2022, 3, d), time(20), self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            if next_d == 13:
                answer_close = datetime.combine(date(2022, 3, next_d), self.market_close_sunday, self.market_tz)
            else:
                answer_close = datetime.combine(date(2022, 3, next_d), self.market_close, self.market_tz)
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
            dt = datetime.combine(date(2022, 3, 11), time(15), self.market_tz)
            response = yfct.GetTimestampNextSession(self.exchange, dt)
            answer_open = datetime.combine(date(2022, 3, 13), self.market_open, self.market_tz)
            answer_close = datetime.combine(date(2022, 3, 13), self.market_close_sunday, self.market_tz)
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
