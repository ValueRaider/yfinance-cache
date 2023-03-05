import unittest
from pprint import pprint

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

from datetime import datetime, date, time, timedelta
dtc = datetime.combine
from zoneinfo import ZoneInfo

# 2022 calendar:
# X* = public holiday
#  -- March --
#  Su   Mo   Tu   We   Th   Fr   Sa
#  -    -    1    2    3    4    5
#  6    7    8    9    10   11   12
#  13   14   15   16   17*  18*  19
#  20   21   22   23   24   25   26
#  27   28   29   30   31


class TestMissingIntervals_TLV(unittest.TestCase):

    def setUp(self):
        self.exchange = "TLV"
        self.tz = 'Asia/Jerusalem'
        self.market_tz = ZoneInfo(self.tz)
        yfct.SetExchangeTzName(self.exchange, self.tz)

        self.market_open = time(9, 59)
        self.market_close = time(17, 15)
        self.market_close_sunday = time(15, 40)

    def test_IdentifyMissingIntervalRanges_basics(self):
        # Test super-simple scenarios
        interval = yfcd.Interval.Hours1

        # Sunday
        d = 6
        day1 = date(2022, 3, d)
        day2 = date(2022, 3, d+1)
        startDt = dtc(day1, time(9, 30), self.market_tz)
        endDt = dtc(day1, time(15, 51), self.market_tz)
        # Test 1: no known intervals -> returns all intervals in date range
        knownIntervalStarts = None
        answer = [( dtc(day1, time(9, 30), self.market_tz), 
                    dtc(day1, time(15, 51), self.market_tz))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("startDt = {}".format(startDt))
            print("endDt = {}".format(endDt))
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise
        # Test 2: known intervals == range -> returns nothing
        knownIntervalStarts = [dtc(day1, time(h, 30), self.market_tz) for h in [9, 10, 11, 12, 13, 14, 15]]
        knownIntervalStarts += [dtc(day2, time(h, 30), self.market_tz) for h in [9, 10, 11, 12, 13, 14, 15]]
        answer = None
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        # Rest of week:
        for d in [7, 8, 9]:
            day1 = date(2022, 3, d)
            day2 = date(2022, 3, d+1)

            startDt = dtc(day1, time(9, 30), self.market_tz)
            endDt = dtc(day2, time(17, 30), self.market_tz)

            # Test 1: no known intervals -> returns all intervals in date range
            knownIntervalStarts = None
            answer = [( dtc(day1, time(9, 30), self.market_tz), 
                        dtc(day2, time(17, 26), self.market_tz))]
            ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts)
            try:
                self.assertEqual(ranges, answer)
            except:
                print("startDt = {}".format(startDt))
                print("endDt = {}".format(endDt))
                print("ranges:")
                pprint(ranges)
                print("answer:")
                pprint(answer)
                raise

            # Test 2: known intervals == range -> returns nothing
            knownIntervalStarts = [dtc(day1, time(h, 30), self.market_tz) for h in [9, 10, 11, 12, 13, 14, 15, 16]]
            knownIntervalStarts += [dtc(day2, time(h, 30), self.market_tz) for h in [9, 10, 11, 12, 13, 14, 15, 16]]
            answer = None
            ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts)
            try:
                self.assertEqual(ranges, answer)
            except:
                print("ranges:")
                pprint(ranges)
                print("answer:")
                pprint(answer)
                raise

    def test_IdentifyMissingIntervalRanges_simpleMissingDays(self):
        # Test simple scenarios of missing days
        interval = yfcd.Interval.Days1

        # missing 2nd day of 2-day range
        start_d = date(2022, 3, 6)
        end_d = date(2022, 3, 8)
        knownIntervalStarts = [date(2022, 3, 6)]
        answer = [(date(2022, 3, 7), date(2022, 3, 8))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, start_d, end_d, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        # missing 1st day of 2-day range
        start_d = date(2022, 3, 6)
        end_d = date(2022, 3, 8)
        knownIntervalStarts = [date(2022, 3, 7)]
        answer = [(date(2022, 3, 6), date(2022, 3, 7))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, start_d, end_d, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        # missing middle day of 3-day range
        start_d = date(2022, 3, 6)
        end_d = date(2022, 3, 9)
        knownIntervalStarts = [date(2022, 3, 6), date(2022, 3, 8)]
        answer = [(date(2022, 3, 7), date(2022, 3, 8))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, start_d, end_d, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        # only have middle day of 3-day range
        start_d = date(2022, 3, 6)
        end_d = date(2022, 3, 9)
        knownIntervalStarts = [date(2022, 3, 7)]
        answer = [(date(2022, 3, 6), date(2022, 3, 7)), 
                  (date(2022, 3, 8), date(2022, 3, 9))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, start_d, end_d, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

    def test_IdentifyMissingIntervalRanges_complexMissingDays(self):
        # Test complex scenarios of missing days
        interval = yfcd.Interval.Days1

        # Missing 1st, 3rd and 5th days of week:
        start_d = date(2022, 3, 6)
        end_d = date(2022, 3, 11)
        knownIntervalStarts = [date(2022, 3, 7), date(2022, 3, 9)]
        answer = [(date(2022, 3, d), date(2022, 3, d+1)) for d in [6, 8, 10]]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, start_d, end_d, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise
        # With merging:
        answer = [(date(2022, 3, 6), date(2022, 3, 11))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, start_d, end_d, interval, knownIntervalStarts, minDistanceThreshold=1)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        # Missing Friday of week 1, and Tuesday of next week (note: second Monday is public holiday)
        start_d = date(2022, 3, 6)
        end_d = date(2022, 3, 18)
        knownIntervalStarts = [date(2022, 3, d) for d in [6, 7, 8, 9 , 15, 16, 17]]
        answer = [(date(2022, 3, 10), date(2022, 3, 15))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, start_d, end_d, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

    def test_IdentifyMissingIntervalRanges_simpleMissingHours(self):
        # Test simple scenarios of missing intermittent hours within days
        interval = yfcd.Interval.Hours1

        tz = self.market_tz
        day = date(2022, 3, 7)
        startDt = dtc(day, time(9, 30), self.market_tz)
        endDt = dtc(day, time(17, 30), self.market_tz)

        # Missing [12.30pm, 13.30pm)
        knownIntervalStarts = []
        for h in [9, 10, 11   , 13, 14, 15, 16]:
            knownIntervalStarts.append(dtc(day, time(h, 30), self.market_tz))
        answer = [(dtc(day, time(12, 30), self.market_tz), dtc(day, time(13, 30), self.market_tz))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        # Missing [12.30pm) , [2.30pm)
        knownIntervalStarts = []
        for h in [9, 10, 11   , 13   , 15, 16]:
            knownIntervalStarts.append(dtc(day, time(h, 30), self.market_tz))
        # - should NOT be merged if threshold=0
        answer = [(dtc(day, time(12, 30), self.market_tz), dtc(day, time(13, 30), self.market_tz)),
                  (dtc(day, time(14, 30), self.market_tz), dtc(day, time(15, 30), self.market_tz))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise
        # - should be merged if threshold>=1
        answer = [(dtc(day, time(12, 30), self.market_tz), dtc(day, time(15, 30), self.market_tz))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts, minDistanceThreshold=1)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        # Missing [1.30pm) , [3.30pm). Should be merged if threshold>=1
        knownIntervalStarts = []
        for h in [9, 10, 11, 12   , 14   , 16]:
            knownIntervalStarts.append(dtc(day, time(h, 30), self.market_tz))
        answer = [(dtc(day, time(13, 30), self.market_tz), dtc(day, time(16, 30), self.market_tz))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts, minDistanceThreshold=1)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        # Missing [10.30am) , [12.30pm). Should be merged if threshold>=1
        knownIntervalStarts = []
        for h in [9   , 11   , 13, 14, 15, 16]:
            knownIntervalStarts.append(dtc(day, time(h, 30), self.market_tz))
        answer = [(dtc(day, time(10, 30), self.market_tz), dtc(day, time(13, 30), self.market_tz))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts, minDistanceThreshold=1)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

    def test_IdentifyMissingIntervalRanges_complexMissingHours(self):
        interval = yfcd.Interval.Hours1

        thr = 2

        # Case 1
        tz = self.market_tz
        day1 = date(2022, 3, 7)
        day2 = date(2022, 3, 8)
        startDt = dtc(day1, time(9, 30), self.market_tz)
        endDt = dtc(day2, time(17, 30), self.market_tz)
        knownIntervalStarts = []
        for h in [9, 10   , 12  , 14, 15, 16]:
            knownIntervalStarts.append(dtc(day1, time(h, 30), self.market_tz))
        for h in [9, 10   , 12  , 14, 15, 16]:
            knownIntervalStarts.append(dtc(day2, time(h, 30), self.market_tz))
        answer = [(dtc(day1, time(11, 30), self.market_tz), dtc(day1, time(14, 30), self.market_tz)),
                  (dtc(day2, time(11, 30), self.market_tz), dtc(day2, time(14, 30), self.market_tz))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts, minDistanceThreshold=thr)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

    def test_IdentifyMissingIntervalRanges_weekly(self):
        # Test simple scenarios of missing weeks
        interval = yfcd.Interval.Week

        knownIntervalStarts = [date(2022, 3, 7)]
        answer = None
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, date(2022, 3, 7), date(2022, 3, 14), interval, knownIntervalStarts)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise


if __name__ == '__main__':
    unittest.main()
