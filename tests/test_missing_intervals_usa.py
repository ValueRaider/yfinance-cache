import unittest
from pprint import pprint

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

from datetime import datetime, date, time
dtc = datetime.combine
from zoneinfo import ZoneInfo

# 2022 calendar:
# X* = day X is USA public holiday that closed NYSE
#  -- February --
#  Mo   Tu   We   Th   Fr   Sa   Su
#  7    8    9    10   11   12   13
#  14   15   16   17   18   19   20
#  21*  22   23   24   25   26   27
#  28


class TestMissingIntervals_USA(unittest.TestCase):

    def setUp(self):
        self.exchange = "NMS"
        self.tz = 'US/Eastern'
        self.market_tz = ZoneInfo(self.tz)
        yfct.SetExchangeTzName(self.exchange, self.tz)

    def test_IdentifyMissingIntervalRanges_basics(self):
        # Test super-simple scenarios
        interval = yfcd.Interval.Hours1

        for d in [14, 15, 16, 17]:
            day1 = date(2022, 2, d)
            day2 = date(2022, 2, d+1)

            # Test 1: no known intervals -> returns all intervals in date range
            knownIntervalStarts = None
            startDt = dtc(day1, time(9, 30), self.market_tz)
            endDt = dtc(day2, time(16), self.market_tz)
            answer = [(dtc(day1, time(9, 30), self.market_tz),
                       dtc(day2, time(16), self.market_tz))]
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
            startDt = dtc(day1, time(9, 30), self.market_tz)
            endDt = dtc(day2, time(16), self.market_tz)
            knownIntervalStarts  = [dtc(day1, time(h, 30), self.market_tz) for h in [9, 10, 11, 12, 13, 14, 15]]
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

    def test_IdentifyMissingIntervalRanges_simpleMissingDays(self):
        # Test simple scenarios of missing days
        interval = yfcd.Interval.Days1

        # missing 2nd day of 2-day range
        start_d = date(2022, 2, 14)
        end_d = date(2022, 2, 16)
        knownIntervalStarts = [date(2022, 2, 14)]
        answer = [(date(2022, 2, 15), date(2022, 2, 16))]
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
        start_d = date(2022, 2, 14)
        end_d = date(2022, 2, 16)
        knownIntervalStarts = [date(2022, 2, 15)]
        answer = [(date(2022, 2, 14), date(2022, 2, 15))]
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
        start_d = date(2022, 2, 14)
        end_d = date(2022, 2, 17)
        knownIntervalStarts = [date(2022, 2, 14), date(2022, 2, 16)]
        answer = [(date(2022, 2, 15), date(2022, 2, 16))]
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
        start_d = date(2022, 2, 14)
        end_d = date(2022, 2, 17)
        knownIntervalStarts = [date(2022, 2, 15)]
        answer = [(date(2022, 2, 14), date(2022, 2, 15)),
                  (date(2022, 2, 16), date(2022, 2, 17))]
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
        start_d = date(2022, 2, 14)
        end_d = date(2022, 2, 19)
        knownIntervalStarts = [date(2022, 2, 15), date(2022, 2, 17)]
        answer = [(date(2022, 2, d), date(2022, 2, d+1)) for d in [14, 16, 18]]
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
        answer = [(date(2022, 2, 14), date(2022, 2, 19))]
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
        start_d = date(2022, 2, 14)
        end_d = date(2022, 2, 26)
        knownIntervalStarts = [date(2022, 2, d) for d in [14, 15, 16, 17 , 23, 24, 25]]
        answer = [(date(2022, 2, 18), date(2022, 2, 23))]
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

        day = date(2022, 2, 14)
        startDt = dtc(day, time(9, 30), self.market_tz)
        endDt = dtc(day, time(16), self.market_tz)

        # Missing [1.30pm, 2.30pm)
        knownIntervalStarts = []
        for h in [9, 10, 11, 12   , 14, 15]:
            knownIntervalStarts.append(dtc(day, time(h, 30), self.market_tz))
        answer = [(dtc(day, time(13, 30), self.market_tz), dtc(day, time(14, 30), self.market_tz))]
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
        for h in [9, 10, 11   , 13   , 15]:
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

        # Missing [13.30pm) , [3.30pm). Should be merged if threshold>=1
        knownIntervalStarts = []
        for h in [9, 10, 11, 12   , 14   ]:
            knownIntervalStarts.append(dtc(day, time(h, 30), self.market_tz))
        answer = [(dtc(day, time(13, 30), self.market_tz), dtc(day, time(16), self.market_tz))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts, minDistanceThreshold=1)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        # Missing [9.30pm) , [11.30pm). Should be merged if threshold>=1
        knownIntervalStarts = []
        for h in [  10,   12, 13, 14, 15]:
            knownIntervalStarts.append(dtc(day, time(h, 30), self.market_tz))
        answer = [(dtc(day, time(9, 30), self.market_tz), dtc(day, time(12, 30), self.market_tz))]
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
        day1 = date(2022, 2, 14)
        day2 = date(2022, 2, 15)
        startDt = dtc(day1, time(9, 30), self.market_tz)
        endDt = dtc(day2, time(16), self.market_tz)
        knownIntervalStarts = []
        # Missing 10am, 12am
        for h in [9   , 11   , 13, 14, 15]:
            knownIntervalStarts.append(dtc(day1, time(h, 30), self.market_tz))
        for h in [9   , 11   , 13, 14, 15]:
            knownIntervalStarts.append(dtc(day2, time(h, 30), self.market_tz))
        answer = [(dtc(day1, time(10, 30), self.market_tz), dtc(day1, time(13, 30), self.market_tz)),
                  (dtc(day2, time(10, 30), self.market_tz), dtc(day2, time(13, 30), self.market_tz))]
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

        knownIntervalStarts = [date(2022, 2, 14)]
        answer = None
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, date(2022, 2, 14), date(2022, 2, 21), interval, knownIntervalStarts)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        knownIntervalStarts = None
        answer = [(date(2022, 2, 14), date(2022, 2, 21))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, date(2022, 2, 14), date(2022, 2, 21), interval, knownIntervalStarts)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        knownIntervalStarts = None
        answer = [(date(2022, 2, 14), date(2022, 2, 21))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, date(2022, 2, 10), date(2022, 2, 25), interval, knownIntervalStarts)
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
