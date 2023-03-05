import unittest
from pprint import pprint

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

from datetime import datetime, date, time, timedelta
dtc = datetime.combine
from zoneinfo import ZoneInfo

# 2022 calendar:
# X* = day X is public holiday that closed exchange
#  -- April --
#  Mo   Tu   We   Th   Fr   Sa   Su
#  4    5    6    7    8    9    10
#  11   12   13   14   15*  16   17
#  18*  19   20   21   22   23   24
#  25*


class TestMissingIntervals_ASX(unittest.TestCase):

    def setUp(self):
        self.market = "au_market"
        self.exchange = "ASX"
        self.tz = 'Australia/Sydney'
        self.market_tz = ZoneInfo(self.tz)
        yfct.SetExchangeTzName(self.exchange, self.tz)

    def test_IdentifyMissingIntervalRanges_basics(self):
        # Test super-simple scenarios
        interval = yfcd.Interval.Hours1

        for d in [4, 5, 6, 7]:
            day1 = date(2022, 4, d)
            day2 = date(2022, 4, d+1)

            # Test 1: no known intervals -> returns all intervals in date range
            knownIntervalStarts = None
            startDt = dtc(day1, time(10), self.market_tz)
            endDt = dtc(day2, time(16), self.market_tz)
            answer = [( dtc(day1, time(10), self.market_tz), 
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
            startDt = dtc(day1, time(10), self.market_tz)
            endDt = dtc(day2, time(16), self.market_tz)
            knownIntervalStarts = [dtc(day1, time(h), self.market_tz) for h in [10, 11, 12, 13, 14, 15, 16]]
            knownIntervalStarts += [dtc(day2, time(h), self.market_tz) for h in [10, 11, 12, 13, 14, 15, 16]]
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
        start_d = date(2022, 4, 4)
        end_d = date(2022, 4, 6)
        knownIntervalStarts = [date(2022, 4, 4)]
        answer = [(date(2022, 4, 5), date(2022, 4, 6))]
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
        start_d = date(2022, 4, 4)
        end_d = date(2022, 4, 6)
        knownIntervalStarts = [date(2022, 4, 5)]
        answer = [(date(2022, 4, 4), date(2022, 4, 5))]
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
        start_d = date(2022, 4, 4)
        end_d = date(2022, 4, 7)
        knownIntervalStarts = [date(2022, 4, 4), date(2022, 4, 6)]
        answer = [(date(2022, 4, 5), date(2022, 4, 6))]
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
        start_d = date(2022, 4, 4)
        end_d = date(2022, 4, 7)
        knownIntervalStarts = [date(2022, 4, 5)]
        answer = [(date(2022, 4, 4), date(2022, 4, 5)), 
                  (date(2022, 4, 6), date(2022, 4, 7))]
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
        start_d = date(2022, 4, 4)
        end_d = date(2022, 4, 9)
        knownIntervalStarts = [date(2022, 4, 5), date(2022, 4, 7)]
        answer = [(date(2022, 4, d), date(2022, 4, d+1)) for d in [4, 6, 8]]
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
        answer = [(date(2022, 4, 4), date(2022, 4, 9))]
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
        start_d = date(2022, 4, 4)
        end_d = date(2022, 4, 16)
        knownIntervalStarts = [date(2022, 4, d) for d in [4, 5, 6, 7 , 13, 14, 15]]
        answer = [(date(2022, 4, 8), date(2022, 4, 13))]
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

        day = date(2022, 4, 4)
        startDt = dtc(day, time(10), self.market_tz)
        endDt = dtc(day, time(16), self.market_tz)

        # Missing [1pm, 2pm)
        knownIntervalStarts = []
        for h in [10, 11, 12,   14, 15]:
            knownIntervalStarts.append(dtc(day, time(h), self.market_tz))
        answer = [(dtc(day, time(13), self.market_tz), dtc(day, time(14), self.market_tz))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        # Missing [12pm) , [2pm)
        knownIntervalStarts = []
        for h in [10, 11,   13,   15]:
            knownIntervalStarts.append(dtc(day, time(h), self.market_tz))
        # - should NOT be merged if threshold=0
        answer = [(dtc(day, time(12), self.market_tz), dtc(day, time(13), self.market_tz)),
                  (dtc(day, time(14), self.market_tz), dtc(day, time(15), self.market_tz))]
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
        answer = [(dtc(day, time(12), self.market_tz), dtc(day, time(15), self.market_tz))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts, minDistanceThreshold=1)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        # Missing [1pm) , [3pm). Should be merged if threshold>=1
        knownIntervalStarts = []
        for h in [10, 11, 12,   14   ]:
            knownIntervalStarts.append(dtc(day, time(h), self.market_tz))
        answer = [(dtc(day, time(13), self.market_tz), dtc(day, time(16), self.market_tz))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts, minDistanceThreshold=1)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        # Missing [10am) , [12pm). Should be merged if threshold>=1
        knownIntervalStarts = []
        for h in [   11   , 13, 14, 15]:
            knownIntervalStarts.append(dtc(day, time(h), self.market_tz))
        answer = [(dtc(day, time(10), self.market_tz), dtc(day, time(13), self.market_tz))]
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
        day1 = date(2022, 4, 4)
        day2 = date(2022, 4, 5)
        startDt = dtc(day1, time(10), self.market_tz)
        endDt = dtc(day2, time(16), self.market_tz)
        knownIntervalStarts = []
        for h in [10   , 12  , 14, 15, 16]:
            knownIntervalStarts.append(dtc(day1, time(h), self.market_tz))
        for h in [10   , 12  , 14, 15]:
            knownIntervalStarts.append(dtc(day2, time(h), self.market_tz))
        answer = [(dtc(day1, time(11), self.market_tz), dtc(day1, time(14), self.market_tz)),
                  (dtc(day2, time(11), self.market_tz), dtc(day2, time(14), self.market_tz))]
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

        knownIntervalStarts = [date(2022, 4, 4)]
        answer = None
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, date(2022, 4, 4), date(2022, 4, 11), interval, knownIntervalStarts)
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
