import unittest
from pprint import pprint

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

## 2022 calendar:
## X* = day X is USA public holiday that closed NYSE
##  -- February --
##  Mo   Tu   We   Th   Fr   Sa   Su
##  7    8    9    10   11   12   13
##  14   15   16   17   18   19   20
##  21*  22   23   24   25   26   27
##  28

class TestMissingIntervals(unittest.TestCase):

    def setUp(self):
        self.monday    = date(year=2022, month=2, day=14)
        self.tuesday   = date(year=2022, month=2, day=15)
        self.wednesday = date(year=2022, month=2, day=16)
        self.thursday  = date(year=2022, month=2, day=17)
        self.friday    = date(year=2022, month=2, day=18)
        self.saturday  = date(year=2022, month=2, day=19)
        self.monday2   = date(year=2022, month=2, day=21)
        self.tuesday2  = date(year=2022, month=2, day=22)
        self.wednesday2= date(year=2022, month=2, day=23)
        self.thursday2 = date(year=2022, month=2, day=24)
        self.friday2   = date(year=2022, month=2, day=25)

        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.exchangeOpenTime = time(hour=9, minute=30)
        self.exchangeLastHrInt = time(hour=15, minute=30)
        self.exchangeCloseTime = time(hour=16, minute=0)

    def test_IdentifyMissingIntervalRanges_basics(self):
        ## Test super-simple scenarios
        interval = yfcd.Interval.Hours1

        ## Test 1: no known intervals -> returns all intervals in date range
        startDay = self.monday
        endDay = startDay + timedelta(days=2)
        knownIntervalStarts = None
        startDt = datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(endDay,   self.exchangeCloseTime, tzinfo=self.market_tz)
        answer = [( datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz), 
                    datetime.combine(endDay,   self.exchangeCloseTime, tzinfo=self.market_tz))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## Test 2: known intervals == range -> returns nothing
        startDay = self.monday
        endDay   = startDay
        startDt = datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(endDay,   self.exchangeCloseTime, tzinfo=self.market_tz)
        knownIntervalStarts = [datetime.combine(startDay, self.exchangeOpenTime, tzinfo=self.market_tz)]
        nextInt = knownIntervalStarts[-1] + timedelta(hours=1)
        while nextInt.time() < self.exchangeCloseTime:
            knownIntervalStarts.append(nextInt)
            nextInt = knownIntervalStarts[-1] + timedelta(hours=1)
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
        ## Test simple scenarios of missing days
        interval = yfcd.Interval.Days1

        ## missing 2nd day of 2-day range
        startDay = self.monday
        endDay = self.wednesday
        knownIntervalStarts = [self.monday]
        answer = [(self.tuesday, self.wednesday)]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDay, endDay, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## missing 1st day of 2-day range
        startDay = self.monday
        endDay = self.wednesday
        knownIntervalStarts = [self.tuesday]
        answer = [(self.monday, self.tuesday)]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDay, endDay, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## missing middle day of 3-day range
        startDay = self.monday
        endDay = self.thursday
        knownIntervalStarts = [self.monday, self.wednesday]
        answer = [(self.tuesday, self.wednesday)]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDay, endDay, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## only have middle day of 3-day range
        startDay = self.monday
        endDay = self.thursday
        knownIntervalStarts = [self.tuesday]
        answer = [(self.monday, self.tuesday), (self.wednesday, self.thursday)]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDay, endDay, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

    def test_IdentifyMissingIntervalRanges_complexMissingDays(self):
        ## Test complex scenarios of missing days
        interval = yfcd.Interval.Days1

        ## Missing 1st, 3rd and 5th days of week:
        startDay = self.monday
        endDay = self.saturday
        knownIntervalStarts = [self.tuesday, self.thursday]
        answer = [(self.monday,self.tuesday) , (self.wednesday,self.thursday) , (self.friday,self.saturday)]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDay, endDay, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise
        # With merging:
        answer = [(self.monday, self.saturday)]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDay, endDay, interval, knownIntervalStarts, minDistanceThreshold=1)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## Missing Friday of week 1, and Tuesday of next week (note: second Monday is public holiday)
        startDay = self.monday
        endDay = self.saturday+timedelta(days=7)
        knownIntervalStarts = [self.monday, self.tuesday, self.wednesday, self.thursday, 
                                          self.wednesday2, self.thursday2, self.friday2]
        answer = [(self.friday, self.wednesday2)]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDay, endDay, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

    def test_IdentifyMissingIntervalRanges_simpleMissingHours(self):
        ## Test simple scenarios of missing intermittent hours within days
        interval = yfcd.Interval.Hours1
        td_hr = timedelta(hours=1)

        day = self.monday
        startDt = datetime.combine(day, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(day, self.exchangeCloseTime, tzinfo=self.market_tz)

        ## Missing [1.30pm,2.30pm)
        knownIntervalStarts = []
        for h in [0,1,2,3,  5,6]:
            ## Missing 4
            knownIntervalStarts.append(startDt + h*td_hr)
        answer = [(startDt+4*td_hr, startDt+5*td_hr)]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## Missing [12.30pm) , [2.30pm)
        knownIntervalStarts = []
        for h in [0,1,2,  4,  6]:
            ## Missing 3,5
            knownIntervalStarts.append(startDt + h*td_hr)
        ## - should NOT be merged if threshold=0
        answer = [(startDt+3*td_hr, startDt+4*td_hr),
                  (startDt+5*td_hr, startDt+6*td_hr)]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise
        ## - should be merged if threshold>=1
        answer = [(startDt+3*td_hr, startDt+6*td_hr)]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts, minDistanceThreshold=1)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## Missing [13.30pm) , [3.30pm). Should be merged if threshold>=1
        knownIntervalStarts = []
        for h in [0,1,2,3,  5  ]:
            ## Missing 4,6
            knownIntervalStarts.append(startDt + h*td_hr)
        answer = [(startDt+4*td_hr, datetime.combine(day, self.exchangeCloseTime, self.market_tz))]
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervalStarts, minDistanceThreshold=1)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## Missing [9.30pm) , [11.30pm). Should be merged if threshold>=1
        knownIntervalStarts = []
        for h in [  1,  3,4,5,6]:
            ## Missing 0,2
            knownIntervalStarts.append(startDt + h*td_hr)
        answer = [(startDt+0*td_hr, startDt+3*td_hr)]
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
        td_hr = timedelta(hours=1)

        thr = 2
        
        ## Case 1
        day1 = self.monday
        day2 = self.tuesday
        day1_startDt = datetime.combine(day1, self.exchangeOpenTime,  tzinfo=self.market_tz)
        day2_startDt = datetime.combine(day2, self.exchangeOpenTime,  tzinfo=self.market_tz)
        startDt = datetime.combine(day1, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(day2, self.exchangeCloseTime, tzinfo=self.market_tz)
        knownIntervalStarts = []
        for h in [  1,2,  4,5,6]:
            ## Missing 1,3
            knownIntervalStarts.append(day1_startDt + h*td_hr)
        for h in [  1,2,  4,5,6]:
            ## Missing 1,3
            knownIntervalStarts.append(day2_startDt + h*td_hr)
        answer = [ (day1_startDt, day1_startDt+4*td_hr) , 
                    (day2_startDt, day2_startDt+4*td_hr)]
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
        ## Test simple scenarios of missing weeks
        interval = yfcd.Interval.Week

        knownIntervalStarts = [self.monday]
        answer = None
        ranges = yfct.IdentifyMissingIntervalRanges(self.exchange, self.monday, self.saturday, interval, knownIntervalStarts)
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
