import unittest

import sys
sys.path.append("/home/gonzo/Repos/yfinance-cache/src/yfinance_cache")
from yfc_time import *

from datetime import datetime, date, time, timedelta

def timeAddTd(t, td):
    dt = datetime.combine(date(year=2022, month=1, day=1), t) + td
    return dt.time()

class TestMissingIntervals(unittest.TestCase):

    def setUp(self):
        self.day = date(year=2022, month=1, day=1)
        self.week_start = date(year=2022, month=1, day=3)

        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.exchangeOpenTime = time(hour=9, minute=30)
        self.exchangeLastHrInt = time(hour=15, minute=30)
        self.exchangeCloseTime = time(hour=16, minute=0)

    def test_IdentifyMissingIntervalRanges_basics(self):
        ## Test super-simple scenarios
        interval = Interval.Hours1

        ## Test 1: no known intervals -> returns all intervals in date range
        startDay = self.week_start
        endDay = startDay + timedelta(days=2)
        knownIntervals = None
        startDt = datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(endDay,   self.exchangeCloseTime, tzinfo=self.market_tz)
        answer = [( datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz), 
                    datetime.combine(endDay,   self.exchangeLastHrInt, tzinfo=self.market_tz))]
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise
        return

        ## Test 2: known intervals == range -> returns nothing
        startDay = self.week_start
        endDay   = startDay
        startDt = datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(endDay,   self.exchangeCloseTime, tzinfo=self.market_tz)
        knownIntervals = [datetime.combine(startDay, self.exchangeOpenTime, tzinfo=self.market_tz)]
        nextInt = knownIntervals[-1] + timedelta(hours=1)
        while nextInt.time() < self.exchangeCloseTime:
            knownIntervals.append(nextInt)
            nextInt = knownIntervals[-1] + timedelta(hours=1)
        answer = None
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals)
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
        interval = Interval.Hours1

        ## missing 2nd day of 2-day range
        startDay = self.week_start
        endDay = startDay + timedelta(days=1)
        startDt = datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(endDay,   self.exchangeCloseTime, tzinfo=self.market_tz)
        knownIntervals = [datetime.combine(startDay, self.exchangeOpenTime, tzinfo=self.market_tz)]
        nextInt = knownIntervals[-1] + timedelta(hours=1)
        while nextInt.time() < self.exchangeCloseTime:
            knownIntervals.append(nextInt)
            nextInt = knownIntervals[-1] + timedelta(hours=1)
        answer = [( datetime.combine(endDay, self.exchangeOpenTime,  tzinfo=self.market_tz), 
                    datetime.combine(endDay, self.exchangeLastHrInt, tzinfo=self.market_tz))]
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## missing 1st day of 2-day range
        startDay = self.week_start
        endDay = startDay + timedelta(days=1)
        startDt = datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(endDay,   self.exchangeCloseTime, tzinfo=self.market_tz)
        knownIntervals = [datetime.combine(endDay, self.exchangeOpenTime, tzinfo=self.market_tz)]
        nextInt = knownIntervals[-1] + timedelta(hours=1)
        while nextInt.time() < self.exchangeCloseTime:
            knownIntervals.append(nextInt)
            nextInt = knownIntervals[-1] + timedelta(hours=1)
        answer = [ (datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz), 
                    datetime.combine(startDay, self.exchangeLastHrInt, tzinfo=self.market_tz))]
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## missing middle day of 3-day range
        startDay = self.week_start
        endDay = startDay + timedelta(days=2)
        startDt = datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(endDay,   self.exchangeCloseTime, tzinfo=self.market_tz)
        knownIntervals = [datetime.combine(startDay, self.exchangeOpenTime, tzinfo=self.market_tz)]
        nextInt = knownIntervals[-1] + timedelta(hours=1)
        while nextInt.time() < self.exchangeCloseTime:
            knownIntervals.append(nextInt)
            nextInt = knownIntervals[-1] + timedelta(hours=1)
        nextInt = datetime.combine(endDay, self.exchangeOpenTime, tzinfo=self.market_tz)
        while nextInt.time() < self.exchangeCloseTime:
            knownIntervals.append(nextInt)
            nextInt = knownIntervals[-1] + timedelta(hours=1)
        answer = [ (datetime.combine(startDay+timedelta(days=1), self.exchangeOpenTime,  tzinfo=self.market_tz), 
                    datetime.combine(startDay+timedelta(days=1), self.exchangeLastHrInt, tzinfo=self.market_tz))]
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## only have middle day of 3-day range
        startDay = self.week_start
        endDay = startDay + timedelta(days=2)
        startDt = datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(endDay,   self.exchangeCloseTime, tzinfo=self.market_tz)
        knownIntervals = [datetime.combine(startDay+timedelta(days=1), self.exchangeOpenTime, tzinfo=self.market_tz)]
        nextInt = knownIntervals[-1] + timedelta(hours=1)
        while nextInt.time() < self.exchangeCloseTime:
            knownIntervals.append(nextInt)
            nextInt = knownIntervals[-1] + timedelta(hours=1)
            nextInt = knownIntervals[-1] + timedelta(hours=1)
        answer = [ (datetime.combine(startDay,                   self.exchangeOpenTime,  tzinfo=self.market_tz), 
                    datetime.combine(startDay,                   self.exchangeLastHrInt, tzinfo=self.market_tz)),
                   (datetime.combine(startDay+timedelta(days=2), self.exchangeOpenTime,  tzinfo=self.market_tz), 
                    datetime.combine(startDay+timedelta(days=2), self.exchangeLastHrInt, tzinfo=self.market_tz))]
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals)
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
        interval = Interval.Hours1

        ## Missing 1st, 3rd and 5th days of week:
        startDay = self.week_start
        endDay = startDay + timedelta(days=4)
        startDt = datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(endDay,   self.exchangeCloseTime, tzinfo=self.market_tz)
        knownIntervals = [datetime.combine(startDay+timedelta(days=1), self.exchangeOpenTime, tzinfo=self.market_tz)]
        nextInt = knownIntervals[-1] + timedelta(hours=1)
        while nextInt.time() < self.exchangeCloseTime:
            knownIntervals.append(nextInt)
            nextInt = knownIntervals[-1] + timedelta(hours=1)
        nextInt = datetime.combine(startDay+timedelta(days=3), self.exchangeOpenTime, tzinfo=self.market_tz)
        while nextInt.time() < self.exchangeCloseTime:
            knownIntervals.append(nextInt)
            nextInt = knownIntervals[-1] + timedelta(hours=1)
        answer = [ (datetime.combine(startDay,                   self.exchangeOpenTime,  tzinfo=self.market_tz), 
                    datetime.combine(startDay,                   self.exchangeLastHrInt, tzinfo=self.market_tz)),
                   (datetime.combine(startDay+timedelta(days=2), self.exchangeOpenTime,  tzinfo=self.market_tz), 
                    datetime.combine(startDay+timedelta(days=2), self.exchangeLastHrInt, tzinfo=self.market_tz)),
                   (datetime.combine(startDay+timedelta(days=4), self.exchangeOpenTime,  tzinfo=self.market_tz), 
                    datetime.combine(startDay+timedelta(days=4), self.exchangeLastHrInt, tzinfo=self.market_tz))]
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## Missing Friday of week 1, and Monday of next week
        startDay = self.week_start
        endDay = startDay + timedelta(days=7) + timedelta(days=4)
        startDt = datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(endDay,   self.exchangeCloseTime, tzinfo=self.market_tz)
        knownIntervals = []
        for d in [0,1,2,3,  8,9,10,11]:
            nextInt = datetime.combine(startDay+timedelta(days=d), self.exchangeOpenTime, tzinfo=self.market_tz)
            while nextInt.time() < self.exchangeCloseTime:
                knownIntervals.append(nextInt)
                nextInt = knownIntervals[-1] + timedelta(hours=1)
        answer = [ (datetime.combine(startDay+timedelta(days=4), self.exchangeOpenTime,  tzinfo=self.market_tz), 
                    datetime.combine(startDay+timedelta(days=7), self.exchangeLastHrInt, tzinfo=self.market_tz))]
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals)
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
        interval = Interval.Hours1
        hour = timedelta(hours=1)

        startDay = self.week_start
        endDay   = self.week_start
        startDt = datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(endDay,   self.exchangeCloseTime, tzinfo=self.market_tz)

        ## Missing [1.30pm,2.30pm)
        knownIntervals = []
        for h in [0,1,2,3,  5,6]:
            ## Missing 4
            knownIntervals.append(datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*h), tzinfo=self.market_tz))
        answer = [ (datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*4), tzinfo=self.market_tz), 
                    datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*4), tzinfo=self.market_tz))]
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## Missing [12.30pm) , [2.30pm)
        knownIntervals = []
        for h in [0,1,2,  4,  6]:
            ## Missing 3,5
            knownIntervals.append(datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*h), tzinfo=self.market_tz))
        ## - should NOT be merged if threshold=0
        answer = [ (datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*3), tzinfo=self.market_tz), 
                    datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*3), tzinfo=self.market_tz)), 
                   (datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*5), tzinfo=self.market_tz), 
                    datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*5), tzinfo=self.market_tz))]
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals, minDistanceThreshold=0)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise
        ## - should be merged if threshold>=1
        answer = [ (datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*3), tzinfo=self.market_tz), 
                    datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*5), tzinfo=self.market_tz))]
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals, minDistanceThreshold=1)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## Missing [13.30pm) , [3.30pm). Should be merged if threshold>=1
        knownIntervals = []
        for h in [0,1,2,3,  5  ]:
            ## Missing 4,6
            knownIntervals.append(datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*h), tzinfo=self.market_tz))
        answer = [ (datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*4), tzinfo=self.market_tz), 
                    datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*6), tzinfo=self.market_tz))]
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals, minDistanceThreshold=1)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

        ## Missing [9.30pm) , [11.30pm). Should be merged if threshold>=1
        knownIntervals = []
        for h in [  1,  3,4,5,6]:
            ## Missing 0,2
            knownIntervals.append(datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*h), tzinfo=self.market_tz))
        answer = [ (datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*0), tzinfo=self.market_tz), 
                    datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*2), tzinfo=self.market_tz))]
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals, minDistanceThreshold=1)
        try:
            self.assertEqual(ranges, answer)
        except:
            print("ranges:")
            pprint(ranges)
            print("answer:")
            pprint(answer)
            raise

    def test_IdentifyMissingIntervalRanges_complexMissingHours(self):
        interval = Interval.Hours1
        hour = timedelta(hours=1)

        thr = 2
        
        ## Case 1
        startDay = self.week_start
        endDay   = self.week_start + timedelta(days=1)
        startDt = datetime.combine(startDay, self.exchangeOpenTime,  tzinfo=self.market_tz)
        endDt   = datetime.combine(endDay,   self.exchangeCloseTime, tzinfo=self.market_tz)
        knownIntervals = []
        for h in [  1,2,  4,5,6]:
            ## Missing 1,3
            knownIntervals.append(datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*h), tzinfo=self.market_tz))
        for h in [  1,2,  4,5,6]:
            ## Missing 1,3
            knownIntervals.append(datetime.combine(endDay, timeAddTd(self.exchangeOpenTime,hour*h), tzinfo=self.market_tz))
        answer = [ (datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*0), tzinfo=self.market_tz), 
                    datetime.combine(startDay, timeAddTd(self.exchangeOpenTime,hour*3), tzinfo=self.market_tz)), 
                   (datetime.combine(endDay,   timeAddTd(self.exchangeOpenTime,hour*0), tzinfo=self.market_tz), 
                    datetime.combine(endDay,   timeAddTd(self.exchangeOpenTime,hour*3), tzinfo=self.market_tz))]
        ranges = IdentifyMissingIntervalRanges(self.exchange, startDt, endDt, interval, knownIntervals, minDistanceThreshold=thr)
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
