import unittest

import sys
sys.path.append("/home/gonzo/Repos/yfinance-cache/src/yfinance_cache")
from yfc_time import *

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from pprint import pprint

class Test_USMarket_Schedules(unittest.TestCase):

    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.market_close_time = time(hour=16, minute=0)

        self.monday = date(year=2022, month=2, day=7)
        self.friday = date(year=2022, month=2, day=11)

    def test_GetTimestampCurrentInterval_open(self):
        intervals = []
        intervals.append(Interval.Mins1)
        intervals.append(Interval.Mins2)
        intervals.append(Interval.Mins5)
        intervals.append(Interval.Mins15)
        intervals.append(Interval.Mins30)
        intervals.append(Interval.Mins60)
        intervals.append(Interval.Mins90)
        intervals.append(Interval.Hours1)
        times = []
        times.append(time(hour=9, minute=30))
        times.append(time(hour=15, minute=30))
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = intervalToTimedelta[interval]
            for weekday in range(5):
                for t in times:
                    ## dt at start of interval:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    intervalRange = GetTimestampCurrentInterval(self.exchange, dt, interval)
                    answer = {"interval_open":datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)}
                    answer["interval_close"] = answer["interval_open"]+interval_td
                    if answer["interval_close"].time() > self.market_close_time:
                        answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

                    ## dt in middle of interval:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    dt += interval_td * 0.5
                    market_close_dt = datetime.combine(dt.date(), self.market_close_time, self.market_tz)
                    if dt >= market_close_dt:
                        dt = market_close_dt - timedelta(minutes=1)
                    intervalRange = GetTimestampCurrentInterval(self.exchange, dt, interval)
                    answer = {"interval_open":datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)}
                    answer["interval_close"] = answer["interval_open"]+interval_td
                    if answer["interval_close"].time() > self.market_close_time:
                        answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

        intervals = []
        intervals.append(Interval.Days1)
        intervals.append(Interval.Days5)
        intervals.append(Interval.Week)
        times = []
        times.append(time(hour=9, minute=30))
        times.append(time(hour=15, minute=30))
        for i in range(len(intervals)):
            interval = intervals[i]
            for weekday in range(5):
                for t in times:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    intervalRange = GetTimestampCurrentInterval(self.exchange, dt, interval)
                    if interval == Interval.Days1:
                        answer = {"interval_open":datetime.combine(self.monday+timedelta(days=weekday), time(hour=9, minute=30), self.market_tz)}
                        answer["interval_close"] = datetime.combine(self.monday+timedelta(days=weekday), time(hour=16), self.market_tz)
                    else:
                        answer = {"interval_open":datetime.combine(self.monday, time(hour=9, minute=30), self.market_tz)}
                        answer["interval_close"] = datetime.combine(self.friday, time(hour=16), self.market_tz)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

    def test_GetTimestampCurrentInterval_closed(self):
        answer = None

        ## Before/after market hours
        times = []
        times.append(time(hour=9, minute=29))
        times.append(time(hour=16, minute=0))
        intervals = []
        intervals.append(Interval.Mins1)
        intervals.append(Interval.Mins2)
        intervals.append(Interval.Mins5)
        intervals.append(Interval.Mins15)
        intervals.append(Interval.Mins30)
        intervals.append(Interval.Mins60)
        intervals.append(Interval.Mins90)
        intervals.append(Interval.Hours1)
        intervals.append(Interval.Days1)
        for i in range(len(intervals)):
            interval = intervals[i]
            for weekday in [0,1,2,3,4]:
                for t in times:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    intervalRange = GetTimestampCurrentInterval(self.exchange, dt, interval)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

        ## Weekend, at times that would be open if weekday
        times = []
        times.append(time(hour=9, minute=30))
        times.append(time(hour=15, minute=30))
        intervals = []
        intervals.append(Interval.Mins1)
        intervals.append(Interval.Mins2)
        intervals.append(Interval.Mins5)
        intervals.append(Interval.Mins15)
        intervals.append(Interval.Mins30)
        intervals.append(Interval.Mins60)
        intervals.append(Interval.Mins90)
        intervals.append(Interval.Hours1)
        intervals.append(Interval.Days1)
        for i in range(len(intervals)):
            interval = intervals[i]
            for weekday in [5,6]:
                for t in times:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    intervalRange = GetTimestampCurrentInterval(self.exchange, dt, interval)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

        ## Handle week-intervals separately
        intervals = []
        intervals.append(Interval.Days5)
        intervals.append(Interval.Week)
        ## 1) - Between Friday close and next Monday open:
        dates = []
        dates.append(datetime.combine(self.friday, time(hour=16, minute=0), self.market_tz))
        dates.append(datetime.combine(self.friday+timedelta(days=1), time(hour=10, minute=0), self.market_tz))
        dates.append(datetime.combine(self.friday+timedelta(days=2), time(hour=10, minute=0), self.market_tz))
        dates.append(datetime.combine(self.friday+timedelta(days=3), time(hour=9, minute=0), self.market_tz))
        for i in range(len(intervals)):
            interval = intervals[i]
            for dt in dates:
                intervalRange = GetTimestampCurrentInterval(self.exchange, dt, interval)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0}".format(dt))
                    print("intervalRange:")
                    pprint(intervalRange)
                    print("answer:")
                    pprint(answer)
                    raise
        ## 2) - Between Monday open and Friday close:
        answer = {}
        answer["interval_open"]  = datetime.combine(self.monday, time(hour=9, minute=30), self.market_tz)
        answer["interval_close"] = datetime.combine(self.friday, time(hour=16, minute=0), self.market_tz)
        dates = []
        dates.append(datetime.combine(self.monday, time(hour=9, minute=30), self.market_tz))
        dates.append(datetime.combine(self.monday, time(hour=20, minute=0), self.market_tz))
        for i in range(len(intervals)):
            interval = intervals[i]
            for dt in dates:
                intervalRange = GetTimestampCurrentInterval(self.exchange, dt, interval)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0}".format(dt))
                    print("intervalRange:")
                    pprint(intervalRange)
                    print("answer:")
                    pprint(answer)
                    raise

    def test_GetTimestampMostRecentInterval_closed(self):
        ## Only test when market closed now, because if open then 
        ## GetTimestampMostRecentInterval() = GetTimestampCurrentInterval()

        t = time(hour=9, minute=29)
        intervals = []
        intervals.append(Interval.Mins1)
        intervals.append(Interval.Mins2)
        intervals.append(Interval.Mins5)
        intervals.append(Interval.Mins15)
        intervals.append(Interval.Mins30)
        intervals.append(Interval.Mins60)
        intervals.append(Interval.Mins90)
        intervals.append(Interval.Hours1)
        for weekday in range(7):
            dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
            answer_day = dt.date() - timedelta(days=1)
            if answer_day.weekday() == 6:
                answer_day -= timedelta(days=2)
            elif answer_day.weekday() == 5:
                answer_day -= timedelta(days=1)

            for i in range(len(intervals)):
                interval = intervals[i]
                interval_td = intervalToTimedelta[interval]
                answer = {"interval_close":datetime.combine(answer_day, time(16, 0), self.market_tz)}
                answer["interval_open"] = answer["interval_close"] - interval_td

                intervalRange = GetTimestampMostRecentInterval(self.exchange, dt, interval)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0} (weekday={1})".format(dt, dt.weekday()))
                    print("intervalRange:")
                    pprint(intervalRange)
                    print("answer:")
                    pprint(answer)
                    raise
        t = time(hour=16, minute=0)
        intervals = []
        intervals.append(Interval.Mins1)
        intervals.append(Interval.Mins2)
        intervals.append(Interval.Mins5)
        intervals.append(Interval.Mins15)
        intervals.append(Interval.Mins30)
        intervals.append(Interval.Mins60)
        intervals.append(Interval.Mins90)
        intervals.append(Interval.Hours1)
        for weekday in range(7):
            dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
            answer_day = dt.date()
            if answer_day.weekday() == 6:
                answer_day -= timedelta(days=2)
            elif answer_day.weekday() == 5:
                answer_day -= timedelta(days=1)

            for i in range(len(intervals)):
                interval = intervals[i]
                interval_td = intervalToTimedelta[interval]
                answer = {"interval_close":datetime.combine(answer_day, time(16, 0), self.market_tz)}
                answer["interval_open"] = answer["interval_close"] - interval_td

                intervalRange = GetTimestampMostRecentInterval(self.exchange, dt, interval)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0} (weekday={1})".format(dt, dt.weekday()))
                    print("intervalRange:")
                    pprint(intervalRange)
                    print("answer:")
                    pprint(answer)
                    raise

        t = time(hour=9, minute=29)
        interval = Interval.Days1
        for weekday in range(7):
            dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
            answer_day = dt.date() - timedelta(days=1)
            if answer_day.weekday() == 6:
                answer_day -= timedelta(days=2)
            elif answer_day.weekday() == 5:
                answer_day -= timedelta(days=1)

            answer = {}
            answer["interval_open"]  = datetime.combine(answer_day, time(9, 30), self.market_tz)
            answer["interval_close"] = datetime.combine(answer_day, time(16, 0), self.market_tz)

            intervalRange = GetTimestampMostRecentInterval(self.exchange, dt, interval)
            try:
                self.assertEqual(intervalRange, answer)
            except:
                print("interval = {0}".format(interval))
                print("dt = {0} (weekday={1})".format(dt, dt.weekday()))
                print("intervalRange:")
                pprint(intervalRange)
                print("answer:")
                pprint(answer)
                raise
        t = time(hour=16, minute=00)
        interval = Interval.Days1
        for weekday in range(7):
            dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
            answer_day = dt.date()
            if answer_day.weekday() == 6:
                answer_day -= timedelta(days=2)
            elif answer_day.weekday() == 5:
                answer_day -= timedelta(days=1)

            answer = {}
            answer["interval_open"]  = datetime.combine(answer_day, time(9, 30), self.market_tz)
            answer["interval_close"] = datetime.combine(answer_day, time(16, 0), self.market_tz)

            intervalRange = GetTimestampMostRecentInterval(self.exchange, dt, interval)
            try:
                self.assertEqual(intervalRange, answer)
            except:
                print("interval = {0}".format(interval))
                print("dt = {0} (weekday={1})".format(dt, dt.weekday()))
                print("intervalRange:")
                pprint(intervalRange)
                print("answer:")
                pprint(answer)
                raise

        ## Week-intervals
        ## Check that between Friday close and Monday open returns last week:
        intervals = [Interval.Days5, Interval.Week]
        dates = []
        dates.append(datetime.combine(self.friday,                   time(hour=17, minute=0), self.market_tz))
        dates.append(datetime.combine(self.friday+timedelta(days=1), time(hour=12, minute=0), self.market_tz))
        dates.append(datetime.combine(self.friday+timedelta(days=2), time(hour=12, minute=0), self.market_tz))
        dates.append(datetime.combine(self.friday+timedelta(days=3), time(hour=9, minute=29), self.market_tz))
        for dt in dates:
            answer_week_start_day = self.monday
            answer_week_end_day = self.monday + timedelta(days=4)
            answer = {}
            answer["interval_open"]  = datetime.combine(answer_week_start_day, time(9, 30), self.market_tz)
            answer["interval_close"] = datetime.combine(answer_week_end_day,   time(16, 0), self.market_tz)

            for interval in intervals:
                intervalRange = GetTimestampMostRecentInterval(self.exchange, dt, interval)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0} (weekday={1})".format(dt, dt.weekday()))
                    print("intervalRange:")
                    pprint(intervalRange)
                    print("answer:")
                    pprint(answer)
                    raise

    def test_GetTimestampNextInterval_open(self):
        ## If during day session, next interval is in same session:
        intervals = []
        intervals.append(Interval.Mins1)
        intervals.append(Interval.Mins2)
        intervals.append(Interval.Mins5)
        intervals.append(Interval.Mins15)
        intervals.append(Interval.Mins30)
        intervals.append(Interval.Mins60)
        intervals.append(Interval.Hours1)
        times = []
        times.append(time(hour=9, minute=30))
        times.append(time(hour=13, minute=30))
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = intervalToTimedelta[interval]
            for weekday in range(5):
                for t in times:
                    ## dt at start of interval:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    intervalRange = GetTimestampNextInterval(self.exchange, dt, interval)
                    answer = {}
                    answer["interval_open"] = dt+interval_td
                    answer["interval_close"] = answer["interval_open"]+interval_td
                    if answer["interval_close"].time() > self.market_close_time:
                        answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

                    ## dt in middle of interval:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    dt += interval_td * 0.5
                    market_close_dt = datetime.combine(dt.date(), self.market_close_time, self.market_tz)
                    if dt >= market_close_dt:
                        dt = market_close_dt - timedelta(minutes=1)
                    intervalRange = GetTimestampNextInterval(self.exchange, dt, interval)
                    answer = {}
                    answer["interval_open"] = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz) +interval_td
                    answer["interval_close"] = answer["interval_open"]+interval_td
                    if answer["interval_close"].time() > self.market_close_time:
                        answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise
        interval = Interval.Mins90
        times = []
        answers = []
        times.append(time(hour=9, minute=30))  ; answers.append({"interval_open":time(hour=11, minute=0), "interval_close":time(hour=12, minute=30)})
        times.append(time(hour=9, minute=45))  ; answers.append({"interval_open":time(hour=11, minute=0), "interval_close":time(hour=12, minute=30)})
        times.append(time(hour=13, minute=30)) ; answers.append({"interval_open":time(hour=14, minute=0), "interval_close":time(hour=15, minute=30)})
        times.append(time(hour=13, minute=45)) ; answers.append({"interval_open":time(hour=14, minute=0), "interval_close":time(hour=15, minute=30)})
        for i in range(len(times)):
            t = times[i]
            answer_t = answers[i]
            for weekday in range(5):
                dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                intervalRange = GetTimestampNextInterval(self.exchange, dt, interval)
                answer = {}
                answer["interval_open"]  = datetime.combine(self.monday+timedelta(days=weekday), answer_t["interval_open"], self.market_tz)
                answer["interval_close"] = datetime.combine(self.monday+timedelta(days=weekday), answer_t["interval_close"], self.market_tz)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0}".format(dt))
                    print("intervalRange:")
                    pprint(intervalRange)
                    print("answer:")
                    pprint(answer)
                    raise

        # If during the final interval of session, next interval is next day first interval
        intervals = []
        intervals.append(Interval.Mins1)
        intervals.append(Interval.Mins2)
        intervals.append(Interval.Mins5)
        intervals.append(Interval.Mins15)
        intervals.append(Interval.Mins30)
        intervals.append(Interval.Mins60)
        intervals.append(Interval.Hours1)
        intervals.append(Interval.Mins90)
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = intervalToTimedelta[interval]

            times = []
            times.append(time(hour=15, minute=59))
            m = min(15, interval_td.seconds//60//2)
            times.append(time(hour=15, minute=59-m))
            for weekday in range(5):
                for t in times:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    intervalRange = GetTimestampNextInterval(self.exchange, dt, interval)

                    next_day = dt.date() + timedelta(days=1)
                    if next_day.weekday() == 5:
                        next_day += timedelta(days=2)
                    elif next_day.weekday() == 6:
                        next_day += timedelta(days=1)
                    answer = {}
                    answer["interval_open"] = datetime.combine(next_day, time(hour=9, minute=30), self.market_tz)
                    answer["interval_close"] = answer["interval_open"]+interval_td
                    if answer["interval_close"].time() > self.market_close_time:
                        answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise

        # If a day/week interval, is next working day/week regardless of today
        intervals = []
        intervals.append(Interval.Days1)
        intervals.append(Interval.Days5)
        intervals.append(Interval.Week)
        times = []
        times.append(time(hour=9, minute=30))
        times.append(time(hour=15, minute=30))
        for i in range(len(intervals)):
            interval = intervals[i]
            for weekday in range(4):
                for t in times:
                    dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                    intervalRange = GetTimestampNextInterval(self.exchange, dt, interval)
                    answer = {}
                    if interval == Interval.Days1:
                        # Next day
                        answer["interval_open"]  = datetime.combine(self.monday+timedelta(days=weekday+1), time(hour=9, minute=30), self.market_tz)
                        answer["interval_close"] = datetime.combine(self.monday+timedelta(days=weekday+1), time(hour=16), self.market_tz)
                    else:
                        # Next week
                        answer["interval_open"]  = datetime.combine(self.monday+timedelta(days=7), time(hour=9, minute=30), self.market_tz)
                        answer["interval_close"] = datetime.combine(self.friday+timedelta(days=7), time(hour=16), self.market_tz)
                    try:
                        self.assertEqual(intervalRange, answer)
                    except:
                        print("interval = {0}".format(interval))
                        print("dt = {0}".format(dt))
                        print("intervalRange:")
                        pprint(intervalRange)
                        print("answer:")
                        pprint(answer)
                        raise
            weekday = 5
            dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
            intervalRange = GetTimestampNextInterval(self.exchange, dt, interval)
            answer = {}
            answer["interval_open"]  = datetime.combine(self.monday+timedelta(days=7), time(hour=9, minute=30), self.market_tz)
            if interval == Interval.Days1:
                answer["interval_close"] = datetime.combine(self.monday+timedelta(days=7), time(hour=16), self.market_tz)
            else:
                answer["interval_close"] = datetime.combine(self.friday+timedelta(days=7), time(hour=16), self.market_tz)
            try:
                self.assertEqual(intervalRange, answer)
            except:
                print("interval = {0}".format(interval))
                print("dt = {0}".format(dt))
                print("intervalRange = {0}".format(intervalRange))
                print("answer = {0}".format(answer))
                raise

    def test_GetTimestampNextInterval_closed(self):
        ## If in morning before market open, next interval next session first interval:
        intervals = []
        intervals.append(Interval.Mins1)
        intervals.append(Interval.Mins2)
        intervals.append(Interval.Mins5)
        intervals.append(Interval.Mins15)
        intervals.append(Interval.Mins30)
        intervals.append(Interval.Mins60)
        intervals.append(Interval.Mins90)
        intervals.append(Interval.Hours1)
        intervals.append(Interval.Days1)
        t = time(hour=9, minute=0)
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = intervalToTimedelta[interval]
            for weekday in range(5):
                dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                intervalRange = GetTimestampNextInterval(self.exchange, dt, interval)

                answer_day = dt.date()
                answer = {}
                answer["interval_open"]  = datetime.combine(answer_day, time(hour=9, minute=30), self.market_tz)
                if interval == Interval.Days1:
                    answer["interval_close"] = datetime.combine(answer_day, time(hour=16, minute=0), self.market_tz)
                else:
                    answer["interval_close"] = answer["interval_open"]+interval_td
                    if answer["interval_close"].time() > self.market_close_time:
                        answer["interval_close"] = datetime.combine(answer["interval_close"].date(), self.market_close_time, self.market_tz)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0}".format(dt))
                    print("intervalRange:")
                    pprint(intervalRange)
                    print("answer:")
                    pprint(answer)
                    raise

        ## If in afternoon after market close, next interval is next session first interval:
        intervals = []
        intervals.append(Interval.Mins1)
        intervals.append(Interval.Mins2)
        intervals.append(Interval.Mins5)
        intervals.append(Interval.Mins15)
        intervals.append(Interval.Mins30)
        intervals.append(Interval.Mins60)
        intervals.append(Interval.Mins90)
        intervals.append(Interval.Hours1)
        intervals.append(Interval.Days1)
        t = time(hour=16, minute=0)
        for i in range(len(intervals)):
            interval = intervals[i]
            interval_td = intervalToTimedelta[interval]
            for weekday in range(5):
                dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                intervalRange = GetTimestampNextInterval(self.exchange, dt, interval)

                answer_day = dt.date()+timedelta(days=1)
                if answer_day.weekday() == 5:
                    answer_day += timedelta(days=2)
                elif answer_day.weekday() == 6:
                    answer_day += timedelta(days=1)
                answer = {}
                answer["interval_open"]  = datetime.combine(answer_day, time(hour=9, minute=30), self.market_tz)
                if interval == Interval.Days1:
                    answer["interval_close"] = datetime.combine(answer_day, time(hour=16, minute=0), self.market_tz)
                else:
                    answer["interval_close"] = answer["interval_open"]+interval_td
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0}".format(dt))
                    print("intervalRange:")
                    pprint(intervalRange)
                    print("answer:")
                    pprint(answer)
                    raise

        # If a week interval, is next week regardless of today
        intervals = []
        intervals.append(Interval.Days5)
        intervals.append(Interval.Week)
        dates = []
        dates.append(datetime.combine(self.friday,                   time(hour=16, minute=0), self.market_tz))
        dates.append(datetime.combine(self.friday+timedelta(days=1), time(hour=12, minute=0), self.market_tz))
        dates.append(datetime.combine(self.friday+timedelta(days=2), time(hour=12, minute=0), self.market_tz))
        dates.append(datetime.combine(self.friday+timedelta(days=3), time(hour=9,  minute=0), self.market_tz))
        for i in range(len(intervals)):
            interval = intervals[i]
            for dt in dates:
                dt = datetime.combine(self.monday+timedelta(days=weekday), t, self.market_tz)
                intervalRange = GetTimestampNextInterval(self.exchange, dt, interval)
                answer = {}
                answer["interval_open"]  = datetime.combine(self.monday+timedelta(days=7), time(hour=9, minute=30), self.market_tz)
                answer["interval_close"] = datetime.combine(self.friday+timedelta(days=7), time(hour=16, minute=0), self.market_tz)
                try:
                    self.assertEqual(intervalRange, answer)
                except:
                    print("interval = {0}".format(interval))
                    print("dt = {0}".format(dt))
                    print("intervalRange:")
                    pprint(intervalRange)
                    print("answer:")
                    pprint(answer)
                    raise

if __name__ == '__main__':
    unittest.main()
