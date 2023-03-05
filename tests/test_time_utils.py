import unittest

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

import pandas as pd
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

class TestTimeUtils(unittest.TestCase):

    def setUp(self):
        self.day = date(year=2022, month=1, day=1)
        self.week_start = date(year=2022, month=1, day=3)

        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.exchangeOpenTime = time(hour=9, minute=30)
        self.exchangeCloseTime = time(hour=16, minute=0)

    def test_ConvertToDatetime(self):
        zi_utc = ZoneInfo("UTC")
        zi_usa = ZoneInfo("US/Eastern")

        dt1 = datetime.combine(date(2022, 2, 8), time(14, 30), zi_utc)
        # dt1 = dt1.astimezone(zi_usa)
        pdt = pd.Timestamp(dt1)
        
        dt2 = yfct.ConvertToDatetime(pdt, tz=zi_usa)
        try:
            self.assertEqual(dt2.astimezone(zi_utc), dt1)
        except:
            print(dt1)
            print(dt2)
            raise

    def test_DateIntervalIndex_getIndexer(self):
        mondays = [date(2022, 5, d) for d in [2, 9, 16, 23, 30]]
        satdays = [m + timedelta(days=5) for m in mondays]
        dii = yfcd.DateIntervalIndex.from_arrays(mondays, satdays, closed="left")

        week0_days = [date(2022, 5, 1)]
        week1_days = [date(2022, 5, d) for d in range(2, 7)]
        week2_days = [d + timedelta(days=7) for d in week1_days]
        week3_days = [d + timedelta(days=7) for d in week2_days]
        week4_days = [d + timedelta(days=7) for d in week3_days]
        week5_days = [d + timedelta(days=7) for d in week4_days]
        week6_days = [d + timedelta(days=7) for d in week5_days]

        idx = dii.get_indexer(week0_days)
        self.assertEqual(idx, [-1])  # pass

        idx = dii.get_indexer(week1_days)
        self.assertEqual(list(idx), [0]*len(week1_days))

        idx = dii.get_indexer(week2_days)
        self.assertEqual(list(idx), [1]*len(week2_days))

        idx = dii.get_indexer(week3_days)
        self.assertEqual(list(idx), [2]*len(week3_days))

        idx = dii.get_indexer(week4_days)
        self.assertEqual(list(idx), [3]*len(week4_days))

        idx = dii.get_indexer(week5_days)
        self.assertEqual(list(idx), [4]*len(week5_days))

        idx = dii.get_indexer(week6_days)
        self.assertEqual(list(idx), [-1]*len(week6_days))

if __name__ == '__main__':
    unittest.main()
