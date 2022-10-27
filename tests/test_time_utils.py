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

if __name__ == '__main__':
    unittest.main()
