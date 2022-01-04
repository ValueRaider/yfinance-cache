import unittest

import datetime, pytz

class TestDatetimeAssumptions(unittest.TestCase):

    def test_dt_equality(self):
        dt1 = datetime.datetime(year=2021, month=1, day=1)
        dt2 = datetime.datetime(year=2021, month=1, day=1)
        self.assertEqual(dt1, dt2)

        dt1 = datetime.datetime(year=2021, month=1, day=1)
        dt2 = datetime.datetime(year=2021, month=1, day=1, tzinfo=pytz.timezone('US/Eastern'))
        self.assertNotEqual(dt1, dt2)

        dt1 = datetime.datetime(year=2021, month=1, day=1, tzinfo=pytz.timezone('GMT'))
        dt2 = datetime.datetime(year=2021, month=1, day=1, tzinfo=pytz.timezone('US/Eastern'))
        self.assertNotEqual(dt1, dt2)

        dt1 = datetime.datetime(year=2021, month=1, day=1)
        dt1 = dt1.replace(tzinfo=pytz.timezone('US/Eastern'))
        dt2 = datetime.datetime(year=2021, month=1, day=1, tzinfo=pytz.timezone('US/Eastern'))
        self.assertEqual(dt1, dt2)

if __name__ == '__main__':
    unittest.main()
