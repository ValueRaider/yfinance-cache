import unittest

import os, shutil, tempfile
import json, pickle

import sys
sys.path.append("/home/gonzo/Repos/yfinance-cache/src/yfinance_cache")
import yfc_ticker as yfc
import yfc_cache_manager as yfcm
from yfc_utils import *

from time import sleep
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from pprint import pprint

class Test_Yfc_Cache(unittest.TestCase):

    def setUp(self):
        self.tempCacheDir = tempfile.TemporaryDirectory()
        yfcm.SetCacheDirpath(self.tempCacheDir.name)


    def tearDown(self):
        self.tempCacheDir.cleanup()


    def test_cache_store(self):
        cat = "test1"
        var = "value"
        value = 123

        ft = "json"

        yfcm.StoreCacheDatum(cat, var, value)
        fp = os.path.join(yfcm.GetCacheDirpath(), cat, var+"."+ft)
        try:
            self.assertTrue(os.path.isfile(fp))
        except:
            print("Does not exist: "+fp)
            raise

        obj = yfcm.ReadCacheDatum(cat, var)
        self.assertEqual(obj, value)

        with open(fp, 'r') as inData:
            js = json.load(inData, object_hook=JsonDecodeDict)
            md = js["metadata"]
            self.assertEqual(js["data"], value)
            self.assertIsNotNone(js["metadata"], "LastWrite")


    def test_cache_store_expiry(self):
        cat = "test1"
        var = "value"
        value = 123

        ft = "json"

        dt = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        exp = dt + timedelta(seconds=1)

        yfcm.StoreCacheDatum(cat, var, value, expiry=exp)
        fp = os.path.join(yfcm.GetCacheDirpath(), cat, var+"."+ft)
        self.assertTrue(os.path.isfile(fp))

        obj = yfcm.ReadCacheDatum(cat, var)
        self.assertEqual(obj, value)

        with open(fp, 'r') as inData:
            js = json.load(inData, object_hook=JsonDecodeDict)

        sleep(1)
        obj = yfcm.ReadCacheDatum(cat, var)
        self.assertIsNone(obj)
        self.assertFalse(os.path.isfile(fp))


    def test_cache_store_packed(self):
        cat = "test1"
        var = "balance_sheet"
        var_grp = "annuals"
        value = 123

        ft = "pkl"

        yfcm.StoreCachePackedDatum(cat, var, value)
        fp = os.path.join(yfcm.GetCacheDirpath(), cat, var_grp+"."+ft)
        try:
            self.assertTrue(os.path.isfile(fp))
        except:
            print("Does not exist: "+fp)
            raise

        obj = yfcm.ReadCachePackedDatum(cat, var)
        self.assertEqual(obj, value)


    def test_cache_store_packed_expiry(self):
        cat = "test1"
        var = "balance_sheet"
        var_grp = "annuals"
        value = 123

        ft = "pkl"

        dt = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        exp = dt + timedelta(seconds=1)

        yfcm.StoreCachePackedDatum(cat, var, value, expiry=exp)
        fp = os.path.join(yfcm.GetCacheDirpath(), cat, var_grp+"."+ft)
        self.assertTrue(os.path.isfile(fp))

        obj = yfcm.ReadCachePackedDatum(cat, var)
        self.assertEqual(obj, value)

        ft = os.path.join(yfcm.GetCacheDirpath(), cat, var+".metadata")
        self.assertEqual(ft, ft)

        sleep(1)
        # sleep(2)
        obj = yfcm.ReadCachePackedDatum(cat, var)
        self.assertIsNone(obj)
        with open(fp, 'rb') as inData:
            pkl = pickle.load(inData)
            self.assertFalse(cat in pkl.keys())


if __name__ == '__main__':
    unittest.main()
