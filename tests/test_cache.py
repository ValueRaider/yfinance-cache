import unittest

from .context import yfc_cache_manager as yfcm
from .context import yfc_dat as yfcd
from .context import yfc_utils as yfcu

import os, shutil, tempfile
import json, pickle

from time import sleep
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
import pandas as pd

from pprint import pprint

class Test_Yfc_Cache(unittest.TestCase):

    def setUp(self):
        self.ticker = "INTC"
        self.objName = "example"

        self.tempCacheDir = tempfile.TemporaryDirectory()
        yfcm.SetCacheDirpath(self.tempCacheDir.name)


    def tearDown(self):
        self.tempCacheDir.cleanup()


    def testGetFilepath(self):
        expected = os.path.join(self.tempCacheDir.name, self.ticker, "info.json")
        fp = yfcm.GetFilepath(self.ticker, "info", {})
        self.assertEqual(fp, expected)


    def test_cache_read_nothing(self):
        obj = yfcm.ReadCacheDatum(self.ticker, self.objName)
        self.assertIsNone(obj)

        obj,md = yfcm.ReadCacheDatum(self.ticker, self.objName, return_metadata_too=True)
        self.assertIsNone(obj)
        self.assertIsNone(md)


    def test_cache_store(self):
        value = 123

        self.assertFalse(yfcm.IsDatumCached(self.ticker, self.objName))

        yfcm.StoreCacheDatum(self.ticker, self.objName, value)

        # Confirm write
        fp = os.path.join(yfcm.GetCacheDirpath(), self.ticker, self.objName+".json")
        self.assertTrue(os.path.isfile(fp))
        self.assertTrue(yfcm.IsDatumCached(self.ticker, self.objName))

        # Confirm value
        with open(fp, 'r') as inData:
            js = json.load(inData, object_hook=yfcu.JsonDecodeDict)
            self.assertEqual(js["data"], value)
        obj = yfcm.ReadCacheDatum(self.ticker, self.objName)
        self.assertEqual(obj, value)



    def test_cache_store_expiry(self):
        value = 123

        dt = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        exp = dt + timedelta(seconds=1)

        yfcm.StoreCacheDatum(self.ticker, self.objName, value, expiry=exp)

        # Confirm write
        fp = os.path.join(yfcm.GetCacheDirpath(), self.ticker, self.objName+".json")
        self.assertTrue(os.path.isfile(fp))
        self.assertTrue(yfcm.IsDatumCached(self.ticker, self.objName))

        # Confirm value
        obj = yfcm.ReadCacheDatum(self.ticker, self.objName)
        self.assertEqual(obj, value)
        with open(fp, 'r') as inData:
            js = json.load(inData, object_hook=yfcu.JsonDecodeDict)
            self.assertEqual(js["data"], value)

        # Confirm expiry
        sleep(1)
        obj = yfcm.ReadCacheDatum(self.ticker, self.objName)
        self.assertIsNone(obj)
        self.assertFalse(os.path.isfile(fp))


    def test_cache_store_packed(self):
        var_grp = "annuals"
        var1 = "balance_sheet"
        val1 = 123
        var2 = "cashflow"
        val2 = 456

        yfcm.StoreCachePackedDatum(self.ticker, var1, val1)
        yfcm.StoreCachePackedDatum(self.ticker, var2, val2)

        # Confirm write
        fp = os.path.join(yfcm.GetCacheDirpath(), self.ticker, var_grp+".pkl")
        self.assertTrue(os.path.isfile(fp))
        self.assertTrue(yfcm.IsDatumCached(self.ticker, var1))
        self.assertTrue(yfcm.IsDatumCached(self.ticker, var2))

        # Confirm value
        obj = yfcm.ReadCachePackedDatum(self.ticker, var1)
        self.assertEqual(obj, val1)
        obj = yfcm.ReadCachePackedDatum(self.ticker, var2)
        self.assertEqual(obj, val2)


    def test_cache_store_packed_expiry(self):
        var = "balance_sheet"
        var_grp = "annuals"
        value = 123

        dt = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        exp = dt + timedelta(seconds=1)

        yfcm.StoreCachePackedDatum(self.ticker, var, value, expiry=exp)

        # Confirm write
        fp = os.path.join(yfcm.GetCacheDirpath(), self.ticker, var_grp+".pkl")
        self.assertTrue(os.path.isfile(fp))
        self.assertTrue(yfcm.IsDatumCached(self.ticker, var))

        # Confirm value
        obj = yfcm.ReadCachePackedDatum(self.ticker, var)
        self.assertEqual(obj, value)

        # Confirm expiry
        sleep(1)
        # sleep(2)
        obj = yfcm.ReadCachePackedDatum(self.ticker, var)
        self.assertIsNone(obj)
        with open(fp, 'rb') as inData:
            pkl = pickle.load(inData)
            self.assertFalse(self.ticker in pkl.keys())


    def test_cache_store_types(self):
        json_values = []
        json_values.append(int(1))
        json_values.append(float(1))
        json_values.append([1, 3])
        json_values.append(pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC")))
        json_values.append(timedelta(seconds=2.01))
        json_values.append({'a':1, 'b':2})

        pkl_values = []
        pkl_values.append(set([1, 3]))

        for value in json_values+pkl_values:
            ext = "json" if value in json_values else "pkl"
            fp = os.path.join(yfcm.GetCacheDirpath(), self.ticker, self.objName+"."+ext)

            # Confirm write
            yfcm.StoreCacheDatum(self.ticker, self.objName, value)
            self.assertTrue(os.path.isfile(fp))

            # Confirm value
            obj = yfcm.ReadCacheDatum(self.ticker, self.objName)
            self.assertEqual(obj, value)


    def test_cache_metadata_write1(self):
        key = "k1"
        value = 123
        md = {key:value}

        yfcm.StoreCacheDatum(self.ticker, self.objName, value, metadata=md) 
        obj,mdc = yfcm.ReadCacheDatum(self.ticker, self.objName, return_metadata_too=True)
        self.assertEqual(obj, value)
        self.assertEqual(md, mdc)

    def test_cache_metadata_write2(self):
        key = "k1"
        value = 123
        md = {key:value}

        yfcm.StoreCacheDatum(self.ticker, self.objName, value)
        yfcm.WriteCacheMetadata(self.ticker, self.objName, key, value)

        obj,mdc = yfcm.ReadCacheDatum(self.ticker, self.objName, return_metadata_too=True)
        self.assertEqual(obj, value)
        self.assertEqual(md, mdc)


    def test_cache_metadata_write_expiry(self):
        key = "k1"
        value = 123
        md = {key:value}
        exp = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC")) + timedelta(hours=1)

        yfcm.StoreCacheDatum(self.ticker, self.objName, value, expiry=exp, metadata=md)
        obj,mdc = yfcm.ReadCacheDatum(self.ticker, self.objName, return_metadata_too=True)
        md["__expiry__"] = exp
        self.assertEqual(obj, value)
        self.assertEqual(md, mdc)


    def test_cache_metadata_overwrite1(self):
        value = 123
        md1 = {"md1":456}
        md2 = {"md2":345}
        yfcm.StoreCacheDatum(self.ticker, self.objName, value, metadata=md1)
        yfcm.StoreCacheDatum(self.ticker, self.objName, value, metadata=md2)
        obj,mdc = yfcm.ReadCacheDatum(self.ticker, self.objName, return_metadata_too=True)
        self.assertEqual(obj, value)
        self.assertEqual(md2, mdc)

    def test_cache_metadata_overwrite2(self):
        value = 123
        key = "md1"
        val1 = 456
        val2 = 345
        yfcm.StoreCacheDatum(self.ticker, self.objName, value)
        yfcm.WriteCacheMetadata(self.ticker, self.objName, key, val1)
        yfcm.WriteCacheMetadata(self.ticker, self.objName, key, val2)
        obj,mdc = yfcm.ReadCacheDatum(self.ticker, self.objName, return_metadata_too=True)
        self.assertEqual(obj, value)
        self.assertEqual(mdc, {key:val2})


    def test_cache_metadata_packed_overwrite1(self):
        var = "balance_sheet"
        value = 123
        md1 = {"md1":456}
        md2 = {"md2":345}
        yfcm.StoreCacheDatum(self.ticker, var, value, metadata=md1)
        yfcm.StoreCacheDatum(self.ticker, var, value, metadata=md2)
        obj,mdc = yfcm.ReadCacheDatum(self.ticker, var, return_metadata_too=True)
        self.assertEqual(obj, value)
        self.assertEqual(md2, mdc)

    def test_cache_metadata_packed_overwrite2(self):
        var = "balance_sheet"
        value = 123
        key = "md1"
        val1 = 123
        val2 = 345
        yfcm.StoreCacheDatum(self.ticker, var, value)
        yfcm.WriteCacheMetadata(self.ticker, var, key, val1)
        yfcm.WriteCacheMetadata(self.ticker, var, key, val2)
        obj,mdc = yfcm.ReadCacheDatum(self.ticker, var, return_metadata_too=True)
        self.assertEqual(obj, value)
        self.assertEqual(mdc, {key:val2})

if __name__ == '__main__':
    unittest.main()
