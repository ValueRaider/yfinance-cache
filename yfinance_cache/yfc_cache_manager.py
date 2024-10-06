import os
import pickle
import json
import appdirs
import pandas as pd

from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from . import yfc_dat as yfcd
from . import yfc_utils as yfcu

# To reduce #files in cache, store some YF objects together into same file (including metadata)
packed_data_cats = {}
packed_data_cats["quarterlys"] = ["quarterly_balance_sheet", "quarterly_cashflow", "quarterly_earnings", "quarterly_financials", "quarterly_income_stmt"]
packed_data_cats["annuals"]    = ["balance_sheet", "cashflow", "earnings", "financials", "income_stmt"]

quarterly_objects = packed_data_cats["quarterlys"]
annual_objects    = packed_data_cats["annuals"]

verbose = False
# verbose = True

# TODO: store DataFrames as CSV, don't need the metadata

global cacheDirpath


def GetCacheDirpath():
    global cacheDirpath
    return cacheDirpath


def ResetCacheDirpath():
    global cacheDirpath
    cacheDirpath = os.path.join(appdirs.user_cache_dir(), "py-yfinance-cache")


def SetCacheDirpath(dp):
    global cacheDirpath
    cacheDirpath = dp
    if verbose:
        print("Set cache dir to {0}".format(cacheDirpath))

    global _option_manager
    _option_manager = OptionsManager()


def IsObjectInPackedData(objectName):
    for k in packed_data_cats.keys():
        if objectName in packed_data_cats[k]:
            return True
    return False


def GetPackedDataCat(objectName):
    for k in packed_data_cats.keys():
        if objectName in packed_data_cats[k]:
            return k
    return None


def is_json_serializable(obj):
    try:
        json.dumps(obj)
        return True
    except (TypeError, OverflowError):
        return False


def GetFilepath(ticker, objectName, obj=None, prune=False):
    if IsObjectInPackedData(objectName):
        return GetFilepathPacked(ticker, objectName)

    fp = None
    if obj is not None:
        if isinstance(obj, list) and (len(obj)==0 or isinstance(obj[0], (int, float, str, datetime, date, timedelta))):
            ext = "json"
            ext_bad = "pkl"
        elif isinstance(obj, dict) and is_json_serializable(obj):
            ext = "json"
            ext_bad = "pkl"
        elif isinstance(obj, (int, float, str, datetime, date, timedelta)):
            ext = "json"
            ext_bad = "pkl"
        else:
            ext = "pkl"
            ext_bad = "json"
        fp = os.path.join(GetCacheDirpath(), ticker, objectName) + "."+ext
        fp_bad = os.path.join(GetCacheDirpath(), ticker, objectName) + "."+ext_bad
        if os.path.isfile(fp_bad):
            if prune:
                os.remove(fp_bad)
            else:
                raise Exception("For {} object {}/{}, a {} file already exists".format(ext, ticker, objectName, ext_bad))
    else:
        fp_base = os.path.join(GetCacheDirpath(), ticker, objectName)
        json_exists = os.path.isfile(fp_base+".json")
        pkl_exists = os.path.isfile(fp_base+".pkl")
        if json_exists and pkl_exists:
            raise Exception("For cached datum '{0}', both a .json and .pkl file exist. Should only be one.".format(objectName))
        elif json_exists:
            fp = fp_base + ".json"
        elif pkl_exists:
            fp = fp_base + ".pkl"
        elif "release-dates" in objectName or "earnings_dates" in objectName:
            fp = fp_base + ".pkl"
    return fp
def GetFilepathPacked(ticker, objectName):
    if not IsObjectInPackedData(objectName):
        return GetFilepath(ticker, objectName)
    pkg = GetPackedDataCat(objectName)
    fp = os.path.join(GetCacheDirpath(), ticker, pkg) + ".pkl"
    return fp


def IsDatumCached(ticker, objectName):
    if verbose:
        print("IsDatumCached({0}, {1})".format(ticker, objectName))

    fp = GetFilepath(ticker, objectName)
    if fp is None or (not os.path.isfile(fp)):
        return False

    if IsObjectInPackedData(objectName):
        if os.path.isfile(fp):
            with open(fp, 'rb') as inData:
                packedData = pickle.load(inData)
            return objectName in packedData.keys()
    else:
        if os.path.isfile(fp):
            if os.path.getsize(fp) == 0:
                # Corrupt
                os.remove(fp)
                return False
            else:
                return True
        else:
            return False


def _ReadData(ticker, objectName):
    d = None

    fp = GetFilepath(ticker, objectName)
    if fp is None or (not os.path.isfile(fp)):
        return None

    if fp.endswith(".json"):
        with open(fp, 'r') as inData:
            d = json.load(inData, object_hook=yfcu.JsonDecodeDict)
    else:
        with open(fp, 'rb') as inData:
            d = pickle.load(inData)
        if not isinstance(d, dict):
            raise Exception("Pickled '{}/{}' data should be dict, but is {}".format(ticker, objectName, type(d)))
        if "data" not in d.keys():
            print(fp)
            raise Exception("Pickled dict missing 'data' key: {}".format(d.keys()))
    return d


def _ReadPackedData(ticker, objectName):
    d = None
    fp = GetFilepath(ticker, objectName)
    if os.path.isfile(fp):
        with open(fp, 'rb') as inData:
            d = pickle.load(inData)
        if not isinstance(d, dict):
            raise Exception("Pickled '{}/{}' packed-data should be dict, but is {}".format(ticker, objectName, type(d)))
    return d


def ReadCacheDatum(ticker, objectName, return_metadata_too=False):
    if verbose:
        print("ReadCacheDatum({0}, {1})".format(ticker, objectName))

    if IsObjectInPackedData(objectName):
        return ReadCachePackedDatum(ticker, objectName, return_metadata_too)

    data = None ; md = None
    d = _ReadData(ticker, objectName)
    if d is not None:
        data   = d["data"]
        md     = d["metadata"] if "metadata" in d else None
        expiry = d["expiry"]   if "expiry"   in d else None

        if expiry is not None:
            dtnow = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC"))
            if dtnow >= expiry:
                if verbose:
                    print("Deleting expired datum '{0}/{1}'".format(ticker, objectName))
                fp = GetFilepath(ticker, objectName)
                os.remove(fp)
                if return_metadata_too:
                    return None, None
                else:
                    return None

            if md is None:
                md = {"__expiry__": expiry}
            else:
                md["__expiry__"] = expiry

    if return_metadata_too:
        return data, md
    else:
        return data


def ReadCachePackedDatum(ticker, objectName, return_metadata_too=False):
    if verbose:
        print("ReadCachePackedDatum({0}, {1})".format(ticker, objectName))

    if not IsObjectInPackedData(objectName):
        raise Exception("Don't call packed-data function on non-packed data '{0}'".format(objectName))

    data = None ; md = None
    pkData = _ReadPackedData(ticker, objectName)
    if (pkData is not None) and (objectName in pkData):
        objData = pkData[objectName]
        data   = objData["data"]
        md     = objData["metadata"] if "metadata" in objData else None
        expiry = objData["expiry"]   if "expiry"   in objData else None

        if expiry is not None:
            dtnow = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC"))
            if dtnow >= expiry:
                if verbose:
                    print("Deleting expired packed datum '{0}/{1}'".format(ticker, objectName))
                del pkData[objectName]
                fp = GetFilepath(ticker, objectName)
                with open(fp, 'wb') as outData:
                    pickle.dump(pkData, outData, 4)
                if return_metadata_too:
                    return None, None
                else:
                    return None

            if md is None:
                md = {"__expiry__": expiry}
            else:
                md["__expiry__"] = expiry

    if return_metadata_too:
        return data, md
    else:
        return data


def StoreCacheDatum(ticker, objectName, datum, expiry=None, metadata=None):
    if verbose:
        print("StoreCacheDatum({0}, {1})".format(ticker, objectName))

    if IsObjectInPackedData(objectName):
        StoreCachePackedDatum(ticker, objectName, datum, metadata=metadata)
        return

    if (metadata is not None) and not isinstance(metadata, dict):
        raise Exception("'metadata' must be dict of scalars")
    if expiry is not None:
        if isinstance(expiry, yfcd.Interval):
            # Convert interval to actual datetime
            expiry = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC")) + yfcd.intervalToTimedelta[expiry]
        if not isinstance(expiry, datetime):
            raise Exception("'expiry' must be datetime or yfcd.Interval")

    td = os.path.join(GetCacheDirpath(), ticker)
    if not os.path.isdir(td):
        os.makedirs(td)

    fp = GetFilepath(ticker, objectName, obj=datum, prune=True)
    if fp is None:
        if datum is None:
            raise Exception(f"GetFilepath() returned None for: ticker={ticker}, objectName={objectName}, datum=None")
        else:
            raise Exception(f"GetFilepath() returned None for: ticker={ticker}, objectName={objectName}, datum={type(datum)}")

    if datum is None:
        if verbose:
            print("- deleting {} at {}".format(objectName, fp))
        os.remove(fp)
        return

    if verbose:
        print("- storing {} at {}".format(objectName, fp))

    d = _ReadData(ticker, objectName)
    if d is None:
        md = None
    else:
        md = d["metadata"] if "metadata" in d else None
        if expiry is None:
            expiry = d["expiry"] if "expiry" in d else None

    if metadata is None:
        # Persist the old metadata
        metadata = md

    # Write
    d = {"data": datum}
    if metadata is not None:
        d["metadata"] = metadata
    if expiry is not None:
        d["expiry"] = expiry
    # TODO: use module 'safer' to avoid writes being corrupted
    if fp.endswith(".json"):
        with open(fp, 'w') as outData:
            json.dump(d, outData, default=yfcu.JsonEncodeValue)
    else:
        with open(fp, 'wb') as outData:
            pickle.dump(d, outData, 4)


def StoreCachePackedDatum(ticker, objectName, datum, expiry=None, metadata=None):
    if verbose:
        print("StoreCachePackedDatum({0}, {1})".format(ticker, objectName))

    if not IsObjectInPackedData(objectName):
        raise Exception("Don't call packed-data function on non-packed data '{0}'".format(objectName))

    if (metadata is not None):
        if not isinstance(metadata, dict):
            raise Exception("'metadata' must be dict of scalars")
    if expiry is not None:
        if isinstance(expiry, yfcd.Interval):
            # Convert interval to actual datetime
            expiry = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC")) + yfcd.intervalToTimedelta[expiry]
        if not isinstance(expiry, datetime):
            raise Exception("'expiry' must be datetime or yfcd.Interval")
        if (metadata is not None) and "Expiry" in metadata.keys():
            raise Exception("'metadata' already contains 'Expiry'")

    td = os.path.join(GetCacheDirpath(), ticker)
    if not os.path.isdir(td):
        os.makedirs(td)

    # Read cached metadata
    fp = GetFilepath(ticker, objectName)
    pkData = _ReadPackedData(ticker, objectName)
    if pkData is None:
        pkData = {}
        objData = None
    elif objectName in pkData:
        objData = pkData[objectName]
        if metadata is None:
            metadata = objData["metadata"] if "metadata" in objData else None
        if expiry is None:
            expiry = objData["expiry"] if "expiry" in objData else None
    else:
        objData = None
        # data = None
        # md = None

    if objData is None:
        objData = {"data": datum}
        if metadata is not None:
            objData["metadata"] = metadata
        if expiry is not None:
            objData["expiry"] = expiry
    else:
        objData["data"] = datum
        if metadata is not None:
            objData["metadata"] = metadata
        if expiry is not None:
            objData["expiry"] = expiry

    pkData[objectName] = objData
    with open(fp, 'wb') as outData:
        pickle.dump(pkData, outData, 4)


def ReadCacheMetadata(ticker, objectName, key):
    md = None
    if IsObjectInPackedData(objectName):
        pkData = _ReadPackedData(ticker, objectName)
        if (pkData is not None) and (objectName in pkData):
            objData = pkData[objectName]
            md      = objData["metadata"] if "metadata" in objData else None
    else:
        d = _ReadData(ticker, objectName)
        if d is None or "metadata" not in d:
            return None
        md = d["metadata"]
        if verbose:
            print("ReadCacheMetadata() read md as:")
            print(md)
    if md is None:
        return None
    elif key not in md:
        return None
    else:
        return md[key]


def WriteCacheMetadata(ticker, objectName, key, value):
    if IsObjectInPackedData(objectName):
        return WriteCachePackedMetadata(ticker, objectName, key, value)

    if verbose:
        if value is None:
            print(f"WriteCacheMetadata({ticker}, {objectName}, {key}) deleting")
        else:
            print(f"WriteCacheMetadata({ticker}, {objectName}, {key}) storing")

    d = _ReadData(ticker, objectName)
    if d is None:
        d = {"data":None}

    if "metadata" in d:
        if value is None:
            if key in d["metadata"]:
                del d["metadata"][key]
        else:
            d["metadata"][key] = value
    elif value is not None:
        d["metadata"] = {key: value}

    if verbose:
        print("WriteCacheMetadata() updated md to:")
        print(d["metadata"])

    fp = GetFilepath(ticker, objectName)
    if fp.endswith(".json"):
        with open(fp, 'w') as outData:
            json.dump(d, outData, default=yfcu.JsonEncodeValue)
    else:
        with open(fp, 'wb') as outData:
            pickle.dump(d, outData, 4)


def WriteCachePackedMetadata(ticker, objectName, key, value):
    if not IsObjectInPackedData(objectName):
        return WriteCacheMetadata(ticker, objectName)

    pkData = _ReadPackedData(ticker, objectName)
    if pkData is None:
        raise Exception("'{}/{}' not in cache, cannot add metadata".format(ticker, objectName))
    if objectName not in pkData:
        raise Exception("'{}/{}' not in cache, cannot add metadata".format(ticker, objectName))

    objData = pkData[objectName]
    if "metadata" not in objData:
        objData["metadata"] = {key: value}
    else:
        objData["metadata"][key] = value
    fp = GetFilepath(ticker, objectName)
    with open(fp, 'wb') as outData:
        pickle.dump(pkData, outData, 4)


ResetCacheDirpath()


class NestedOptions:
    def __init__(self, name, data):
        self.__dict__['name'] = name
        self.__dict__['data'] = data

    def __getattr__(self, key):
        return self.data.get(key)

    def __setattr__(self, key, value):
        if self.name == 'max_ages':
            # Type-check value
            pd.Timedelta(value)

        self.data[key] = value
        global _option_manager
        _option_manager._save_option()

    def __len__(self):
        return len(self.__dict__['data'])

    def __repr__(self):
        return json.dumps(self.data, indent=4)

class OptionsManager:
    def __init__(self):
        self._initialised = False

    def _load_option(self):
        self._initialised = True  # prevent infinite loop
        d = GetCacheDirpath()
        self.option_file = os.path.join(d, 'options.json')
        try:
            with open(self.option_file, 'r') as file:
                self.options = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            self.options = {}
            # Initialise
            self.__getattr__('max_ages').calendar = '7d'
            self.__getattr__('max_ages').info = '45d'

    def _save_option(self):
        with open(self.option_file, 'w') as file:
            json.dump(self.options, file, indent=4)

    def __getattr__(self, key):
        if not self._initialised:
            self._load_option()

        if key not in self.options:
            self.options[key] = {}
        return NestedOptions(key, self.options[key])

    def __repr__(self):
        if not self._initialised:
            self._load_option()

        return json.dumps(self.options, indent=4)

# Global instance
_option_manager = OptionsManager()

