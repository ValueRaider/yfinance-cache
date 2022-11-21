import os
import pickle, json
from pprint import pprint
from enum import Enum

from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from . import yfc_dat as yfcd
from . import yfc_utils as yfcu

# To reduce #files in cache, store some YF objects together into same file (including metadata)
packed_data_cats = {}
packed_data_cats["quarterlys"] = ["quarterly_balance_sheet", "quarterly_cashflow", "quarterly_earnings", "quarterly_financials"]
packed_data_cats["annuals"]    = ["balance_sheet", "cashflow", "earnings", "financials"]

quarterly_objects = packed_data_cats["quarterlys"]
annual_objects    = packed_data_cats["annuals"]

verbose=False
# verbose=True

## TODO: store DataFrames as CSV, don't need the metadata

global cacheDirpath


def GetCacheDirpath():
	global cacheDirpath
	return cacheDirpath


def ResetCacheDirpath():
	global cacheDirpath
	cacheDirpath = os.path.join(yfcu.GetUserCacheDirpath(), "yfinance-cache")


def SetCacheDirpath(dp):
	global cacheDirpath
	cacheDirpath = dp
	if verbose:
		print("Set cache dir to {0}".format(cacheDirpath))


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


def GetFilepath(ticker, objectName, object=None, prune=False):
	if IsObjectInPackedData(objectName):
		return GetFilepathPacked(ticker, objectName)

	fp = None
	if not object is None:
		if isinstance(object, (list,int,float,str,datetime,date,timedelta)):
			ext = "json"
			ext2 = "pkl"
		else:
			ext = "pkl"
			ext2 = "json"
		fp = os.path.join(GetCacheDirpath(), ticker, objectName) + "."+ext
		fp2 = os.path.join(GetCacheDirpath(), ticker, objectName) + "."+ext2
		if os.path.isfile(fp2):
			if prune:
				os.remove(fp2)
			else:
				raise Exception("For {} object {}/{}, a {} file already exists".format(ext, ticker, objectName, ext2))
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
	return fp
def GetFilepathPacked(ticker, objectName):
	if not IsObjectInPackedData(objectName):
		return InferFilepath(ticker, objectName)
	pkg = GetPackedDataCat(objectName)
	fp = os.path.join(GetCacheDirpath(), ticker, pkg) +".pkl"
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
		return os.path.isfile(fp)


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
		if not "data" in d.keys():
			print(fp)
			raise Exception("Pickled dict missing 'data' key ({0}) - {1}".format(pkl.keys()))
	return d


def _ReadPackedData(ticker, objectName):
	pkData = None
	fp = GetFilepath(ticker, objectName)
	if os.path.isfile(fp):
		with open(fp, 'rb') as inData:
			pkData = pickle.load(inData)
		if not isinstance(pkData, dict):
			raise Exception("Pickled '{}/{}' packed-data should be dict, but is {}".format(ticker, objectName, type(d)))
	return pkData


def ReadCacheDatum(ticker, objectName, return_metadata_too=False):
	if verbose:
		print("ReadCacheDatum({0}, {1})".format(ticker, objectName))

	if IsObjectInPackedData(objectName):
		return ReadCachePackedDatum(ticker, objectName, return_metadata_too)

	data = None ; md = None
	d = _ReadData(ticker, objectName)
	if not d is None:
		data   = d["data"]
		md     = d["metadata"] if "metadata" in d else None
		expiry = d["expiry"]   if "expiry"   in d else None

		if not expiry is None:
			dtnow = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
			if dtnow >= expiry:
				if verbose:
					print("Deleting expired datum '{0}/{1}'".format(ticker, objectName))
				fp = GetFilepath(ticker, objectName)
				os.remove(fp)
				if return_metadata_too:
					return None,None
				else:
					return None

			if md is None:
				md = {"__expiry__":expiry}
			else:
				md["__expiry__"] = expiry

	if return_metadata_too:
		return data,md
	else:
		return data


def ReadCachePackedDatum(ticker, objectName, return_metadata_too=False):
	if verbose:
		print("ReadCachePackedDatum({0}, {1})".format(ticker, objectName))

	if not IsObjectInPackedData(objectName):
		raise Exception("Don't call packed-data function on non-packed data '{0}'".format(objectName))

	data = None ; md = None
	pkData = _ReadPackedData(ticker, objectName)
	if (not pkData is None) and (objectName in pkData):
		objData = pkData[objectName]
		data   = objData["data"]
		md     = objData["metadata"] if "metadata" in objData else None
		expiry = objData["expiry"]   if "expiry"   in objData else None

		if not expiry is None:
			dtnow = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
			if dtnow >= expiry:
				if verbose:
					print("Deleting expired packed datum '{0}/{1}'".format(ticker, objectName))
				del pkData[objectName]
				fp = GetFilepath(ticker, objectName)
				with open(fp, 'wb') as outData:
					pickle.dump(pkData, outData, 4)
				if return_metadata_too:
					return None,None
				else:
					return None

			if md is None:
				md = {"__expiry__":expiry}
			else:
				md["__expiry__"] = expiry

	if return_metadata_too:
		return data,md
	else:
		return data


def StoreCacheDatum(ticker, objectName, datum, expiry=None, metadata=None):
	if verbose:
		print("StoreCacheDatum({0}, {1})".format(ticker, objectName))

	if IsObjectInPackedData(objectName):
		StoreCachePackedDatum(ticker, objectName, datum, metadata=metadata)
		return

	if (not metadata is None) and not isinstance(metadata, dict):
		raise Exception("'metadata' must be dict of scalars")
	if not expiry is None:
		if isinstance(expiry, yfcd.Interval):
			# Convert interval to actual datetime
			expiry = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC")) + yfcd.intervalToTimedelta[expiry]
		if not isinstance(expiry, datetime):
			raise Exception("'expiry' must be datetime or yfcd.Interval")

	td = os.path.join(GetCacheDirpath(), ticker)
	if not os.path.isdir(td):
		os.makedirs(td)

	fp = GetFilepath(ticker, objectName, object=datum, prune=True)

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
	d = {"data":datum}
	if not metadata is None:
		d["metadata"] = metadata
	if not expiry is None:
		d["expiry"] = expiry
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

	if (not metadata is None):
		if not isinstance(metadata, dict):
			raise Exception("'metadata' must be dict of scalars")
	if not expiry is None:
		if isinstance(expiry, yfcd.Interval):
			# Convert interval to actual datetime
			expiry = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC")) + yfcd.intervalToTimedelta[expiry]
		if not isinstance(expiry, datetime):
			raise Exception("'expiry' must be datetime or yfcd.Interval")
		if (not metadata is None) and "Expiry" in metadata.keys():
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
		data = None
	elif objectName in pkData:
		objData = pkData[objectName]
		data   = objData["data"]
		if metadata is None:
			metadata = objData["metadata"] if "metadata" in objData else None
		if expiry is None:
			expiry = objData["expiry"] if "expiry" in objData else None
	else:
		objData = None
		data = None
		md = None

	if objData is None:
		objData = {"data":datum}
		if not metadata is None:
			objData["metadata"] = metadata
		if not expiry is None:
			objData["expiry"] = expiry
	else:
		objData["data"] = datum
		if not metadata is None:
			objData["metadata"] = metadata
		if not expiry is None:
			objData["expiry"] = expiry

	pkData[objectName] = objData
	with open(fp, 'wb') as outData:
		pickle.dump(pkData, outData, 4)


def ReadCacheMetadata(ticker, objectName, key):
	data=None ; md=None
	if IsObjectInPackedData(objectName):
		pkData = _ReadPackedData(ticker, objectName)
		if (not pkData is None) and (objectName in pkData):
			objData = pkData[objectName]
			data   = objData["data"]
			md     = objData["metadata"] if "metadata" in objData else None
	else:
		d = _ReadData(ticker, objectName)
		if not "metadata" in d:
			return None
		md = d["metadata"]
		if verbose:
			print("ReadCacheMetadata() read md as:")
			print(md)
	if md is None:
		return None
	elif not key in md:
		return None
	else:
		return md[key]


def WriteCacheMetadata(ticker, objectName, key, value):
	if IsObjectInPackedData(objectName):
		return WriteCachePackedMetadata(ticker, objectName, key, value)

	d = _ReadData(ticker, objectName)
	if d is None:
		raise Exception("'{}/{}' not in cache, cannot add metadata".format(ticker, objectName))

	if "metadata" in d:
		d["metadata"][key] = value
	else:
		d["metadata"] = {key:value}

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
	if not objectName in pkData:
		raise Exception("'{}/{}' not in cache, cannot add metadata".format(ticker, objectName))

	objData = pkData[objectName]
	if not "metadata" in objData:
		objData["metadata"] = {key:value}
	else:
		objData["metadata"][key] = value
	fp = GetFilepath(ticker, objectName)
	with open(fp, 'wb') as outData:
		pickle.dump(pkData, outData, 4)


ResetCacheDirpath()
