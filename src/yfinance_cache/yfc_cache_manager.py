import os
import pickle, json
from datetime import datetime

# from .yfc_utils import *
from yfc_utils import *


dict_objects = ["info"]


class Modifiers(Enum):
	Access = 1
	Write = 2


def GetCacheDirpath():
	cache_dirpath = None
	_os = GetOperatingSystem()
	if _os == OperatingSystem.Windows:
		cache_dirpath = os.getenv("APPDATA") + "\\yfinance-cache"
		raise Exception("Not tested. Does this make sense as cache dir in Windows? - {0}".format(cache_dirpath))
	elif _os == OperatingSystem.Linux:
		cache_dirpath = os.path.join(os.getenv("HOME"), ".cache", "yfinance-cache")
	else:
		raise Exception("Not implemented: cache dirpath under OS '{0}'".format(_os))
	return cache_dirpath


def UpdateDatumMetadata(ticker, objectName, modifier):
	fp = os.path.join(GetCacheDirpath(), ticker, objectName+".metadata")
	md = {}
	if os.path.isfile(fp):
		# with open(fp, 'rb') as inData:
		# 	md = pickle.load(inData)
		with open(fp, 'r') as inData:
			md = json.load(inData, object_hook=JsonDecodeDict)

	if modifier == Modifiers.Access:
		md["LastAccess"] = datetime.today()
	elif modifier == Modifiers.Write:
		md["LastWrite"] = datetime.today()
	else:
		raise Exception("Unsupported modifiers '{0}'".format(modifier))

	# print("{0} metdata = {1}".format(ticker, md))

	# with open(fp, 'wb') as outData:
	# 	pickle.dump(md, outData, 4)
	with open(fp, 'w') as outData:
		json.dump(md, outData, default=JsonEncodeValue)


def IsDatumCached(ticker, objectName):
	fp = os.path.join(GetCacheDirpath(), ticker, objectName)
	if objectName in dict_objects:
		fp += ".json"
	else:
		fp += ".pkl"
	return os.path.isfile(fp)

def ReadCacheDatum(ticker, objectName):
	# print("ReadCacheDatum({0}, {1})".format(ticker, objectName))
	data = None

	fp = os.path.join(GetCacheDirpath(), ticker, objectName)
	if objectName in dict_objects:
		fp += ".json"
		if os.path.isfile(fp):
			with open(fp, 'r') as inData:
				data = json.load(inData, object_hook=JsonDecodeDict)
	else:
		fp += ".pkl"
		if os.path.isfile(fp):
			with open(fp, 'rb') as inData:
				data = pickle.load(inData)

	if not data is None:
		UpdateDatumMetadata(ticker, objectName, Modifiers.Access)

	return data


def StoreCacheDatum(ticker, objectName, datum):
	td = os.path.join(GetCacheDirpath(), ticker)
	if not os.path.isdir(td):
		os.makedirs(td)
	fp = os.path.join(td, objectName)
	if objectName in dict_objects:
		with open(fp+".json", 'w') as outData:
			json.dump(datum, outData, default=JsonEncodeValue)
	else:
		with open(fp+".pkl", 'wb') as outData:
			pickle.dump(datum, outData, 4)

	UpdateDatumMetadata(ticker, objectName, Modifiers.Write)

