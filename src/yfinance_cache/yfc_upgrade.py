import json, pickle
import os

import pandas as pd
import numpy as np

from . import yfc_cache_manager as yfcm
from . import yfc_utils as yfcu
from . import yfc_time as yfct
from . import yfc_dat as yfcd
from . import yfc_ticker as yfc

def merge_files():
	## To reduce filesystem load, merge files and unpack in memory

	d = yfcm.GetCacheDirpath()

	for tkr in os.listdir(d):
		quarterly_objects = ["quarterly_balance_sheet", "quarterly_cashflow", "quarterly_earnings", "quarterly_financials"]
		qf = "quarterlys.pkl"
		qfp = os.path.join(d, tkr, qf)
		if not os.path.isfile(qfp):
			# print(qfp)
			## Need to merge old separated quarterly data
			qData = {}
			for c in quarterly_objects:
				cf = c+".pkl"
				cfp = os.path.join(d, tkr, cf)
				if os.path.isfile(cfp):
					with open(cfp, 'rb') as inData:
						cData = pickle.load(inData)
					mdfp = os.path.join(d, tkr, c+".metadata")
					with open(mdfp, 'rb') as inData:
						metaData = json.load(inData, object_hook=yfcu.JsonDecodeDict)

					qData[c] = {"data":cData, "metadata":metaData}

			if len(qData) > 0:
				with open(qfp, 'wb') as outData:
					pickle.dump(qData, outData, 4)
				## Cleanup
				for c in quarterly_objects:
					cf = c+".pkl"
					cfp = os.path.join(d, tkr, cf)
					if os.path.isfile(cfp):
						os.remove(cfp)
						os.remove(os.path.join(d, tkr, c+".metadata"))

		annual_objects = ["balance_sheet", "cashflow", "earnings", "financials"]
		af = "annual.pkl"
		afp = os.path.join(d, tkr, af)
		if not os.path.isfile(afp):
			aData = {}
			for c in annual_objects:
				cf = c+".pkl"
				cfp = os.path.join(d, tkr, cf)
				if os.path.isfile(cfp):
					with open(cfp, 'rb') as inData:
						cData = pickle.load(inData)
					mdfp = os.path.join(d, tkr, c+".metadata")
					with open(mdfp, 'rb') as inData:
						metaData = json.load(inData, object_hook=yfcu.JsonDecodeDict)

					aData[c] = {"data":cData, "metadata":metaData}

			if len(aData) > 0:
				with open(afp, 'wb') as outData:
					pickle.dump(aData, outData, 4)
				## Cleanup
				for c in annual_objects:
					cf = c+".pkl"
					cfp = os.path.join(d, tkr, cf)
					if os.path.isfile(cfp):
						os.remove(cfp)
						os.remove(os.path.join(d, tkr, c+".metadata"))

		ipf = "info.pkl"
		ipfp = os.path.join(d, tkr, ipf)
		# if not os.path.isfile(ipfp):
		# 	ijfp = os.path.join(d, tkr, "info.json")
		# 	imfp = os.path.join(d, tkr, "info.metadata")
		# 	if os.path.isfile(ijfp):
		# 		with open(ijfp, 'r')as inData:
		# 			info = json.load(inData, object_hook=yfcu.JsonDecodeDict)
		# 		with open(imfp, 'r') as inData:
		# 			md = json.load(inData, object_hook=yfcu.JsonDecodeDict)
		# 		data = {"info":{"data":info, "metadata":md}}
		# 		with open(ipfp, 'wb') as outData:
		# 			pickle.dump(data, outData, 4)
		# 		os.remove(ijfp)
		# 		os.remove(imfp)
		## Update: keeping 'info' as json, but merging in metadata
		ijfp = os.path.join(d, tkr, "info.json")
		imfp = os.path.join(d, tkr, "info.metadata")
		if os.path.isfile(ipfp):
			# Revert pkl to json
			with open(ipfp, 'rb') as inData:
				pkl = pickle.load(inData)
				pkl = pkl["info"]
			data = pkl["data"]
			md   = pkl["metadata"]
			with open(ijfp, 'w') as outData:
				json.dump({"data":data,"metadata":md}, outData, default=yfcu.JsonEncodeValue)
			os.remove(ipfp)
		elif os.path.isfile(ijfp) and os.path.isfile(imfp):
			# Merge
			with open(ijfp, 'r') as inData:
				data = json.load(inData, object_hook=yfcu.JsonDecodeDict)
			with open(imfp, 'r') as inData:
				md = json.load(inData, object_hook=yfcu.JsonDecodeDict)
			with open(ijfp, 'w') as outData:
				json.dump({"data":data,"metadata":md}, outData, default=yfcu.JsonEncodeValue)
			os.remove(imfp)

		for i in yfc_time.intervalToString.values():
			hstr = "history-"+i
			hpkp = os.path.join(d, tkr, hstr+".pkl")
			hmfp = os.path.join(d, tkr, hstr+".metadata")
			if os.path.isfile(hmfp):
				# Merge into pkl
				with open(hmfp, 'r') as inData:
					md = json.load(inData, object_hook=yfcu.JsonDecodeDict)
				with open(hpkp, 'rb') as inData:
					pkl = pickle.load(inData)
				with open(hpkp, 'wb') as outData:
					pickle.dump({"data":pkl,"metadata":md}, outData, 4)
				os.remove(hmfp)


def clean_metadata():
	# Remove fields: FileType, LastAccess, LastWrite
	d = yfcm.GetCacheDirpath()

	fields = ["FileType", "LastAccess", "LastWrite"]

	for tkr in os.listdir(d):
		tkrd = os.path.join(d, tkr)
		for f in os.listdir(tkrd):
			fp = os.path.join(tkrd, f)
			f_pieces = f.split('.')
			ext = f_pieces[-1]
			if ext == "metadata":
				raise Exception("Found metadata file, should have been merged: "+fp)

			f_base = '.'.join(f_pieces[:-1])

			if ext == "json":
				with open(fp, 'r') as inData:
					js = json.load(inData, object_hook=yfcu.JsonDecodeDict)
				md_changed = False
				for k in fields:
					if k in js["metadata"].keys():
						del js["metadata"][k]
						md_changed = True
				if md_changed:
					with open(fp, 'w') as outData:
						json.dump(js, outData, default=yfcu.JsonEncodeValue)
			else:
				with open(fp, 'rb') as inData:
					pkl = pickle.load(inData)
				md_changed = False
				if not isinstance(pkl, dict):
					pkl = {"data":pkl, "metadata":{}}
					md_changed = True
				if "metadata" in pkl.keys():
					for k in fields:
						if k in pkl["metadata"].keys():
							del pkl["metadata"][k]
							md_changed = True
				else:
					for c in pkl.keys():
						if not "metadata" in pkl[c].keys():
							print(pkl)
							print(pkl.keys())
							raise Exception("Missing metadata from pickle data: {0}".format(fp))
						for k in fields:
							if k in pkl[c]["metadata"].keys():
								del pkl[c]["metadata"][k]
								md_changed = True
				if md_changed:
					with open(fp, 'wb') as outData:
						pickle.dump(pkl, outData, 4)


def price_history():
	## 1) unpack pickle to a simple {"data":..., "metadata":...} structure
	## 2) add 'Final?' column



 	## Add 'Final?' column to price histories
	d = yfcm.GetCacheDirpath()

	for tkr in os.listdir(d):
		tkrd = os.path.join(d, tkr)
		for f in os.listdir(tkrd):
			fp = os.path.join(tkrd, f)
			f_pieces = f.split('.')
			ext = f_pieces[-1]

			f_base = '.'.join(f_pieces[:-1])

			if ("history" in f_base) and (ext == "pkl"):
				with open(fp, 'rb') as inData:
					pkl = pickle.load(inData)

				pkl_changed = False
				if not "data" in pkl:
					if f_base in pkl and "data" in pkl[f_base]:
						pkl = pkl[f_base]
						pkl_changed = True

				if not "data" in pkl:
					print("Expected '{}' to contain {'data':...}, instead it is:".format(fp))
					print(pkl)
					raise Exception("look above")

				df = pkl["data"]
				if not isinstance(df, pd.DataFrame):
					raise Exception("Expected '{}' to contain a pd.DataFrame".format(fp))

				if not "Final?" in df.columns:
					interval = None
					for i,istr in yfcd.intervalToString.items():
						if f_base.endswith(istr):
							interval = i
							break
					if interval is None:
						raise Exception("Failed to map '{}' to Interval".format(fp_base))
					n = df.shape[0]

					exchange = yfc.Ticker(tkr).info["exchange"]
					intervals = np.array([yfct.GetTimestampCurrentInterval(exchange, i, interval, allowLateDailyData=True,weeklyUseYahooDef=True) for i in df.index])
					f_na = intervals==None
					if sum(f_na) > 0:
						for idx in np.where(f_na)[0]:
							dt = df.index[idx]
							print("Failed to map: {} (exchange{}, xcal={})".format(dt, exchange, yfcd.exchangeToXcalExchange[exchange]))
						raise Exception("Problem with dates returned by Yahoo, see above")
					if interval in [yfcd.Interval.Days1, yfcd.Interval.Days5, yfcd.Interval.Week]:
						# The time between intervalEnd and midnight is ambiguous. Yahoo shouldn't have new data, 
						# but with daily candles it can. So treat midnight as threshold for final data.
						data_final = df["FetchDate"].dt.date.values >= np.array([intervals[i]["interval_close"].date() for i in range(n)])
					else:
						data_final = df["FetchDate"].values >= (interval_closes+self.yf_lag)

					df["Final?"] = data_final
					pkl["data"] = df
					pkl_changed = True

				if pkl_changed:
					with open(fp, 'wb') as outData:
						pickle.dump(pkl, outData, 4)


# merge_files()

# clean_metadata()

price_history()