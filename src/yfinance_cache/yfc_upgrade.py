import json, pickle
import os
from pprint import pprint
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np
import datetime

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
	## 3) daily and weekly tables should be indexed by Date, not Datetime - UPDATE: STORE/LOAD as Pandas.Timestamp for DatetimeIndex
	## 3b) ensure 'FetchDate' is DatetimeIndex
	## 4) rename "Splits" -> "Stock Splits" if present

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

				if "1d" in fp or "1w" in fp:
					df_changed = False
					# if df.index.name == "Datetime":
					# 	print("Converting DT index to D in: {}".format(fp))
					# 	df.index = df.index.date
					# 	df.index.name = "Date"
					# 	df_changed = True
					# # elif not isinstance(df.index,pd.DatetimeIndex):
					# # 	# Contains mix of Python date/datetimes and Pandas timestamps.
					# # 	# -> Convert to date.
					# # 	print("Fixing non-pd.DatetimeIndex in: {}".format(fp))
					# # 	days = []
					# # 	for i in range(len(df.index)):
					# # 		dt = df.index[i]
					# # 		if isinstance(dt,pd.Timestamp):
					# # 			days.append(dt.astimezone(ZoneInfo("UTC")).to_pydatetime().date())
					# # 		elif isinstance(dt,datetime.datetime):
					# # 			days.append(dt.date())
					# # 		else:
					# # 			days.append(dt)
					# # 	df.index = pd.DatetimeIndex(days)
					# # 	pkl["data"] = df
					# # 	pkl_changed = True
					# dt0 = df.index[0]
					# if isinstance(dt0, pd.Timestamp):
					# 	print("Converting PD.TS index to D in: {}".format(fp))
					# 	df.index = df.index.date
					# 	df.index.name = "Date"
					# 	df_changed = True
					# if df.index.name in [None, ""]:
					# 	print("Fixing bad index name")
					# 	df.index.name = "Date"
					# 	df_changed = True

					if df_changed:
						pkl["data"] = df
						pkl_changed = True

				try:
					df["FetchDate"].dt
				except AttributeError:
					## .dt attribute available. Implies column contains mix of different date/datetime types. Fix
					df["FetchDate"] = [pd.Timestamp(x) for x in df["FetchDate"]]
					pkl["data"] = df
					pkl_changed = True

				if "Splits" in df.columns:
					df = df.rename(columns={"Splits":"Stock Splits"})
					pkl["data"] = df
					pkl_changed = True
				if (df.columns.values=="Stock Splits").sum() > 1:
					# Whoops, have duplicated column. Need to drop one with most nans
					# 1) count nans
					col_idxs = np.where(df.columns=="Stock Splits")[0]
					nan_counts = {}
					for col_idx in col_idxs:
						nan_counts[col_idx] = df.iloc[:,col_idx].isna().sum()
					# 2) find col with least nans
					least_na_idx = col_idxs[0]
					for col_idx in col_idxs:
						if nan_counts[col_idx] < nan_counts[least_na_idx]:
							least_na_idx = col_idx
					# 3) drop the others
					cols_to_keep = list(range(0,len(df.columns)))
					for col_idx in col_idxs:
						if col_idx != least_na_idx:
							cols_to_keep.remove(col_idx)
					df = df.iloc[:,cols_to_keep]
					pkl["data"] = df
					pkl_changed = True


				if pkl_changed:
					with open(fp, 'wb') as outData:
						pickle.dump(pkl, outData, 4)


def price_history_cleanup_tz_mess():
	## 1) delete rows with NANs <- UPDATE: leave NAN rows, because is placeholder for days without trades
	## 2) Ensure 'FetchDate' columns entries all have timezone
	## 3) Remove duplicate-date rows
	## 4) Remove duplicate-interval rows

	d = yfcm.GetCacheDirpath()

	for tkr in os.listdir(d):
		tkrd = os.path.join(d, tkr)
		for f in os.listdir(tkrd):
			fp = os.path.join(tkrd, f)
			f_pieces = f.split('.')
			ext = f_pieces[-1]

			# if fp != "/home/gonzo/.cache/yfinance-cache/MEL.NZ/history-1d.pkl":
			# 	continue

			f_base = '.'.join(f_pieces[:-1])

			if ("history" in f_base) and (ext == "pkl"):
				with open(fp, 'rb') as inData:
					pkl = pickle.load(inData)

				# print("Checking "+fp)

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

				# # Remove empty rows
				f_na = df["Close"].isna()
				if sum(f_na) > 0:
					df = df[~f_na]
					pkl["data"] = df
					pkl_changed = True

				# FetchDate has timezone?
				try:
					df["FetchDate"].dt
				except AttributeError:
					# Happens if some FetchDate entries added without timezones. 
					# Fix = add a timezone. Not important what it is
					df["FetchDate"] = pd.to_datetime(df["FetchDate"], utc=True)
					pkl["data"] = df
					pkl_changed = True

				if "1d" in fp or "1w" in fp:
					# Check for duplicate dates
					n = df.shape[0]
					df_changed = False
					for i in range(n-2, -1, -1):
						idxUp = i
						idxDown = i+1
						# if df.index[idxUp].date() == df.index[idxDown].date():
						if df.index[idxUp] == df.index[idxDown]:
							df = df.drop(df.index[idxUp])
							df_changed = True
						break
					if df_changed:
						pkl["data"] = df
						pkl_changed = True
				else:
					# Check for duplicate datetimes
					n = df.shape[0]
					df_changed = False
					for i in range(n-2, -1, -1):
						idxUp = i
						idxDown = i+1
						if df.index[idxUp] == df.index[idxDown]:
							df = df.drop(df.index[idxUp])
							df_changed = True
						break
					if df_changed:
						pkl["data"] = df
						pkl_changed = True

				# Check for duplicate intervals:
				if "1w" in fp:
					interval = yfcd.Interval.Week
				elif "1d" in fp:
					interval = yfcd.Interval.Days1
				elif "1h" in fp:
					interval = yfcd.Interval.Hours1
				else:
					raise Exception("Failed to infer interval of: "+fp)
				dat = yfc.Ticker(tkr)
				exchange = dat.info["exchange"]
				tz_exchange = ZoneInfo(yfct.GetExchangeTzName(exchange))
				if interval in [yfcd.Interval.Days1, yfcd.Interval.Week]:
					h_intervalStarts = df.index
				else:
					h_intervalStarts = np.array([yfct.ConvertToDatetime(dt, tz=tz_exchange) for dt in df.index])
				intervals = np.array([yfct.GetTimestampCurrentInterval(exchange, x, interval, weeklyUseYahooDef=True) for x in h_intervalStarts])
				f_na = intervals == None
				if sum(f_na) > 0:
					print("Removing rows that don't map to intervals")
					df = df[~f_na]
					h_intervalStarts = h_intervalStarts[~f_na]
					intervals = intervals[~f_na]
					#
					pkl["data"] = df
					pkl_changed = True
				interval_opens = np.array([x["interval_open"] for x in intervals])
				uniq_idxs = np.unique(interval_opens, return_index=True)[1]
				dup_idxs = list(set(range(0,df.shape[0])) - set(uniq_idxs))
				# print("- dup_idxs: {}".format(type(dup_idxs)))
				# pprint(dup_idxs)
				dup_interval_opens = interval_opens[dup_idxs]
				f_dup = np.isin(interval_opens, dup_interval_opens)
				if sum(f_dup) > 0:
					print("Removing duplicate rows in same interval:")
					print(df[f_dup])
					df = df[~f_dup]
					#
					pkl["data"] = df
					pkl_changed = True

				if pkl_changed:
					print("Have fixed tz issues in: "+fp)
					with open(fp, 'wb') as outData:
						pickle.dump(pkl, outData, 4)


# merge_files()

# clean_metadata()

# price_history()

# price_history_cleanup_tz_mess()

## TODO: for each prices table in cache, compare against yfinance directly
##       Best implemented in yfc_ticker

