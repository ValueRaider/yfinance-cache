import yfc_cache_manager as yfcm

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
						metaData = json.load(inData, object_hook=JsonDecodeDict)

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
						metaData = json.load(inData, object_hook=JsonDecodeDict)

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
		# 			info = json.load(inData, object_hook=JsonDecodeDict)
		# 		with open(imfp, 'r') as inData:
		# 			md = json.load(inData, object_hook=JsonDecodeDict)
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
				json.dump({"data":data,"metadata":md}, outData, default=JsonEncodeValue)
			os.remove(ipfp)
		elif os.path.isfile(ijfp) and os.path.isfile(imfp):
			# Merge
			with open(ijfp, 'r') as inData:
				data = json.load(inData, object_hook=JsonDecodeDict)
			with open(imfp, 'r') as inData:
				md = json.load(inData, object_hook=JsonDecodeDict)
			with open(ijfp, 'w') as outData:
				json.dump({"data":data,"metadata":md}, outData, default=JsonEncodeValue)
			os.remove(imfp)

		for i in yfc_time.intervalToString.values():
			hstr = "history-"+i
			hpkp = os.path.join(d, tkr, hstr+".pkl")
			hmfp = os.path.join(d, tkr, hstr+".metadata")
			if os.path.isfile(hmfp):
				# Merge into pkl
				with open(hmfp, 'r') as inData:
					md = json.load(inData, object_hook=JsonDecodeDict)
				with open(hpkp, 'rb') as inData:
					pkl = pickle.load(inData)
				with open(hpkp, 'wb') as outData:
					pickle.dump({"data":pkl,"metadata":md}, outData, 4)
				os.remove(hmfp)


def clean_metadata():
	# Remove fields: FileType, LastAccess
	d = yfcm.GetCacheDirpath()

	fields = ["FileType", "LastAccess"]

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
					js = json.load(inData, object_hook=JsonDecodeDict)
				md_changed = False
				for k in fields:
					if k in js["metadata"].keys():
						del js["metadata"][k]
						md_changed = True
				if md_changed:
					with open(fp, 'w') as outData:
						json.dump(js, outData, default=JsonEncodeValue)
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

merge_files()

clean_metadata()
