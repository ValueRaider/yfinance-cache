from enum import Enum
import os
from datetime import datetime, timedelta


class OperatingSystem(Enum):
	Windows = 1
	Linux = 2
	OSX = 3


def GetOperatingSystem():
	if os.name == "nt":
		return OperatingSystem.Windows
	elif os.name == "posix":
		return OperatingSystem.Linux
	else:
		raise Exception("Unknwon os.name = '{0}'".format(os.name))


def JsonEncodeValue(value):
	if isinstance(value, datetime):
		return value.isoformat()
	elif isinstance(value, timedelta):
		e = "timedelta-{0}".format(value.total_seconds())
		return e
	raise TypeError()


def JsonDecodeDict(value):
	for k in value.keys():
		v = value[k]
		if isinstance(v,str) and v.startswith("timedelta-"):
			try:
				sfx = '-'.join(v.split('-')[1:])
				sfxf = float(sfx)
				value[k] = timedelta(seconds=sfxf)
			except:
				pass
		else:
			try:
				value[k] = datetime.fromisoformat(v)
			except:
				pass
	return value

