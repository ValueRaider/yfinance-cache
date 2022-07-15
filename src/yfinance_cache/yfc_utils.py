from enum import Enum
import os
from datetime import datetime, timedelta
import re

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


def GetUserCacheDirpath():
	_os = GetOperatingSystem()
	if _os == OperatingSystem.Windows:
		dp = os.getenv("APPDATA")
		raise Exception("Not tested. Does this make sense as cache dir in Windows? - {0}".format(dp))
	elif _os == OperatingSystem.Linux:
		dp = os.path.join(os.getenv("HOME"), ".cache")
	else:
		raise Exception("Not implemented: cache dirpath under OS '{0}'".format(_os))
	return dp


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


def GetSigFigs(n):
	if n == 0:
		return 0
	n_str = str(n).replace('.', '')
	m = re.match( r'0*[1-9](\d*[1-9])?', n_str)
	sf = len(m.group())

	return sf


def GetMagnitude(n):
	m = 0
	if n >= 1.0:
		while n >= 1.0:
			n *= 0.1
			m += 1
	else:
		while n < 1.0:
			n *= 10.0
			m -= 1

	return m


def CalculateRounding(n, sigfigs):
	if GetSigFigs(round(n)) >= sigfigs:
		return 0
	elif round(n,0)==n:
		return 0
	else:
		return sigfigs - GetSigFigs(round(n))
