from enum import Enum
import os
from datetime import datetime


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
	raise TypeError()


def JsonDecodeDict(value):
	for k in value.keys():
		try:
			value[k] = datetime.fromisoformat(value[k])
		except:
			pass
	return value

