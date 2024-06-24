import logging
import os

from . import yfc_cache_manager as yfcm


yfc_logging_mode = None

def EnableLogging(mode=logging.INFO):
    global yfc_logging_mode
    ok_values = [logging.INFO, logging.DEBUG]
    if mode not in ok_values:
        raise Exception('Logging mode must be one of:', ok_values)
    yfc_logging_mode = mode

def DisableLogging():
    global yfc_logging_mode
    yfc_logging_mode = None

def IsLoggingEnabled():
    global yfc_logging_mode
    return yfc_logging_mode is not None

loggers = {}
def GetLogger(tkr):
    if tkr in loggers:
        return loggers[tkr]

    global yfc_logging_mode
    tkr_dp = os.path.join(yfcm.GetCacheDirpath(), tkr)
    if not os.path.isdir(tkr_dp):
        os.mkdir(tkr_dp)

    log_fp = os.path.join(tkr_dp, "events.log")
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    log_file_handler = logging.FileHandler(log_fp, mode='a')
    log_file_handler.setFormatter(formatter)
    # screen_handler = logging.StreamHandler(stream=sys.stdout)
    # screen_handler.setFormatter(formatter)
    logger = logging.getLogger(tkr)
    logger.setLevel(yfc_logging_mode)
    logger.addHandler(log_file_handler)
    logger.propagate = False

    loggers[tkr] = logger
    return logger


yfc_trace_mode = False

def EnableTracing():
    global yfc_trace_mode
    yfc_trace_mode = True

def DisableTracing():
    global yfc_trace_mode
    yfc_trace_mode = False

def IsTracingEnabled():
    global yfc_trace_mode
    return yfc_trace_mode

class Tracer:
    def __init__(self):
        self._trace_depth = 0

    def Print(self, log_msg):
        if not IsTracingEnabled():
            return
        print(" "*self._trace_depth*2 + log_msg)

    def Enter(self, log_msg):
        if not IsTracingEnabled():
            return
        self.Print(log_msg)
        self._trace_depth += 1

        if self._trace_depth > 20:
            raise Exception("infinite recursion detected")

    def Exit(self, log_msg):
        if not IsTracingEnabled():
            return
        self._trace_depth -= 1
        self.Print(log_msg)

tc = Tracer()

def TraceEnter(log_msg):
    global tc
    tc.Enter(log_msg)

def TracePrint(log_msg):
    global tc
    tc.Print(log_msg)

def TraceExit(log_msg):
    global tc
    tc.Exit(log_msg)
