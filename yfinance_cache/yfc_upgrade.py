import os

from . import yfc_cache_manager as yfcm

def _init_options():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-initialised-options")
    if os.path.isfile(state_fp):
        return

    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w') as f:
            pass
        return

    o = yfcm._option_manager
    if len(o.max_ages) == 0:
        o.max_ages.calendar = '7d'
        o.max_ages.info = '180d'

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w') as f:
        pass
