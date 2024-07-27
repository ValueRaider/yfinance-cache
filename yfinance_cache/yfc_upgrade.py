import os
import pickle as pkl
import pandas as pd

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
        with open(state_fp, 'w'):
            pass
        return

    o = yfcm._option_manager
    if len(o.max_ages) == 0:
        o.max_ages.calendar = '7d'
        o.max_ages.info = '180d'

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass

def _reset_cached_cals():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-reset-cals")
    if os.path.isfile(state_fp):
        return

    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    import shutil
    dp = yfcm.GetCacheDirpath()
    for d in os.listdir(dp):
        if d.startswith("exchange-"):
            shutil.rmtree(os.path.join(dp, d))

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


def _fix_dt_types_in_divs_splits():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-fixed-types-in-divs-splits")
    if os.path.isfile(state_fp):
        return
    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    dp = yfcm.GetCacheDirpath()
    for d in os.listdir(dp):
        if d.startswith("exchange-"):
            pass
        else:
            # dividends
            divs_fp = os.path.join(dp, d, 'dividends.pkl')
            if os.path.isfile(divs_fp):
                with open(divs_fp, 'rb') as F:
                    data = pkl.load(F)
                df = data['data']
                # print(df)
                c = 'Superseded div FetchDate'
                if c in df.columns:
                    # Ensure NaN values are pd.NaT not np.nan
                    f_na = df[c].isna()    
                    if f_na.any():
                        if not pd.api.types.is_datetime64_any_dtype(df[c]):
                            df.loc[f_na, c] = pd.NaT
                            df[c] = pd.to_datetime(df[c])
                            with open(divs_fp, 'wb') as F:
                                data['data'] = df
                                pkl.dump(data, F, 4)

            # splits
            splits_fp = os.path.join(dp, d, 'splits.pkl')
            if os.path.isfile(splits_fp):
                with open(splits_fp, 'rb') as F:
                    data = pkl.load(F)
                df = data['data']
                # print(df)
                c = 'Superseded split FetchDate'
                if c in df.columns:
                    # Ensure NaN values are pd.NaT not np.nan
                    f_na = df[c].isna()    
                    if f_na.any():
                        if not pd.api.types.is_datetime64_any_dtype(df[c]):
                            df.loc[f_na, c] = pd.NaT
                            df[c] = pd.to_datetime(df[c])
                            with open(splits_fp, 'wb') as F:
                                data['data'] = df
                                pkl.dump(data, F, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass

