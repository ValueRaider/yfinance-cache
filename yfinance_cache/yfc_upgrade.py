import os
import pickle as pkl
import json
import pandas as pd
import numpy as np

from . import yfc_cache_manager as yfcm
from . import yfc_dat as yfcd
from . import yfc_utils as yfcu


def _tidy_upgrade_history():
    actions = ["have-initialised-options",
                "have-reset-cals",
                "have-fixed-types-in-divs-splits",
                "have-sorted-release-dates",
                "have-initialised-history-metadata",
                "have-fixed-prices-inconsistencies"
                ]

    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    for f in os.listdir(yfc_dp):
        if f not in actions:
            os.remove(os.path.join(yfc_dp, f))


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


def _sort_release_dates():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-sorted-release-dates")
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
            fp = os.path.join(dp, d, 'full-release-dates.pkl')
            if os.path.isfile(fp):
                with open(fp, 'rb') as F:
                    data = pkl.load(F)
                data['data'] = sorted(data['data'])
                with open(fp, 'wb') as F:
                    pkl.dump(data, F, 4)

            fp = os.path.join(dp, d, 'interim-release-dates.pkl')
            if os.path.isfile(fp):
                with open(fp, 'rb') as F:
                    data = pkl.load(F)
                data['data'] = sorted(data['data'])
                with open(fp, 'wb') as F:
                    pkl.dump(data, F, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


def _init_history_metadata():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-initialised-history-metadata")
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
            fp = os.path.join(dp, d, 'listing_date.json')
            new_fp = os.path.join(dp, d, 'history_metadata.json')
            if os.path.isfile(fp):
                with open(fp, 'r') as F:
                    data = json.load(F, object_hook=yfcu.JsonDecodeDict)
                data['data'] = {'listingDate': data['data']}
                with open(new_fp, 'w') as F:
                    json.dump(data, F, default=yfcu.JsonEncodeValue)
                os.remove(fp)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


def _fix_prices_inconsistencies():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-fixed-prices-inconsistencies")
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
            for i in yfcd.Interval:
                istr = yfcd.intervalToString[i]
                prices_fp = os.path.join(dp, d, f'history-{istr}.pkl')
                if os.path.isfile(prices_fp):
                    with open(prices_fp, 'rb') as F:
                        data = pkl.load(F)
                    h = data['data']
                    h_modified = False

                    for c in ['FetchDate', 'LastSplitAdjustDt', 'LastDivAdjustDt']:
                        try:
                            h[c].dt
                        except AttributeError:
                            h[c] = pd.to_datetime(h[c])
                            h_modified = True

                    if 'Capital Gains' in h.columns and (h['Capital Gains']==0).all():
                        h = h.drop('Capital Gains', axis=1)
                        h_modified = True

                    if np.isnan(h["CDF"].to_numpy()).any():
                        h["CDF"] = h["CDF"].bfill().ffill()
                        h_modified = True

                    if h_modified:
                        with open(prices_fp, 'wb') as F:
                            data['data'] = h
                            pkl.dump(data, F, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


def _add_xcal_to_options():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-added-xcal-to-options")
    if os.path.isfile(state_fp):
        return
    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    o = yfcm._option_manager
    o.calendar.accept_unexpected_Yahoo_intervals = True

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass

