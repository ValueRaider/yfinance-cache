import os
import pickle as pkl
import json
import pandas as pd
import numpy as np
import datetime
import shutil

from . import yfc_cache_manager as yfcm
from . import yfc_dat as yfcd
from . import yfc_utils as yfcu
from . import yfc_time as yfct


def _tidy_upgrade_history():
    actions = ["have-initialised-options",
                "have-reset-cals",
                "have-fixed-types-in-divs-splits",
                "have-sorted-release-dates",
                "have-initialised-history-metadata",
                "have-fixed-prices-inconsistencies",
                "have-added-xcal-to-options",
                "have-added-options-max-age-to-options",
                "have-upgraded-calendar-to-df",
                "have-added-repaired-to-cached-divs",
                "have-fixed-prices-final-again",
                "have-reset-xcals-again",
                "have-reset-ccy-cal",
                "have-fixed-24h-prices-final"
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


def _add_options_max_age_to_options():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-added-options-max-age-to-options")
    if os.path.isfile(state_fp):
        return
    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    o = yfcm._option_manager
    o.max_ages.options = '1d'

    # Also ensure all fetched options have metadata with FetchDate
    dp = yfcm.GetCacheDirpath()
    for d in os.listdir(dp):
        if d.startswith("exchange-"):
            pass
        else:
            options_fp = os.path.join(dp, d, f'options.pkl')
            if os.path.isfile(options_fp):
                with open(options_fp, 'rb') as F:
                    data = pkl.load(F)

                md_modified = False
                if 'metadata' not in data:
                    md = {}
                else:
                    md = data['metadata']
                if 'FetchDate' not in md.keys():
                    mod_dt = datetime.datetime.fromtimestamp(os.path.getmtime(options_fp))
                    md['FetchDate'] = mod_dt
                    md['LastCheck'] = mod_dt
                    md_modified = True

                if md_modified:
                    with open(options_fp, 'wb') as F:
                        data['metadata'] = md
                        pkl.dump(data, F, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


def _upgrade_calendar_to_df():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-upgraded-calendar-to-df")
    if os.path.isfile(state_fp):
        return
    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    print("YFC upgrading cached calendars ...")

    columns = ['Earnings Date1', 'Earnings Date2', 'Earnings Low', 'Earnings High', 'Earnings Average', 'Revenue Low', 'Revenue High', 'Revenue Average', 'Ex-Dividend Date']
    date_cols = [c for c in columns if 'Date' in c]
    num_cols = [c for c in columns if ('Revenue' in c or 'Earnings' in c) and c not in date_cols]
    columns += ['FetchDate', 'LastCheck']
    dp = yfcm.GetCacheDirpath()
    for d in os.listdir(dp):
        if d.startswith("exchange-"):
            pass
        else:
            calendar_fp = os.path.join(dp, d, f'calendar.pkl')
            calendars_fp = os.path.join(dp, d, f'calendars.pkl')
            if os.path.isfile(calendar_fp):
                with open(calendar_fp, 'rb') as F:
                    data = pkl.load(F)
                if isinstance(data['data'], pd.DataFrame):
                    os.rename(calendar_fp, calendars_fp)
                    continue

                md = data['metadata']
                data = data['data']

                info = yfcm.ReadCacheDatum(d, 'info')
                if info is None:
                    tz = None
                else:
                    tz = info['timeZoneFullName']

                if 'Earnings Date' in data:
                    if len(data['Earnings Date']) > 0:
                        data['Earnings Date1'] = data['Earnings Date'][0]
                    else:
                        data['Earnings Date1'] = None
                    if len(data['Earnings Date']) > 1:
                        data['Earnings Date2'] = data['Earnings Date'][1]
                    else:
                        data['Earnings Date2'] = None
                    del data['Earnings Date']
                else:
                    # No calendar
                    data['Earnings Date1'] = None
                    data['Earnings Date2'] = None

                if 'Ex-Dividend Date' not in data:
                    data['Ex-Dividend Date'] = None

                for c in num_cols:
                    if c not in data:
                        data[c] = None

                if 'LastCheck' in md:
                    data['LastCheck'] = md['LastCheck']
                else:
                    data['LastCheck'] = data['FetchDate']

                df = pd.DataFrame(data, index=[0])
                df = df[columns]  # order

                data = {'data':df, 'metadata':None}
                with open(calendars_fp, 'wb') as F:
                    pkl.dump(data, F, 4)
                os.remove(calendar_fp)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


def _add_holdings_analysis_to_options():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-added-holdings-analysis-to-options")
    if os.path.isfile(state_fp):
        return
    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    o = yfcm._option_manager
    o.max_ages.holdings = '91d'
    o.max_ages.analysis = '91d'

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


def _add_repaired_to_cached_divs():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-added-repaired-to-cached-divs")
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
            divs_fp = os.path.join(dp, d, f'dividends.pkl')
            if os.path.isfile(divs_fp):
                with open(divs_fp, 'rb') as F:
                    data = pkl.load(F)
                divs = data['data']
                divs_modified = False

                if divs.empty:
                    continue

                if 'Close repaired?' not in divs.columns:
                    divs['Close repaired?'] = False
                    divs_modified = True
                    prices_fp = os.path.join(dp, d, f'history-1d.pkl')
                    if os.path.isfile(prices_fp):
                        with open(prices_fp, 'rb') as F:
                            prices = pkl.load(F)['data']
                        for dt in divs.index:
                            if dt not in prices.index:
                                # must have deleted old prices from cache
                                idx = -1
                            else:
                                idx = prices.index.get_loc(dt)
                            if idx == 0:
                                # pass
                                raise Exception(f'{d}: should have close before {dt}')
                            elif idx > 1:
                                divs.loc[dt, 'Close repaired?'] = prices['Repaired?'].iloc[idx-1]
                else:
                    f_na = divs['Close repaired?'].isna()
                    if f_na.any():
                        divs.loc[f_na, 'Close repaired?'] = False
                        divs_modified = True
                        prices_fp = os.path.join(dp, d, f'history-1d.pkl')
                        if os.path.isfile(prices_fp):
                            with open(prices_fp, 'rb') as F:
                                prices = pkl.load(F)['data']
                            for i in np.where(f_na)[0]:
                                dt = divs.index[i]
                                if dt not in prices.index:
                                    # must have deleted old prices from cache
                                    idx = -1
                                else:
                                    idx = prices.index.get_loc(dt)
                                if idx == 0:
                                    # pass
                                    raise Exception(f'{d}: should have close before {dt}')
                                elif idx > 1:
                                    divs.loc[dt, 'Close repaired?'] = prices['Repaired?'].iloc[idx-1]
                        divs_modified = True

                if divs_modified:
                    with open(divs_fp, 'wb') as F:
                        data['data'] = divs
                        pkl.dump(data, F, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


def _fix_prices_final_again():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-fixed-prices-final-again")
    if os.path.isfile(state_fp):
        return
    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    dp = yfcm.GetCacheDirpath()
    contents = os.listdir(dp)

    n = len(contents)
    if n == 0:
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    e = n/680
    print(f"YFC recalculating 'Final?' column in prices, estimate {e:.1f} minutes to process {n} tickers.")

    try:
        from tqdm import tqdm
        iterator = tqdm(range(len(contents)))
        manual_progress_bar = None
    except Exception:
        # Use YF's progress bar
        iterator = range(len(contents))
        from yfinance import utils as yf_utils
        manual_progress_bar = yf_utils.ProgressBar(len(contents))
    for i in iterator:
        d = contents[i]
        if manual_progress_bar is not None:
            manual_progress_bar.animate()

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
                    if h is None or h.empty:
                        continue

                    info = yfcm.ReadCacheDatum(d, 'info')
                    lastDataDts = yfct.CalcIntervalLastDataDt_batch(info['exchange'], h.index.to_numpy(), i)#, bfill=True)
                    data_final = h['FetchDate'] >= lastDataDts
                    if (h["Final?"] != data_final).any():
                        h["Final?"] = data_final
                        with open(prices_fp, 'wb') as F:
                            data['data'] = h
                            pkl.dump(data, F, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


def _reset_cached_cals_again():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-reset-xcals-again")
    if os.path.isfile(state_fp):
        return
    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    dp = yfcm.GetCacheDirpath()
    contents = os.listdir(dp)

    n = len(contents)
    if n == 0:
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    for d in contents:
        if d.startswith("exchange-"):
            shutil.rmtree(os.path.join(dp, d))

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


def _reset_CCY_cal():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-reset-ccy-cal")
    if os.path.isfile(state_fp):
        return
    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    dp = yfcm.GetCacheDirpath()
    contents = os.listdir(dp)

    n = len(contents)
    if n == 0:
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    d = 'exchange-CCY'
    if os.path.isdir(os.path.join(dp, d)):
        shutil.rmtree(os.path.join(dp, d))

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


def _fix_24_hour_prices_final():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-fixed-24h-prices-final")
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
        if d.startswith("exchange-") or d.endswith('.json') or d.startswith('_'):
            pass
        else:
            info_fp = os.path.join(dp, d, f'info.json')
            info = yfcm._ReadData(d, 'info')['data']
            if 'exchange' not in info:
                # not listed probably, skip for now
                continue
            exchange = info['exchange']
            if exchange not in ['CCC', 'CCY']:
                continue

            for i in yfcd.Interval:
                istr = yfcd.intervalToString[i]
                prices_fp = os.path.join(dp, d, f'history-{istr}.pkl')
                if os.path.isfile(prices_fp):
                    with open(prices_fp, 'rb') as F:
                        data = pkl.load(F)
                    h = data['data']
                    if h is None or h.empty:
                        continue

                    info = yfcm.ReadCacheDatum(d, 'info')
                    lastDataDts = yfct.CalcIntervalLastDataDt_batch(info['exchange'], h.index.to_numpy(), i)#, bfill=True)
                    data_final = h['FetchDate'] >= lastDataDts
                    if (h["Final?"] != data_final).any():
                        h["Final?"] = data_final
                        with open(prices_fp, 'wb') as F:
                            data['data'] = h
                            pkl.dump(data, F, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass

#
