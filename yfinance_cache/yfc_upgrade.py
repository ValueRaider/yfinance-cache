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
    actions = ["have-added-repaired-to-cached-divs",
                "have-fixed-prices-final-again",
                "have-reset-xcals-again",
                "have-reset-ccy-cal",
                "have-fixed-24h-prices-final",
                "have-fixed-prices-final-again-x2",
                "have-added-unexpected-intervals-to-options",
                "have-added-event-type-to-earnings-dates",
                "have-fixed-financials-dtypes"
                ]

    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    if not os.path.isdir(yfc_dp):
        return
    for f in os.listdir(yfc_dp):
        if f not in actions:
            os.remove(os.path.join(yfc_dp, f))


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
    contents = os.listdir(dp)
    contents = [x for x in contents if x not in ['options.json', '_YFC_']]

    n = len(contents)
    if n == 0:
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    for d in contents:
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
    contents = [x for x in contents if x not in ['options.json', '_YFC_']]

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
    contents = [x for x in contents if x not in ['options.json', '_YFC_']]

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
    contents = [x for x in contents if x not in ['options.json', '_YFC_']]

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
    contents = os.listdir(dp)
    contents = [x for x in contents if x not in ['options.json', '_YFC_']]

    n = len(contents)
    if n == 0:
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

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


def _fix_prices_final_again_x2():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-fixed-prices-final-again-x2")
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
    contents = [x for x in contents if x not in ['options.json', '_YFC_']]

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
                    tz_key = 'exchangeTimezoneName'
                    if tz_key not in info:
                        tz_key = 'timeZoneFullName'
                    yfct.SetExchangeTzName(info['exchange'], info[tz_key])
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


def _add_unexpected_intervals_to_options():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-added-unexpected-intervals-to-options")
    if os.path.isfile(state_fp):
        return
    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    o = yfcm._option_manager
    if 'calendar' not in o:
        o.calendar.accept_unexpected_Yahoo_intervals = True

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


def _add_event_type_to_earnings_dates():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-added-event-type-to-earnings-dates")
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
    contents = [x for x in contents if x not in ['options.json', '_YFC_']]

    n = len(contents)
    if n == 0:
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    r = n/1200
    print(f"YFC adding 'Event Type' to earnings dates, estimate {round(r):.0f} seconds to process {n} tickers.")

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
            edf_fp = os.path.join(dp, d, 'earnings_dates.pkl')
            if os.path.isfile(edf_fp):
                with open(edf_fp, 'rb') as F:
                    data = pkl.load(F)
                edf = data['data']
                if edf is None or edf.empty:
                    continue
                c = 'Event Type'
                edf_changed = False
                if c not in edf.columns:
                    edf[c] = 'Earnings'
                    edf_changed = True

                if edf_changed:
                    with open(edf_fp, 'wb') as F:
                        data['data'] = edf
                        pkl.dump(data, F, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


def _fix_financials_dtypes():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-fixed-financials-dtypes")
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
    contents = [x for x in contents if x not in ['options.json', '_YFC_']]

    n = len(contents)
    if n == 0:
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    r = n/900
    print(f"YFC converting cached financials to numeric type, estimate {round(r):.0f} seconds to process {n} tickers.")

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
            for x in ['quarterlys', 'annuals']:
                fp = os.path.join(dp, d, f'{x}.pkl')
                if os.path.isfile(fp):
                    with open(fp, 'rb') as F:
                        data = pkl.load(F)
                    for k in data.keys():
                        data[k]['data'] = data[k]['data'].astype('float')
                    with open(fp, 'wb') as F:
                        pkl.dump(data, F, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass

#
