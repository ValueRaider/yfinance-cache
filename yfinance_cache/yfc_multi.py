import multiprocessing
from functools import partial
import time
import traceback, sys

import pandas as pd
from scipy.stats import mode

from . import yfc_ticker
from . import yfc_utils as yfcu
from . import yfc_dat as yfcd

def reinitialize_locks(locks):
    yfcd.exchange_locks = locks

def download(tickers,
            threads=True, ignore_tz=None, 
            progress=True,
            interval="1d", group_by='column',
            max_age=None,  # defaults to half of interval
            period=None,
            start=None, end=None, prepost=False, actions=True,
            adjust_splits=True, adjust_divs=True,
            keepna=False,
            proxy=None, rounding=False,
            debug=True, quiet=False,
            trigger_at_market_close=False, session=None):

    if ignore_tz is None:
        # Set default value depending on interval
        ignore_tz = interval[1:] not in ['m', 'h']

    # create ticker list
    tickers = tickers if isinstance(tickers, (list, set, tuple)) else tickers.replace(',', ' ').split()
    tickers = list(set([ticker.upper() for ticker in tickers]))

    if progress:
        try:
            import tqdm
            have_tqdm = True
        except Exception:
            have_tqdm = False

    if threads:
        if threads is True:
            threads = multiprocessing.cpu_count()
        if progress:
            queue = multiprocessing.Manager().Queue()
            partial_func = partial(download_one_parallel, queue=queue, 
                                    period=period, interval=interval,
                                    max_age=max_age,
                                    start=start, end=end, prepost=prepost,
                                    actions=actions, adjust_divs=adjust_divs,
                                    adjust_splits=adjust_splits, keepna=keepna,
                                    proxy=proxy,
                                    rounding=rounding, session=session)
            with multiprocessing.Pool(processes=threads, initializer=reinitialize_locks, initargs=(yfcd.exchange_locks,)) as pool:
                result_async = pool.map_async(partial_func, tickers)

                if have_tqdm:
                    r = tqdm.tqdm(tickers)
                else:
                    r = range(len(tickers))
                for i in r:
                    status, value = queue.get()  # Blocks until a value is available
                    if status == 'error':
                        e, tb = value
                        print(tb)
                        sys.exit(1)
                    if not have_tqdm:
                        yfcu.display_progress_bar(i + 1, len(tickers))
                results = result_async.get()
        else:
            partial_func = partial(download_one, 
                                    period=period, interval=interval,
                                    max_age=max_age,
                                    start=start, end=end, prepost=prepost,
                                    actions=actions, adjust_divs=adjust_divs,
                                    adjust_splits=adjust_splits, keepna=keepna,
                                    proxy=proxy,
                                    rounding=rounding, session=session)
            with multiprocessing.Pool(processes=threads) as pool:
                results = pool.map(partial_func, tickers)
        dfs = {tickers[i]:results[i] for i in range(len(tickers))}
    else:
        dfs = {}
        hist_args = {'period':period, 'interval':interval,
                     'max_age':max_age,
                     'start':start, 'end':end, 'prepost':prepost,
                     'actions':actions, 'adjust_divs':adjust_divs,
                     'adjust_splits':adjust_splits, 'keepna':keepna,
                     'proxy':proxy,
                     'rounding':rounding}
        if progress:
            if have_tqdm:
                for tkr in tqdm.tqdm(tickers):
                    df = yfc_ticker.Ticker(tkr, session=session).history(**hist_args)
                    dfs[tkr] = df
            else:
                for i in range(len(tickers)):
                    tkr = tickers[i]
                    df = yfc_ticker.Ticker(tkr, session=session).history(**hist_args)
                    dfs[tkr] = df
                    yfcu.display_progress_bar(i + 1, len(tickers))
        else:
            for i in range(len(tickers)):
                tkr = tickers[i]
                df = yfc_ticker.Ticker(tkr, session=session).history(**hist_args)
                dfs[tkr] = df

    
    if len(tickers) == 1:
        ticker = tickers[0]
        return dfs[ticker]

    reindex_dfs(dfs, ignore_tz)
    try:
        data = pd.concat(dfs.values(), axis=1, sort=True, keys=dfs.keys())
    except Exception:
        reindex_dfs(dfs)
        data = pd.concat(dfs.values(), axis=1, sort=True, keys=dfs.keys())

    if group_by == 'column':
        data.columns = data.columns.swaplevel(0, 1)
        data.sort_index(level=0, axis=1, inplace=True)

    return data


def reindex_dfs(dfs, ignore_tz):
    if ignore_tz:
        for tkr in dfs.keys():
            if (dfs[tkr] is not None) and (not dfs[tkr].empty):
                dfs[tkr].index = dfs[tkr].index.tz_localize(None)
    else:
        # Align each df to most common timezone
        tzs = [df.index.tz for df in dfs.values() if df is not None and not df.empty]
        tz_mode = mode(tzs, keepdims=False).mode
        for tkr in dfs.keys():
            if (dfs[tkr] is not None) and (not dfs[tkr].empty):
                dfs[tkr].index = dfs[tkr].index.tz_convert(tz_mode)

    all_indices = set()
    for df in dfs.values():
        all_indices.update(df.index)
    idx = sorted(all_indices)
    idx = pd.to_datetime(idx)
    for key, df in dfs.items():
        dfs[key] = df.reindex(idx)


def download_one_parallel(ticker, queue, start=None, end=None, max_age=None,
                  adjust_divs=True, adjust_splits=True,
                  actions=False, period="max", interval="1d",
                  prepost=False, proxy=None, rounding=False,
                  keepna=False, session=None):
    try:
        df = download_one(ticker, start=start, end=end, max_age=max_age,
                      adjust_divs=adjust_divs, adjust_splits=adjust_splits,
                      actions=actions, period=period, interval=interval,
                      prepost=prepost, proxy=proxy, rounding=rounding,
                      keepna=keepna, session=session)
        queue.put(('success', 0))
        return df
    except Exception as e:
        tb = traceback.format_exception(type(e), e, e.__traceback__)
        queue.put(('error', (e, ''.join(tb))))
        return e


def download_one(ticker, start=None, end=None, max_age=None,
                  adjust_divs=True, adjust_splits=True,
                  actions=False, period="max", interval="1d",
                  prepost=False, proxy=None, rounding=False,
                  keepna=False, session=None):
    dat = yfc_ticker.Ticker(ticker, session=session)
    df = dat.history(
            period=period, interval=interval, max_age=max_age,
            start=start, end=end, prepost=prepost,
            actions=actions, adjust_divs=adjust_divs,
            adjust_splits=adjust_splits, proxy=proxy,
            rounding=rounding, keepna=keepna
    )
    return df
