import pickle
import os
from zoneinfo import ZoneInfo
import appdirs

import pandas as pd
import numpy as np
import datetime
import click

from . import yfc_cache_manager as yfcm
# from . import yfc_utils as yfcu
from . import yfc_time as yfct
from . import yfc_dat as yfcd
from . import yfc_ticker as yfc
from . import yfc_prices_manager as yfcp

import yfinance as yf


def _move_cache_dirpath():
    oldCacheDirpath = os.path.join(appdirs.user_cache_dir(), "yfinance-cache")
    cacheDirpath = os.path.join(appdirs.user_cache_dir(), "py-yfinance-cache")
    if not os.path.isdir(cacheDirpath):
        if os.path.isdir(oldCacheDirpath):
            os.rename(oldCacheDirpath, cacheDirpath)
            print("Moved!")


def _sanitise_prices():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-sanitised-prices")
    if os.path.isfile(state_fp):
        return

    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w') as f:
            pass
        return

    # print("Sanitising prices ...")
    # Update: run silently

    tkrs = os.listdir(d)
    for tkr in tkrs:
        tkrd = os.path.join(d, tkr)

        for interval in yfcd.intervalToString.values():
            prices_fp = os.path.join(tkrd, f"history-{interval}.pkl")
            if os.path.isfile(prices_fp):
                with open(prices_fp, 'rb') as f:
                    prices_pkl = pickle.load(f)
                df = prices_pkl["data"]
                df_modified = False

                if "Adj Close" in df.columns:
                    df = df.drop("Adj Close", axis=1)
                    df_modified = True

                if "Repaired?" in df.columns:
                    f_na = df["Repaired?"].isna()
                    if f_na.any():
                        df.loc[f_na, "Repaired?"] = False
                        df_modified = True

                if df_modified:
                    prices_pkl["data"] = df
                    with open(prices_fp, 'wb') as f:
                        pickle.dump(prices_pkl, f, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w') as f:
        pass


def _reset_calendar_cache():
    # Calendar cache was broken because wasn't updating 'sessions_nanos',
    # so have to wipe it.

    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-reset-calendar-cache")
    if os.path.isfile(state_fp):
        return

    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w') as f:
            pass
        return

    # print("Resetting calendar cache ...")
    # Update: silently

    tkrs = os.listdir(d)
    for tkr in tkrs:
        tkrd = os.path.join(d, tkr)
        cal_fp = os.path.join(tkrd, "cal.pkl")
        if os.path.isfile(cal_fp):
            os.remove(cal_fp)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w') as f:
        pass


def _prune_incomplete_daily_intervals():
    d = yfcm.GetCacheDirpath()

    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-pruned-bad-daily-data")
    if os.path.isfile(state_fp):
        return

    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w') as f:
            pass
        return

    print("Scanning cache for incomplete daily+ price data ...")
    tkrs_repaired = set()

    tkrs = os.listdir(d)
    for tkr in tkrs:
        tkrd = os.path.join(d, tkr)
        for f in os.listdir(tkrd):
            fp = os.path.join(tkrd, f)
            f_pieces = f.split('.')
            ext = f_pieces[-1]
            f_base = '.'.join(f_pieces[:-1])
            if ("history" in f_base) and (ext == "pkl"):
                interval = None
                for i, istr in yfcd.intervalToString.items():
                    if f_base.endswith(istr):
                        interval = i
                        break
                if interval is None:
                    raise Exception("Failed to map '{}' to Interval".format(f_base))
                itd = yfcd.intervalToTimedelta[interval]

                pkData = None
                with open(fp, 'rb') as f:
                    pkData = pickle.load(f)
                    h = pkData["data"]
                h_modified = False

                with open(tkrd+"/info.pkl", 'rb') as f:
                    info = pickle.load(f)["data"]

                # Scan for any daily/weekly intervals marked final but not
                # updated after midnight
                if itd >= datetime.timedelta(days=1):
                    tz_exchange = ZoneInfo(info["exchangeTimezoneName"])
                    f_final = h["Final?"].values
                    f_sameDay = h["FetchDate"].dt.tz_convert(tz_exchange).dt.date == h.index.date
                    f_bad = f_final & f_sameDay
                    if f_bad.any():
                        idx = np.where(f_bad)[0][0]
                        if idx == 0:
                            h = None
                        else:
                            h = h.loc[:h.index[idx-1]]
                        h_modified = True

                    if h_modified:
                        tkrs_repaired.add(tkr)
                        if h is None:
                            os.remove(fp)
                        else:
                            with open(fp, 'wb') as f:
                                pkData["data"] = h
                                pickle.dump(pkData, f, 4)

    if len(tkrs_repaired) == 0:
        print("No problems founds")
    else:
        print("Pruned bad daily+ intervals from these tickers:")
        print(sorted(list(tkrs_repaired)))

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w') as f:
        pass


def _separate_events_from_prices():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-separated-events-from-prices")
    if os.path.isfile(state_fp):
        return

    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w') as f:
            pass
        return

    tkrs = os.listdir(d)
    if len(tkrs) == 1 and tkrs[0] == "_YFC_":
        with open(state_fp, 'w') as f:
            pass
        return

    msg = "IMPORTANT: Old version of yfinance_cache had bug in handling dividends & stock splits."\
          f" Fix requires restructuring YFC cache. If you want to backup cache folder first, it's at: {d}"\
          "\nProceed? Can't go back."
    r = click.confirm(msg, default=False)
    if not r:
        quit()

    print("Upgrading dividends/splits management, just a few seconds ...")
    print("")
    print("After upgrade, you can run yfc.verify_cached_tickers_prices() (or Ticker.verify_cached_prices()) to compared cached prices against Yahoo Finance and discard incorrect data.")

    for tkr in tkrs:
        tkrd = os.path.join(d, tkr)

        divs_fp = os.path.join(tkrd, "dividends.pkl")
        splits_fp = os.path.join(tkrd, "splits.pkl")
        prices_fp = os.path.join(tkrd, "history-1d.pkl")
        if not os.path.isfile(divs_fp) and os.path.isfile(prices_fp):
            # Need to separate
            with open(prices_fp, 'rb') as f:
                dailyPklData = pickle.load(f)
            h = dailyPklData["data"]
            divs_df = h[h["Dividends"] != 0][["Dividends"]].copy()
            splits_df = h[h["Stock Splits"] != 0][["Stock Splits"]].copy()

            fp = os.path.join(tkrd, "fast_info.pkl")
            if os.path.isfile(fp):
                with open(fp, 'rb') as f:
                    tz = ZoneInfo(pickle.load(f)["data"]["timezone"])
            else:
                fp = os.path.join(tkrd, "info.pkl")
                with open(fp, 'rb') as f:
                    info = pickle.load(f)["data"]
                    if "exchangeTimezoneName" in info:
                        tz = ZoneInfo(info["exchangeTimezoneName"])
                    else:
                        tz = ZoneInfo(info["timeZoneFullName"])

            # Add 'FetchDate'
            if divs_df.empty:
                divs_fetch_dt = pd.NaT
            else:
                divs_fetch_dt = datetime.datetime.combine(divs_df.index[-1], datetime.time(10), tz)
            divs_df["FetchDate"] = divs_fetch_dt
            if divs_df.shape[0] > 0:
                divs_df.index = divs_df.index.tz_convert(tz)
            divs_df["Supersede?"] = False
            if splits_df.empty:
                splits_fetch_dt = pd.NaT
            else:
                splits_fetch_dt = datetime.datetime.combine(splits_df.index[-1], datetime.time(10), tz)
            splits_df["FetchDate"] = splits_fetch_dt
            splits_df["Supersede?"] = False
            if splits_df.shape[0] > 0:
                splits_df.index = splits_df.index.tz_convert(tz)

            divs_pkl = {"data": divs_df, "metadata": None}
            splits_pkl = {"data": splits_df, "metadata": None}

            # Write separated events to file:
            with open(divs_fp, 'wb') as f:
                pickle.dump(divs_pkl, f, 4)
            with open(splits_fp, 'wb') as f:
                pickle.dump(splits_pkl, f, 4)

            # Replace 'LastAdjustD' with 'LastDivAdjustDt' & 'LastSplitAdjustDt'
            # Also add field for tracking whether 'repair' check run
            for interval in yfcd.intervalToString.values():
                prices_fp = os.path.join(tkrd, f"history-{interval}.pkl")
                if os.path.isfile(prices_fp):
                    with open(prices_fp, 'rb') as f:
                        prices_pkl = pickle.load(f)

                    lastAdjustD = prices_pkl["metadata"]["LastAdjustD"]
                    lastDivAdjustD = lastAdjustD  # default
                    if divs_df.shape[0] > 0:
                        df = divs_df[divs_df.index.tz_convert(tz).date <= lastAdjustD]
                        if df.shape[0] > 0:
                            lastDivAdjustD = df.index[-1].date()
                    lastSplitAdjustD = lastAdjustD  # default
                    if splits_df.shape[0] > 0:
                        splits_df.index = splits_df.index.tz_convert(tz)
                        df = splits_df[splits_df.index.tz_convert(tz).date <= lastAdjustD]
                        if df.shape[0] > 0:
                            lastDivAdjustD = df.index[-1].date()

                    df = prices_pkl["data"]

                    df["FetchDate"] = df["FetchDate"].dt.tz_convert(tz)  # Ensure ZoneInfo
                    df["C-Check?"] = False

                    lastDivAdjustDt = datetime.datetime.combine(lastDivAdjustD, datetime.time(10), tz)
                    lastSplitAdjustDt = datetime.datetime.combine(lastSplitAdjustD, datetime.time(10), tz)

                    df["LastDivAdjustDt"] = np.maximum(lastDivAdjustDt, df["FetchDate"])
                    df["LastSplitAdjustDt"] = np.maximum(lastSplitAdjustDt, df["FetchDate"])

                    prices_pkl["data"] = df
                    with open(prices_fp, 'wb') as f:
                        pickle.dump(prices_pkl, f, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w') as f:
        pass


def _fix_dividend_adjust():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-fixed-dividend-adjustment")
    if os.path.isfile(state_fp):
        return

    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w') as f:
            pass
        return

    # Precondition:
    _separate_events_from_prices()

    # print("Fixing dividend adjustment, just a few seconds ...")
    # Update: run silently

    for tkr in os.listdir(d):
        tkrd = os.path.join(d, tkr)

        # First, recalculate dividend adjustment
        divs_fp = os.path.join(tkrd, "dividends.pkl")
        if not os.path.isfile(divs_fp):
            continue

        with open(divs_fp, 'rb') as f:
            divsPklData = pickle.load(f)
        divs_df = divsPklData["data"]

        if divs_df.empty:
            # Just add 'Back Adj.' column and continue
            divs_df["Back Adj."] = 1.0
            divsPklData["data"] = divs_df
            with open(divs_fp, 'wb') as f:
                pickle.dump(divsPklData, f, 4)

            # Also ensure no evidence of dividends in prices:
            for interval in yfcd.Interval:
                istr = yfcd.intervalToString[interval]
                prices_fp = os.path.join(tkrd, f"history-{istr}.pkl")
                if not os.path.isfile(prices_fp):
                    continue
                with open(prices_fp, 'rb') as f:
                    pricesPklData = pickle.load(f)
                df = pricesPklData["data"]
                df_modified = False
                if (df["CDF"].to_numpy() != 1).any():
                    df["CDF"] = 1
                    df_modified = True
                if (df["Dividends"].to_numpy() != 0).any():
                    df["Dividends"] = 0
                    df_modified = True
                if df_modified:
                    pricesPklData["data"] = df
                    with open(prices_fp, 'wb') as f:
                        pickle.dump(pricesPklData, f, 4)

            continue

        # First, recalculate dividend-adjustment factor:
        prices_fp = os.path.join(tkrd, "history-1d.pkl")
        with open(prices_fp, 'rb') as f:
            pricesPklData = pickle.load(f)
        prices_1d_df = pricesPklData["data"]
        divs_df["Close day before"] = 0.0
        for dt in divs_df.index:
            if dt == prices_1d_df.index[0]:
                idx = 0
            else:
                idx = prices_1d_df.index.get_loc(dt) - 1
            close_day_before = prices_1d_df["Close"].iloc[idx]
            divs_df.loc[dt, "Close day before"] = close_day_before
        divs_df["Back Adj."] = 1.0 - divs_df["Dividends"].to_numpy() / divs_df["Close day before"].to_numpy()
        divs_df = divs_df.drop("Close day before", axis=1)
        divsPklData["data"] = divs_df
        with open(divs_fp, 'wb') as f:
            pickle.dump(divsPklData, f, 4)

        # Next, copy it into 1D price table to recalc CDF:
        divs_df["_date"] = divs_df.index.date
        prices_1d_df["_date"] = prices_1d_df.index.date
        prices_1d_df["_indexBackup"] = prices_1d_df.index
        prices_1d_df = prices_1d_df.merge(divs_df[["Back Adj.", "_date"]], how="left").drop("_date", axis=1)
        prices_1d_df.index = prices_1d_df["_indexBackup"] ; prices_1d_df.index.name = "Date" ; prices_1d_df = prices_1d_df.drop("_indexBackup", axis=1)
        prices_1d_df.loc[prices_1d_df["Back Adj."].isna().to_numpy(), "Back Adj."] = 1.0
        cdf = prices_1d_df["Back Adj."].shift(-1, fill_value=1.0).sort_index(ascending=False).cumprod().sort_index(ascending=True)
        prices_1d_df["CDF"] = cdf
        prices_1d_df = prices_1d_df.drop("Back Adj.", axis=1)
        pricesPklData["data"] = prices_1d_df
        with open(prices_fp, 'wb') as f:
            pickle.dump(pricesPklData, f, 4)

        # Next, copy into all other prices tables:
        prices_1d_df["_date"] = prices_1d_df.index.date
        for interval in yfcd.Interval:
            if interval == yfcd.Interval.Days1:
                continue
            istr = yfcd.intervalToString[interval]
            itd = yfcd.intervalToTimedelta[interval]
            prices_fp = os.path.join(tkrd, f"history-{istr}.pkl")
            if not os.path.isfile(prices_fp):
                continue

            with open(prices_fp, 'rb') as f:
                pricesPklData = pickle.load(f)
            prices_df = pricesPklData["data"]

            # Process intraday separate to interday:
            if itd < datetime.timedelta(days=1):
                # intraday - copy CDF from daily
                prices_df["_date"] = prices_df.index.date
                prices_df = prices_df.drop("CDF", axis=1)
                prices_df["_indexBackup"] = prices_df.index
                prices_df = prices_df.merge(prices_1d_df[["_date", "CDF"]], how="left", on="_date")
                prices_df.index = prices_df["_indexBackup"] ; prices_df.index.name = "Date" ; prices_df = prices_df.drop("_indexBackup", axis=1)
            else:
                # interday - recalc CDF using these prices
                f_div = prices_df["Dividends"] != 0
                if not f_div.any():
                    continue
                c = "Back Adj."
                prices_df[c] = 1.0
                prices_df.loc[f_div, c] = 1.0 - prices_df.loc[f_div, "Dividends"] / prices_df.loc[f_div, "Close"]
                cdf = prices_df["Back Adj."].shift(-1, fill_value=1.0).sort_index(ascending=False).cumprod().sort_index(ascending=True)
                prices_df["CDF"] = cdf
                prices_df = prices_df.drop(c, axis=1)

            pricesPklData["data"] = prices_df
            with open(prices_fp, 'wb') as f:
                pickle.dump(pricesPklData, f, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w') as f:
        pass


def _fix_listing_date():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-fixed-listing-dates")
    if os.path.isfile(state_fp):
        return

    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w') as f:
            pass
        return

    for tkr in os.listdir(d):
        if tkr.startswith("EXCHANGE-") or tkr.startswith('_'):
            # Not a security
            continue

        tkrd = os.path.join(d, tkr)

        istr = yfcd.intervalToString[yfcd.Interval.Days1]
        prices_fp = os.path.join(tkrd, f"history-{istr}.pkl")
        if not os.path.isfile(prices_fp):
            # No data to prune
            continue

        lst_fp = os.path.join(tkrd, "listing_date.json")
        if os.path.isfile(lst_fp):
            dat = yf.Ticker(tkr)
            df = dat.history(period="1d")
            if "firstTradeDate" not in dat.history_metadata:
                continue
            listing_date = dat.history_metadata["firstTradeDate"]
            if isinstance(listing_date, int):
                fp_info = os.path.join(tkrd, "fast_info.pkl")
                if os.path.isfile(fp_info):
                    with open(fp_info, 'rb') as f:
                        tz = pickle.load(f)["data"]["timezone"]
                else:
                    fp_info = os.path.join(tkrd, "info.pkl")
                    with open(fp_info, 'rb') as f:
                        info = pickle.load(f)["data"]
                        if "exchangeTimezoneName" in info:
                            tz = info["exchangeTimezoneName"]
                        else:
                            tz = info["timeZoneFullName"]
                listing_date = pd.to_datetime(listing_date, unit='s', utc=True).tz_convert(tz)
            yfcm.StoreCacheDatum(tkr, "listing_date", listing_date.date())

            for interval in yfcd.Interval:
                istr = yfcd.intervalToString[interval]
                prices_fp = os.path.join(tkrd, f"history-{istr}.pkl")
                if not os.path.isfile(prices_fp):
                    continue
                with open(prices_fp, 'rb') as f:
                    prices_pkl = pickle.load(f)
                df = prices_pkl["data"]
                if df.index[0] < listing_date:
                    print("Pruning " + prices_fp)
                    df = df.loc[listing_date:]
                    prices_pkl["data"] = df
                    with open(prices_fp, 'wb') as f:
                        pickle.dump(prices_pkl, f, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w') as f:
        pass


def _upgrade_divs_splits_supersede():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-upgraded-div-split-supersede")
    if os.path.isfile(state_fp):
        return

    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w') as f:
            pass
        return

    for tkr in os.listdir(d):
        if tkr.startswith("EXCHANGE-") or tkr.startswith('_'):
            # Not a security
            continue

        tkrd = os.path.join(d, tkr)

        divs_fp = os.path.join(tkrd, "dividends.pkl")
        if os.path.isfile(divs_fp):
            with open(divs_fp, 'rb') as f:
                divsPklData = pickle.load(f)
            divs_df = divsPklData["data"]

            if "Supersede?" in divs_df.columns:
                divs_df = divs_df.drop("Supersede?", axis=1)
            divs_df["Superseded back adj."] = 0.0
            divs_df["Superseded div FetchDate"] = pd.NaT

            divsPklData["data"] = divs_df
            with open(divs_fp, 'wb') as f:
                pickle.dump(divsPklData, f, 4)

        splits_fp = os.path.join(tkrd, "splits.pkl")
        if os.path.isfile(splits_fp):
            with open(splits_fp, 'rb') as f:
                splitsPklData = pickle.load(f)
            splits_df = splitsPklData["data"]

            if "Supersede?" in splits_df.columns:
                splits_df = splits_df.drop("Supersede?", axis=1)
            splits_df["Superseded split"] = 0.0
            splits_df["Superseded split FetchDate"] = pd.NaT

            splitsPklData["data"] = splits_df
            with open(splits_fp, 'wb') as f:
                pickle.dump(splitsPklData, f, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w') as f:
        pass


def _add_repaired_column():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-added-repaired-column")
    if os.path.isfile(state_fp):
        return

    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w') as f:
            pass
        return

    tkrs = os.listdir(d)
    for tkr in tkrs:
        tkrd = os.path.join(d, tkr)

        for interval in yfcd.intervalToString.values():
            prices_fp = os.path.join(tkrd, f"history-{interval}.pkl")
            if os.path.isfile(prices_fp):
                with open(prices_fp, 'rb') as f:
                    prices_pkl = pickle.load(f)
                df = prices_pkl["data"]
                df_modified = False

                if not "Repaired?" in df.columns:
                    df["Repaired?"] = False
                    df_modified = True

                if df_modified:
                    prices_pkl["data"] = df
                    with open(prices_fp, 'wb') as f:
                        pickle.dump(prices_pkl, f, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w') as f:
        pass


def _recalc_final_column():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-recalced-final-column")
    if os.path.isfile(state_fp):
        return

    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w') as f:
            pass
        return

    # print("Running: _recalc_final_column()")
    print("Fixing 'Final?' column, may take a few minutes (~500 symbols per minute)")

    tkrs = os.listdir(d)
    try:
        import tqdm
        tkrs = tqdm.tqdm(tkrs)
    except:
        pass
    for tkr in tkrs:
        tkrd = os.path.join(d, tkr)

        exchange = None

        for interval in yfcd.intervalToString.keys():
            istr = yfcd.intervalToString[interval]
            prices_fp = os.path.join(tkrd, f"history-{istr}.pkl")
            if os.path.isfile(prices_fp):
                with open(prices_fp, 'rb') as f:
                    prices_pkl = pickle.load(f)
                df = prices_pkl["data"]
                df_modified = False

                if exchange is None:
                    fp = os.path.join(tkrd, "fast_info.pkl")
                    if not os.path.isfile(fp):
                        fp = os.path.join(tkrd, "info.pkl")
                    with open(fp, 'rb') as f:
                        exchange = pickle.load(f)["data"]["exchange"]

                interday = interval in [yfcd.Interval.Days1, yfcd.Interval.Week, yfcd.Interval.Months1, yfcd.Interval.Months3]
                intervals = yfct.GetTimestampCurrentInterval_batch(exchange, df.index.to_numpy(), interval, discardTimes=interday, ignore_breaks=True)
                lastDataDts = yfct.CalcIntervalLastDataDt_batch(exchange, intervals["interval_open"].to_numpy(), interval)
                f_na = intervals["interval_open"].isna().to_numpy()
                if f_na.any():
                    # Hacky solution to handle xcal having incorrect schedule, for valid Yahoo data
                    itd = yfcd.intervalToTimedelta[interval]
                    lastDataDts[f_na] = intervals.index[f_na] + itd
                    if not interday:
                        lastDataDts[f_na] += yfct.GetExchangeDataDelay(exchange)
                        # For some exchanges, Yahoo has trades that occurred soon afer official market close, e.g. Johannesburg:
                        if exchange in ["JNB"]:
                            lastDataDts[f_na] += timedelta(minutes=15)
                data_final = (df["FetchDate"] >= lastDataDts).to_numpy()
                df_modified = (data_final != df["Final?"].to_numpy()).any()
                if df_modified:
                    df["Final?"] = data_final
                    prices_pkl["data"] = df
                    with open(prices_fp, 'wb') as f:
                        pickle.dump(prices_pkl, f, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w') as f:
        pass


def _upgrade_divs_supersede_again():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-upgraded-div-supersede-again")
    if os.path.isfile(state_fp):
        return

    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w') as f:
            pass
        return

    for tkr in os.listdir(d):
        if tkr.startswith("EXCHANGE-") or tkr.startswith('_'):
            # Not a security
            continue

        tkrd = os.path.join(d, tkr)

        divs_fp = os.path.join(tkrd, "dividends.pkl")
        if os.path.isfile(divs_fp):
            with open(divs_fp, 'rb') as f:
                divsPklData = pickle.load(f)
            divs_df = divsPklData["data"]

            # Add column "Superseded div"
            divs_df["Superseded div"] = 0.0
            f = divs_df["Superseded back adj."].to_numpy() != 0.0
            if f.any():
                ratio = (1.0-divs_df.loc[f,"Superseded back adj."].to_numpy()) / (1.0-divs_df.loc[f,"Back Adj."].to_numpy())
                divs_df.loc[f,"Superseded div"] = ratio * divs_df.loc[f,"Dividends"].to_numpy()

            divsPklData["data"] = divs_df
            with open(divs_fp, 'wb') as f:
                pickle.dump(divsPklData, f, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w') as f:
        pass


def _recalc_multiday_final_column():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-recalced-multiday-final-column")
    if os.path.isfile(state_fp):
        return

    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w') as f:
            pass
        return

    # print("Running: _recalc_final_column()")
    print("Fixing 'Final?' column, may take a few minutes (~500 symbols per minute)")

    keys = []
    for k in yfcd.intervalToString.keys():
        istr = yfcd.intervalToString[k]
        if istr.endswith('wk') or istr.endswith('mo') or istr.endswith('y'):
            keys.append(k)

    tkrs = os.listdir(d)
    try:
        import tqdm
        tkrs = tqdm.tqdm(tkrs)
    except:
        pass
    for tkr in tkrs:
        tkrd = os.path.join(d, tkr)

        exchange = None

        for interval in keys:
            istr = yfcd.intervalToString[interval]
            prices_fp = os.path.join(tkrd, f"history-{istr}.pkl")
            if os.path.isfile(prices_fp):
                with open(prices_fp, 'rb') as f:
                    prices_pkl = pickle.load(f)
                df = prices_pkl["data"]
                df_modified = False

                if exchange is None:
                    fp = os.path.join(tkrd, "fast_info.pkl")
                    if not os.path.isfile(fp):
                        fp = os.path.join(tkrd, "info.pkl")
                    with open(fp, 'rb') as f:
                        exchange = pickle.load(f)["data"]["exchange"]

                interday = interval in [yfcd.Interval.Days1, yfcd.Interval.Week, yfcd.Interval.Months1, yfcd.Interval.Months3]
                intervals = yfct.GetTimestampCurrentInterval_batch(exchange, df.index.to_numpy(), interval, discardTimes=interday, ignore_breaks=True)
                lastDataDts = yfct.CalcIntervalLastDataDt_batch(exchange, intervals["interval_open"].to_numpy(), interval)
                f_na = intervals["interval_open"].isna().to_numpy()
                if f_na.any():
                    # Hacky solution to handle xcal having incorrect schedule, for valid Yahoo data
                    itd = yfcd.intervalToTimedelta[interval]
                    lastDataDts[f_na] = intervals.index[f_na] + itd
                    if not interday:
                        lastDataDts[f_na] += yfct.GetExchangeDataDelay(exchange)
                        # For some exchanges, Yahoo has trades that occurred soon afer official market close, e.g. Johannesburg:
                        if exchange in ["JNB"]:
                            lastDataDts[f_na] += timedelta(minutes=15)
                data_final = (df["FetchDate"] >= lastDataDts).to_numpy()
                df_modified = (data_final != df["Final?"].to_numpy()).any()
                if df_modified:
                    df["Final?"] = data_final
                    prices_pkl["data"] = df
                    with open(prices_fp, 'wb') as f:
                        pickle.dump(prices_pkl, f, 4)

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w') as f:
        pass

