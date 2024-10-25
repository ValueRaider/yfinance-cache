import yfinance as yf

from . import yfc_dat as yfcd
from . import yfc_cache_manager as yfcm
from . import yfc_utils as yfcu

import numpy as np
import pandas as pd
import scipy.stats as stats
from time import sleep
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import os
from statistics import mean
import math
from decimal import Decimal
from pprint import pprint
import pulp


d_today = date.today()
yf_spam_window = timedelta(days=7)
# give Yahoo time to update their financials
yf_min_grace_days_period = timedelta(days=2)
yf_max_grace_days_period = timedelta(days=28)
company_release_delay = timedelta(days=2)


print_fetches = False
# print_fetches = True


def sort_estimates(lst):
    if len(lst) < 2:
        return lst

    pivot_index = len(lst) // 2
    pivot = lst[pivot_index]

    less = []
    greater = []

    for i, val in enumerate(lst):
        if i == pivot_index:
            continue
        try:
            less_than = val < pivot
        except yfcd.AmbiguousComparisonException:
            if hasattr(val, "prob_lt"):
                less_than = val.prob_lt(pivot) > 0.5
            else:
                less_than = pivot.prob_gt(val) > 0.5
        if less_than:
            less.append(val)
        else:
            greater.append(val)

    return sort_estimates(less) + [pivot] + sort_estimates(greater)


class EarningsRelease():
    def __init__(self, interval, period_end, release_date, full_year_end):
        if not isinstance(period_end, (date, yfcd.DateEstimate)):
            raise Exception("'period_end' must be a 'yfcd.DateEstimate' or date object or None, not {0}".format(type(period_end)))
        if (release_date is not None):
            if not isinstance(release_date, (date, yfcd.DateEstimate)):
                raise Exception("'release_date' must be a 'yfcd.DateEstimate' or date object or None, not {0}".format(type(release_date)))
            if release_date < period_end:
                raise Exception("release_date={0} cannot occur before period_end={1}".format(release_date, period_end))
            if release_date > (period_end + timedelta(days=90)):
                raise Exception("release_date={0} shouldn't occur 90 days after period_end={1}".format(release_date, period_end))
        if not isinstance(full_year_end, date):
            raise Exception("'full_year_end' must be a date object or None, not {0}".format(type(full_year_end)))
        self.interval = interval
        self.period_end = period_end
        self.release_date = release_date
        self.full_year_end = full_year_end

    def __str__(self):
        s = f'{self.interval} earnings'
        s += f" ending {self.period_end}"
        s += " released"
        s += " ?" if self.release_date is None else f" {self.release_date}"
        if self.release_date is not None:
            delay = self.release_date - self.period_end
            if isinstance(delay, (timedelta, pd.Timedelta)):
                delay = f"{delay.days} days"
            s += f" (delay = {delay})"
        return s

    def __repr__(self):
        return self.__str__()

    def __lt__(self, other):
        return self.period_end < other.period_end or (self.period_end == other.period_end and self.release_date < other.release_date)

    def __le__(self, other):
        return (self == other) or (self < other)

    def __eq__(self, other):
        return self.period_end == other.period_end and self.release_date == other.release_date

    def __gt__(self, other):
        return self.period_end > other.period_end or (self.period_end == other.period_end and self.release_date > other.release_date)

    def __ge__(self, other):
        return (self == other) or (self > other)

    def is_end_of_year(self):
        r_is_end_of_year = False
        rpe = self.period_end
        diff = (rpe - self.full_year_end)
        diff += timedelta(days=365)  # just in case is negative
        diff = diff % timedelta(days=365)
        tol = 35
        try:
            if (diff > timedelta(days=-tol) and diff < timedelta(days=tol)) or \
                (diff > timedelta(days=365-tol) and diff < timedelta(days=365+tol)):
                # Aligns with annual release date
                r_is_end_of_year = True
        except yfcd.AmbiguousComparisonException:
            r_is_end_of_year = True
        return r_is_end_of_year

    def year_pct(self):
        if self.is_end_of_year():
            return 1.0
        else:
            rpe = self.period_end
            diff = (rpe - self.full_year_end)
            diff += timedelta(days=365)  # just in case is negative
            diff = diff % timedelta(days=365)
            tol = 35
            try:
                if diff > timedelta(days=91-tol) and diff < timedelta(days=91+tol):
                    return 0.25
            except yfcd.AmbiguousComparisonException:
                return 0.25
            try:
                if diff > timedelta(days=182-tol) and diff < timedelta(days=182+tol):
                    return 0.5
            except yfcd.AmbiguousComparisonException:
                return 0.5
            try:
                if diff > timedelta(days=274-tol) and diff < timedelta(days=274+tol):
                    return 0.75
            except yfcd.AmbiguousComparisonException:
                return 0.75
            raise Exception(f'Failed to determine % progress through year of release (diff={diff}): {self.__str__()}')


interval_str_to_days = {}
interval_str_to_days['ANNUAL'] = yfcd.ComparableRelativedelta(years=1)
interval_str_to_days['HALF'] = yfcd.ComparableRelativedelta(months=6)
interval_str_to_days['QUART'] = yfcd.ComparableRelativedelta(months=3)


class FinancialsManager:
    def __init__(self, ticker, exchange, tzName, session):
        yfcu.TypeCheckStr(ticker, "ticker")
        yfcu.TypeCheckStr(exchange, "exchange")
        yfcu.TypeCheckStr(tzName, "tzName")

        self.ticker = ticker
        self.exchange = exchange
        self.tzName = tzName
        self.session = session
        self.dat = yf.Ticker(self.ticker, session=self.session)

        # self._earnings = None
        # self._quarterly_earnings = None
        self._income_stmt = None
        self._quarterly_income_stmt = None
        self._balance_sheet = None
        self._quarterly_balance_sheet = None
        self._cashflow = None
        self._quarterly_cashflow = None

        self._earnings_dates = None
        self._calendar = None
        self._calendar_clean = None

        self._pruned_tbl_cache = {}
        self._fin_tbl_cache = {}

    def get_income_stmt(self, refresh=True):
        if self._income_stmt is not None:
            return self._income_stmt
        self._income_stmt = self._get_fin_table(yfcd.Financials.IncomeStmt, yfcd.ReportingPeriod.Full, refresh)
        return self._income_stmt

    def get_quarterly_income_stmt(self, refresh=True):
        if self._quarterly_income_stmt is not None:
            return self._quarterly_income_stmt
        self._quarterly_income_stmt = self._get_fin_table(yfcd.Financials.IncomeStmt, yfcd.ReportingPeriod.Interim, refresh)
        return self._quarterly_income_stmt

    def get_balance_sheet(self, refresh=True):
        if self._balance_sheet is not None:
            return self._balance_sheet
        self._balance_sheet = self._get_fin_table(yfcd.Financials.BalanceSheet, yfcd.ReportingPeriod.Full, refresh)
        return self._balance_sheet

    def get_quarterly_balance_sheet(self, refresh=True):
        if self._quarterly_balance_sheet is not None:
            return self._quarterly_balance_sheet
        self._quarterly_balance_sheet = self._get_fin_table(yfcd.Financials.BalanceSheet, yfcd.ReportingPeriod.Interim, refresh)
        return self._quarterly_balance_sheet

    def get_cashflow(self, refresh=True):
        if self._cashflow is not None:
            return self._cashflow
        self._cashflow = self._get_fin_table(yfcd.Financials.CashFlow, yfcd.ReportingPeriod.Full, refresh)
        return self._cashflow

    def get_quarterly_cashflow(self, refresh=True):
        if self._quarterly_cashflow is not None:
            return self._quarterly_cashflow
        self._quarterly_cashflow = self._get_fin_table(yfcd.Financials.CashFlow, yfcd.ReportingPeriod.Interim, refresh)
        return self._quarterly_cashflow

    def _get_fin_table(self, finType, period, refresh=True):
        debug = False
        # debug = True

        if debug:
            print(f"{self.ticker}: _get_fin_table({finType}, {period}, refresh={refresh})")

        if not isinstance(finType, yfcd.Financials):
            raise Exception('Argument finType must be type Financials')
        if not isinstance(period, yfcd.ReportingPeriod):
            raise Exception('Argument period must be type ReportingPeriod')

        cache_key = (finType, period, refresh)
        if cache_key in self._fin_tbl_cache:
            return self._fin_tbl_cache[cache_key]
        if not refresh:
            cache_key2 = (finType, period, True)
            if cache_key2 in self._fin_tbl_cache:
                return self._fin_tbl_cache[cache_key2]

        if period == yfcd.ReportingPeriod.Interim:
            name = 'quarterly_'
        else:
            name = ''
        if finType == yfcd.Financials.IncomeStmt:
            name += 'income_stmt'
        elif finType == yfcd.Financials.BalanceSheet:
            name += 'balance_sheet'
        elif finType == yfcd.Financials.CashFlow:
            name += 'cashflow'

        df, md = None, None
        if yfcm.IsDatumCached(self.ticker, name):
            df, md = yfcm.ReadCacheDatum(self.ticker, name, True)
            mod_dt = None
            if md is None or len(md) == 0:
                # Fix metadata
                fp = yfcm.GetFilepath(self.ticker, name)
                mod_dt = datetime.fromtimestamp(os.path.getmtime(fp)).astimezone()
                md = {'FetchDates':{}}
                for dt in df.columns:
                    md['FetchDates'][dt] = mod_dt
                yfcm.WriteCacheMetadata(self.ticker, name, 'FetchDates', md['FetchDates'])
                md['LastFetch'] = mod_dt
                yfcm.WriteCacheMetadata(self.ticker, name, 'LastFetch', md['LastFetch'])
            elif 'FetchDates' not in md:
                if mod_dt is None:
                    fp = yfcm.GetFilepath(self.ticker, name)
                    mod_dt = datetime.fromtimestamp(os.path.getmtime(fp)).astimezone()
                for dt in df.columns:
                    md['FetchDates'][dt] = mod_dt
                yfcm.WriteCacheMetadata(self.ticker, name, 'FetchDates', md['FetchDates'])
            elif 'LastFetch' not in md:
                if mod_dt is None:
                    fp = yfcm.GetFilepath(self.ticker, name)
                    mod_dt = datetime.fromtimestamp(os.path.getmtime(fp)).astimezone()
                md['LastFetch'] = mod_dt
                yfcm.WriteCacheMetadata(self.ticker, name, 'LastFetch', md['LastFetch'])

            if md['LastFetch'].tzinfo is None:
                md['LastFetch'] = md['LastFetch'].astimezone()
                yfcm.WriteCacheMetadata(self.ticker, name, 'LastFetch', md['LastFetch'])

        do_fetch = False
        if df is None:
            do_fetch = True
        elif refresh:
            dt_now = pd.Timestamp.utcnow().tz_convert(self.tzName)
            if df.empty:
                # Nothing to estimate releases on, so just periodically check
                try:
                    age = dt_now - md["LastFetch"]
                except Exception:
                    print(md)
                    raise
                if age > pd.Timedelta(days=30):
                    do_fetch = True
            else:
                td_1d = pd.Timedelta(1, unit='D')
                releases = self.get_release_dates(period, refresh=False)
                next_release = None
                if releases is None:
                    # Use crude logic to estimate when to re-fetch
                    if 'LastFetch' in md.keys():
                        do_fetch = md['LastFetch'] < (dt_now - td_1d*30)
                    else:
                        do_fetch = True
                else:
                    releases = sorted(releases)
                    # last_d = df.columns.max().date()
                    # Update: analyse pruned dates:
                    last_d = self._prune_yf_financial_df(df).columns.max().date()
                    # Find next release after last fetch:
                    for r in releases:
                        try:
                            if r.period_end <= last_d:
                                continue
                        except yfcd.AmbiguousComparisonException:
                            # Treat as match
                            continue
                        next_release = r
                        break
                    if next_release is None:
                        pprint(releases)
                        print("- last_d =", last_d)
                        raise Exception('Failed to determine next release after cached financials')
                    if debug:
                        print("- last_d =", last_d, ", last_fetch =", md['LastFetch'].date())
                        print("- next_release:", next_release)
                    rd = next_release.release_date
                    try:
                        next_release_in_future = rd > d_today
                    except yfcd.AmbiguousComparisonException:
                        next_release_in_future = False
                    if debug:
                        print("- next_release_in_future =", next_release_in_future)
                    if not next_release_in_future:
                        try:
                            fetched_long_after_release = md['LastFetch'].date() > (r.release_date + yf_max_grace_days_period)
                        except yfcd.AmbiguousComparisonException:
                            fetched_long_after_release = (r.release_date + yf_max_grace_days_period).prob_lt(md['LastFetch'].date()) > 0.5
                        if debug:
                            print("- fetched_long_after_release =", fetched_long_after_release)
                        if fetched_long_after_release:
                            # Yahoo should have returned the expected data in previous fetch!
                            # So keep re-fetching but with longer delays
                            refetch_delay = yf_min_grace_days_period
                            try:
                                refetch_delay = 0.5 * (md['LastFetch'].date() - (r.release_date + yf_min_grace_days_period))
                            except yfcd.AmbiguousComparisonException:
                                pass
                            if debug:
                                print("- refetch_delay =", refetch_delay)
                            try:
                                fair_to_expect_Yahoo_updated = (md['LastFetch'].date() + refetch_delay) < d_today
                            except yfcd.AmbiguousComparisonException:
                                fair_to_expect_Yahoo_updated = True
                        else:
                            try:
                                fair_to_expect_Yahoo_updated = (d_today-rd) >= yf_min_grace_days_period
                            except yfcd.AmbiguousComparisonException:
                                fair_to_expect_Yahoo_updated = True
                        if debug:
                            print("- fair_to_expect_Yahoo_updated =", fair_to_expect_Yahoo_updated)
                        if fair_to_expect_Yahoo_updated:
                            if debug:
                                print("- expect new release, but did we already fetch recently?")
                            if md['LastFetch'] < (dt_now - yf_spam_window):
                                do_fetch = True

        if debug:
            print("- do_fetch =", do_fetch)
        if do_fetch:
            if print_fetches:
                msg = f"{self.ticker}: fetching {name}"
                if md is not None:
                    msg += f" (last fetch = {md['LastFetch']})"
                print(msg)
            df_new = getattr(self.dat, name)
            fetch_dt = pd.Timestamp.utcnow().tz_convert(self.tzName)
            if md is None:
                md = {'FetchDates':{}}
            for dt in df_new.columns:
                md['FetchDates'][dt] = fetch_dt
            md['LastFetch'] = fetch_dt
            if df is None or df.empty:
                df = df_new
            elif df_new is not None and not df_new.empty:
                df_pruned = df.drop([c for c in df.columns if c in df_new], axis=1)
                df_new_pruned = df_new.drop([c for c in df_new.columns if c in df], axis=1)
                if df_pruned.empty and df_new_pruned.empty:
                    if next_release is not None and hasattr(next_release.release_date, 'confidence') and next_release.release_date.confidence == yfcd.Confidence.Low:
                        # Probably not released yet
                        pass
                    # else:
                    #     # Update: also check if a large amount of time has passed since release.
                    #     # Will Yahoo ever have it?
                    #     td_since_release = d_today - next_release.release_date
                    #     try:
                    #         Yahoo_very_late = td_since_release > yf_max_grace_days_period
                    #     except yfcd.AmbiguousComparisonException:
                    #         Yahoo_very_late = False
                    #     if Yahoo_very_late:
                    #         # print("- next_release:", next_release)
                    #         # print("- df:", df.columns, df.shape)
                    #         # print("- df_new:", df_new.columns, df_new.shape)
                    #         # print("- metadata old:") ; pprint(md_old)
                    #         # print("- td_since_release:", td_since_release)
                    #         ok = click.confirm(f"WARNING: Yahoo very late uploading newer {finType} for {self.ticker}, is this acceptable?", default=False)
                    #         if ok:
                    #             # print(f"WARNING: Yahoo missing newer financials for {self.ticker}")
                    #             pass
                    #         else:
                    #             # print("- next_release:", next_release)
                    #             # print("- df:", df.columns, df.shape)
                    #             # print("- df_new:", df_new.columns, df_new.shape)
                    #             # print("- metadata old:") ; pprint(md_old)
                    #             raise Exception(f'Why asking Yahoo for {finType} when nothing new ready?')
                elif not df_new.empty:
                    if df_pruned.empty:
                        df = df_new
                    else:
                        # Before merging, check for new/missing fields. Insert any with value NaN.
                        missing_keys = [k for k in df_pruned.index if k not in df_new.index]
                        new_keys = [k for k in df_new.index if k not in df_pruned.index]
                        actions = []
                        for k in missing_keys:
                            actions.append((k, "missing", df_pruned.index.get_loc(k)))
                        for k in new_keys:
                            actions.append((k, "new", df_new.index.get_loc(k)))
                        actions = sorted(actions, key=lambda x: x[2])
                        for a in actions:
                            k = a[0]
                            if a[1] == 'missing':
                                empty_row = pd.DataFrame(data={c:[np.nan] for c in df_new.columns}, index=[k])
                                idx = df_pruned.index.get_loc(k)
                                df_new = pd.concat([df_new.iloc[:idx], empty_row, df_new.iloc[idx:]])
                            else:
                                empty_row = pd.DataFrame(data={c:[np.nan] for c in df_pruned.columns}, index=[k])
                                idx = df_new.index.get_loc(k)
                                df_pruned = pd.concat([df_pruned.iloc[:idx], empty_row, df_pruned.iloc[idx:]])
                        df_new = df_new.reindex(df_pruned.index)
                        df = pd.concat([df_new, df_pruned], axis=1)
            yfcm.StoreCacheDatum(self.ticker, name, df, metadata=md)

        self._fin_tbl_cache[cache_key] = df
        return df

    def _get_interval_from_table(self, tbl):
        debug = False
        # debug = True

        if debug:
            print("_get_interval_from_table()")

        dates = tbl.columns

        # Ensure only well-populated columns are retained, corresponding to report releases
        tbl = self._prune_yf_financial_df(tbl)
        tbl = tbl[tbl.columns.sort_values(ascending=False)]
        dates = tbl.columns
        if debug:
            print("- tbl:") ; print(tbl)
        if len(dates) <= 1:
            return yfcd.TimedeltaEstimate(yfcd.ComparableRelativedelta(months=6), yfcd.Confidence.Medium)

        intervals = [(dates[i-1] - dates[i]).days for i in range(1,len(dates))]
        intervals = np.array(intervals)

        # Cluster actual intervals
        def safe_add_to_cluster(clusters, num, std_pct_threshold):
            for c in clusters:
                c2 = np.append(c, num)
                if (np.std(c2) / np.mean(c2)) < std_pct_threshold:
                    c.append(num)
                    return True
            return False
        def cluster_numbers(numbers, std_pct):
            clusters = []
            for n in sorted(numbers):
                if not clusters or not safe_add_to_cluster(clusters, n, std_pct):
                    clusters.append([n])
            return clusters
        clusters = cluster_numbers(intervals, 0.05)

        # Map clusters to legal intervals
        tol = 10
        intervals = []
        for i in range(len(clusters)-1, -1, -1):
            m = np.mean(clusters[i])
            if abs(m-365) < tol:
                intervals.append(yfcd.ComparableRelativedelta(years=1))
            elif abs(m-182) < tol:
                intervals.append(yfcd.ComparableRelativedelta(months=6))
            elif abs(m-91) < tol:
                intervals.append(yfcd.ComparableRelativedelta(months=3))
            elif abs(m-274) < tol:
                # 9 months, nonsense, but implies quarterly
                intervals.append(yfcd.TimedeltaEstimate(yfcd.ComparableRelativedelta(months=3), yfcd.Confidence.Medium))
            else:
                del clusters[i]
        if len(intervals) == 1:
            # good!
            return intervals[0]
        else:
            # Return the smallest. In case of ambiguous comparison, keep most confident.
            best = intervals[0]
            for i in range(1, len(intervals)):
                i2 = intervals[i]
                try:
                    best = min(best, i2)
                except yfcd.AmbiguousComparisonException:
                    best_confidence = best.confidence if hasattr(best, 'confidence') else yfcd.Confidence.High
                    i2_confidence = i2.confidence if hasattr(i2, 'confidence') else yfcd.Confidence.High
                    if i2_confidence > best_confidence:
                        best = i2
            return best

    def _get_interval(self, finType, refresh=True):
        debug = False
        # debug = True

        if debug:
            print(f"_get_interval({finType})")

        if not isinstance(finType, yfcd.Financials):
            raise Exception('Argument finType must be type Financials')

        tbl = self._get_fin_table(finType, yfcd.ReportingPeriod.Interim, refresh)

        return self._get_interval_from_table(tbl)

    def get_release_dates(self, period, as_df=False, refresh=True, check=False):
        # First, check cache:
        if period == yfcd.ReportingPeriod.Full:
            cache_key = "full"
        elif period == yfcd.ReportingPeriod.Interim:
            cache_key = "interim"
        else:
            raise Exception(f"Unknown period value '{period}'")
        cache_key += "-release-dates"
        releases, md = None, None
        if yfcm.IsDatumCached(self.ticker, cache_key):
            releases, md = yfcm.ReadCacheDatum(self.ticker, cache_key, True)
            if len(releases) == 0:
                releases = None

        max_age = pd.Timedelta(yfcm._option_manager.max_ages.calendar)
        dt_now = pd.Timestamp.now()
        d_exchange = pd.Timestamp.utcnow().tz_convert(self.tzName).date()
        if releases is None:
            if md is None:
                do_calc = True
            else:
                do_calc = md['CalcDate'] < (dt_now - max_age)
        else:
            do_calc = False

            # Check if cached release dates need a recalc
            if md['CalcDate'] < (dt_now - max_age):
                prev_r, next_r = None, None
                for i in range(len(releases)-1):
                    r0 = releases[i]
                    r1 = releases[i+1]
                    try:
                        r_is_history = r0.release_date < d_exchange
                    except yfcd.AmbiguousComparisonException:
                        r_is_history = r0.release_date.prob_lt(d_exchange) > 0.9
                    if r_is_history:
                        prev_r = r0
                        next_r = r1
                if hasattr(prev_r, 'confidence'):
                    do_calc = True
                elif hasattr(next_r, 'confidence'):
                    try:
                        d_exchange < next_r.release_date
                    except yfcd.AmbiguousComparisonException:
                        print("- next release date is estimated, time to recalc:", next_r)
                        do_calc = True
                # print("- releases:") ; pprint(releases)
                # print("- md:") ; pprint(md)
                # raise Exception('review cached release dates')

        if do_calc:
            releases = self._calc_release_dates(period, refresh, check)
            md = {'CalcDate':pd.Timestamp.now()}
            if releases is None:
                yfcm.StoreCacheDatum(self.ticker, cache_key, [], metadata=md)
            else:
                yfcm.StoreCacheDatum(self.ticker, cache_key, releases, metadata=md)
        if releases is None:
            return None

        if not as_df:
            return releases

        period_ends = []
        period_ends_est = []
        release_dates = []
        release_dates_est = []
        delays = []
        for r in releases:
            rpe = r.period_end ; rrd = r.release_date
            if rpe is None or rrd is None:
                print(r)
                raise Exception('Release missing dates')
            period_ends.append(rpe if isinstance(rpe, date) else rpe.date)
            period_ends_est.append(rpe.confidence if isinstance(rpe, yfcd.DateEstimate) else yfcd.Confidence.High)
            dt1 = rpe if isinstance(rpe, date) else rpe.date
            if isinstance(rrd, yfcd.DateRange):
                rrd_range = rrd.end - rrd.start
                release_dates_est.append(yfcd.Confidence.High)
                release_dates.append((rrd.start, rrd.end))
                midpoint = rrd.start + timedelta(days=rrd_range.days//2)
                delays.append(midpoint - dt1)
            elif isinstance(rrd, yfcd.yfcd.DateRangeEstimate):
                release_dates_est.append(rrd.confidence)
                rrd_range = rrd.end - rrd.start
                midpoint = rrd.start + rrd_range*0.5
                if isinstance(midpoint, datetime):
                    midpoint = midpoint.date()
                release_dates.append((rrd.start, rrd.end))
                delays.append(midpoint - dt1)
            else:
                release_dates.append(rrd if isinstance(rrd, date) else rrd.date)
                release_dates_est.append(rrd.confidence if isinstance(rrd, yfcd.DateEstimate) else yfcd.Confidence.High)
                dt2 = rrd if isinstance(rrd, date) else rrd.date
                delays.append(dt2 - dt1)
        df = pd.DataFrame({'Period end':period_ends, 'PE confidence':period_ends_est, 'Release date':release_dates, 'RD confidence':release_dates_est, 'Delay':delays})
        df['Period end'] = pd.to_datetime(df['Period end'])
        df['Period end'] = df['Period end'].dt.tz_localize(self.tzName)
        df = df.set_index('Period end')

        # Set timezone
        # release_dates_formatted = []
        # for i in range(df.shape[0]):
        #     idx = df.index[i]
        #     x = df['Release date'].iloc[i]
        #     if isinstance(x, tuple):
        #         x = (pd.to_datetime(x[0]).tz_localize(self.tzName), pd.to_datetime(x[1]).tz_localize(self.tzName))
        #     else:
        #         x = pd.to_datetime(x).tz_localize(self.tzName)
        #     release_dates_formatted.append(x)
        # df['Release date'] = release_dates_formatted

        return df

    def _calc_release_dates(self, period, refresh=True, check=False):
        debug = False
        # debug = True

        if debug:
            print(f"_calc_release_dates({period}, refresh={refresh})")

        if not isinstance(period, yfcd.ReportingPeriod):
            raise Exception('Argument period must be type ReportingPeriod')
        yfcu.TypeCheckBool(refresh, 'refresh')
        yfcu.TypeCheckBool(check, 'check')

        # Get period ends
        tbl = None
        finType = None
        for f in yfcd.Financials:
            t = self._get_fin_table(f, period, refresh)
            t = self._prune_yf_financial_df(t)
            if tbl is None:
                tbl = t ; finType = f
            elif t is not None and t.shape[1] > tbl.shape[1]:
                tbl_wasnt_empty = not tbl.empty
                tbl = t ; finType = f
                if tbl_wasnt_empty:
                    break

        if tbl is None or tbl.empty:
            return None
        tbl_cols = tbl.columns
        if isinstance(tbl_cols[0], (datetime, pd.Timestamp)):
            tbl_cols = [c.date() for c in tbl_cols]
        period_ends = [d.date() for d in tbl.columns if d.date() <= d_today]
        period_ends.sort(reverse=True)
        if debug:
            print("- period_ends:")
            for x in period_ends:
                print(x)

        # Get calendar
        cal_release_dates = self._get_calendar_dates(refresh)
        if cal_release_dates is None:
            cal_release_dates = []
        if debug:
            if len(cal_release_dates) == 0:
                print("- calendar empty")
            else:
                print("- cal_release_dates:")
                for x in cal_release_dates:
                    print(x)

        # Get earnings dates
        edf = self.get_earnings_dates(start=tbl.columns.min().date(), refresh=refresh, clean=False)

        # Get full year end date
        tbl = None
        for f in yfcd.Financials:
            t = self._get_fin_table(f, yfcd.ReportingPeriod.Full, refresh=False)  # minimise fetches
            t = self._prune_yf_financial_df(t)
            if t is not None and not t.empty:
                tbl = t
                break
        if tbl is None and refresh:
            for f in yfcd.Financials:
                t = self._get_fin_table(f, yfcd.ReportingPeriod.Full, refresh)
                t = self._prune_yf_financial_df(t)
                if t is not None and not t.empty:
                    tbl = t
                    break
        if not tbl.empty:
            year_end = tbl.columns.max().date()
        else:
            year_end = None
        if pd.isna(year_end):
            print(tbl.iloc[0:4])
            raise Exception("'year_end' is NaN")
        if debug:
            print("- year_end =", year_end)

        # Clean earnings dates
        if (edf is None) or (edf.shape[0]==0):
            if debug:
                print("- earnings_dates table is empty")
            release_dates = cal_release_dates
        else:
            # Prune old dates
            f_old = edf.index.date < period_ends[-1]
            if f_old.any():
                edf = edf[~f_old]

            if edf.shape[0] > 1:
                # Drop dates that occurred just before another
                edf = edf.sort_index(ascending=True)
                d = edf.index.to_series().diff()
                d.iloc[0] = pd.Timedelta(999, unit='d')
                x_near = np.abs(d) < pd.Timedelta(5, "days")
                if x_near.any():
                    edf = edf[~x_near]
                edf = edf.sort_index(ascending=False)

            release_dates = cal_release_dates

            for i in range(edf.shape[0]):
                dt = edf.index[i].date()
                r = edf.iloc[i]
                td = None
                if td is None:
                    if pd.isnull(r["Reported EPS"]) and pd.isnull(r["Surprise(%)"]) and not r['Date confirmed?']:
                        td = yfcd.DateEstimate(dt, yfcd.Confidence.Medium)
                    else:
                        td = dt

                # Protect against duplicating entries in calendar
                duplicate = False
                for c in release_dates:
                    diff = c - td
                    try:
                        duplicate = diff > timedelta(days=-20) and diff < timedelta(days=20)
                    except yfcd.AmbiguousComparisonException:
                        p1 = diff.prob_gt(timedelta(days=-20))
                        p2 = diff.prob_lt(timedelta(days=20))
                        duplicate = p1 > 0.9 and p2 > 0.9
                    if duplicate:
                        break
                if not duplicate:
                    release_dates.append(td)
        if debug:
            print("- edf:")
            print(edf)
            release_dates.sort(reverse=True)
            print("- release_dates:")
            pprint(release_dates)

        # Deduce interval
        if period == yfcd.ReportingPeriod.Full:
            interval_td = interval_str_to_days['ANNUAL']
        else:
            interval_td = self._get_interval(finType, refresh)
        if debug:
            print(f"- interval_td = {interval_td}")

        # Now combine known dates into 'Earnings Releases':
        if debug:
            print("# Now combine known dates into 'Earnings Releases':")
        releases = []
        for d in period_ends:
            r = EarningsRelease(interval_td, d, None, year_end)
            releases.append(r)
        if debug:
            releases.sort()
            print("> releases with known period-end-dates:")
            pprint(releases)

        # Fill gap between last release and now+9mo with estimated releases
        if debug:
            print("# Fill gap between last release and now with estimated releases")
        releases.sort(reverse=True)
        last_release = releases[0]
        if debug:
            print("- last_release:", last_release)
        ct = 0
        while True:
            ct += 1
            if ct > 10:
                for r in releases:
                    print(r)
                print("interval_td = {0}".format(interval_td))
                raise Exception("Infinite loop detected while estimating next financial report")

            next_period_end = yfcd.DateEstimate(interval_td + last_release.period_end, yfcd.Confidence.High)

            r = EarningsRelease(interval_td, next_period_end, None, year_end)

            releases.insert(0, r)
            last_release = r
            if debug:
                print("Inserting:", r)

            try:
                if r.period_end > (d_today+timedelta(days=270)):
                    break
            except yfcd.AmbiguousComparisonException:
                p = r.period_end.prob_gt(d_today+timedelta(days=270))
                if p > 0.9:
                    break
        releases.sort()
        if debug:
            print("# Intermediate set of releases:")
            pprint(releases)

        if release_dates is None or len(release_dates) == 0:
            if debug:
                print("No release dates in Yahoo so estimating all with Low confidence")
            for i in range(len(releases)):
                releases[i].release_date = yfcd.DateEstimate(releases[i].period_end+timedelta(days=5)+yfcd.confidence_to_buffer[yfcd.Confidence.Low], yfcd.Confidence.Low)
            return releases
        release_dates.sort()

        # Add more releases to ensure their date range fully overlaps with release dates
        release_dates.sort()
        releases.sort()
        ct = 0
        while True:
            try:
                gt_than = releases[0].period_end > release_dates[0]
            except yfcd.AmbiguousComparisonException:
                if hasattr(releases[0].period_end, 'prob_gt'):
                    p = releases[0].period_end.prob_gt(release_dates[0])
                else:
                    p = release_dates[0].prob_lt(releases[0].period_end)
                gt_than = p > 0.9
            if not gt_than:
                break

            ct += 1
            if ct > 100:
                raise Exception("Infinite loop detected while adding release objects")
            prev_period_end = releases[-1].period_end - interval_td
            conf = yfcd.Confidence.High
            if isinstance(prev_period_end, date):
                prev_period_end = yfcd.DateEstimate(prev_period_end, conf)
            else:
                prev_period_end = yfcd.DateEstimate(prev_period_end.date, min(prev_period_end.confidence, conf))

            r = EarningsRelease(interval_td, prev_period_end, None, year_end)

            releases.insert(0, r)
            if debug:
                print("Inserting:", r)
        ct = 0
        while True:
            try:
                less_than = releases[-1].period_end+interval_td < release_dates[-1]
            except yfcd.AmbiguousComparisonException:
                p = (releases[-1].period_end+interval_td).prob_lt(release_dates[-1])
                less_than = p > 0.5
            if not less_than:
                break

            ct += 1
            if ct > 20:
                raise Exception("Infinite loop detected while adding release objects")
            next_period_end = releases[-1].period_end + interval_td
            if isinstance(next_period_end, date):
                next_period_end = yfcd.DateEstimate(next_period_end, yfcd.Confidence.Medium)
            else:
                next_period_end = yfcd.DateEstimate(next_period_end.date, min(next_period_end.confidence, yfcd.Confidence.Medium))

            r = EarningsRelease(interval_td, next_period_end, None, year_end)
            releases.append(r)
            if debug:
                print("Appending:", r)
        # Fill in gaps in periods with estimates:
        for i in range(len(releases)-2, -1, -1):
            while True:
                r0 = releases[i]
                r1 = releases[i+1]
                try:
                    diff = r1.period_end - r0.period_end
                    gap_too_large = (diff/1.5) > interval_td
                except yfcd.AmbiguousComparisonException:
                    gap_too_large = False
                if gap_too_large:
                    new_r = EarningsRelease(interval_td, r1.period_end - interval_td, None, year_end)
                    if debug:
                        print(f"Inserting release estimate into gap: {new_r} (diff={diff}, interval_td={interval_td}, {type(interval_td)})")
                    releases.insert(i+1, new_r)
                else:
                    break
        if debug:
            releases.sort()
            print("# Final set of releases:")
            pprint(releases)

        # Assign known dates to appropriate release(s) without dates
        if debug:
            print("# Assigning known dates to releases ...")
        rdts = []
        for dt in release_dates:
            if hasattr(dt, 'confidence'):
                dt = dt.date
            elif isinstance(dt, yfcd.DateRange):
                dt = dt.start + (dt.end-dt.start)*0.5
            rdts.append(dt)
        pes = []
        for r in releases:
            pe = r.period_end
            if hasattr(pe, 'confidence'):
                pe = pe.date
            pes.append(pe)
        rdts = sorted(rdts)
        pes = sorted(pes)
        # Create the LP problem
        prob = pulp.LpProblem("MatchingReleaseDates", pulp.LpMinimize)
        # Variables: x_ij
        x = pulp.LpVariable.dicts("assignment", 
                                  ((i,j) for i in range(len(pes)) for j in range(len(rdts))),
                                  cat='Binary')

        # Constraints
        # - period end assigned max once
        for i in range(len(pes)):
            prob += pulp.lpSum(x[i,j] for j in range(len(rdts))) <= 1
        # - release date assigned max once
        for j in range(len(rdts)):
            prob += pulp.lpSum(x[i,j] for i in range(len(pes))) <= 1
        # - release date > period end
        for i in range(len(pes)):
            for j in range(len(rdts)):
                if rdts[j] <= pes[i]:
                    prob += x[i,j] == 0
        # - for p1 > p0, then r1 > r0
        for i0 in range(len(pes)-1):
            for i1 in range(i0+1, len(pes)):
                for j0 in range(len(rdts)-1):
                    for j1 in range(j0+1, len(rdts)):
                        prob += x[i0,j1] + x[i1,j0] <= 1

        # Penalise no assignments.
        obj_assigns = pulp.lpSum(1 - x[i,j] for i in range(len(pes)) for j in range(len(rdts)))

        # Penalise assigning R0->P0 when R0 is > P1
        # Aka, minimise how far R0 extends past P1
        slack_vars = pulp.LpVariable.dicts("Slack", ((i, j) for i in range(len(pes) - 1) for j in range(len(rdts))), lowBound=0)
        for i in range(len(pes) - 1):
            for j in range(len(rdts)):
                prob += (rdts[j] - pes[i + 1]).days/14 * x[i, j] - slack_vars[(i, j)] <= 0
        penalty_overlap = pulp.lpSum([slack_vars[i, j] for i, j in slack_vars])

        # Objective: Minimize delay between assigned period end and release date
        obj_min_delay = pulp.lpSum((rdts[j] - pes[i]).days/14 * x[i,j] for i in range(len(pes)) for j in range(len(rdts)))

        # Penalty: avoid adjacent assigned release dates closer than 30 days
        proximity1_slack_vars = pulp.LpVariable.dicts("Proximity1Slack", ((i, j) for i in range(len(pes)-1) for j in range(len(rdts)-1)), lowBound=0)
        for i0 in range(len(pes)-1):
            for j0 in range(len(rdts)-1):  # j0 is R0, j0+1 is R1
                for i1 in range(i0+1, len(pes)):
                    day_diff = (rdts[j0+1] - rdts[j0]).days
                    if day_diff < 30:
                        prob += proximity1_slack_vars[i0,j0] >= (30-day_diff)/14 * (x[i0,j0]+x[i1,j0+1])
        obj_space_out_release_dts = pulp.lpSum(proximity1_slack_vars[i,j] for i in range(len(pes)-1) for j in range(len(rdts)-1))

        # Penalty: avoid release date being very soon after period end
        proximity2_slack_vars = pulp.LpVariable.dicts("Proximity2Slack", ((i, j) for i in range(len(pes)) for j in range(len(rdts))), lowBound=0)
        # Add constraints for slack variables
        # proximity = 5
        # proximity = 10
        proximity = 14
        for i in range(len(pes)):
            for j in range(len(rdts)):
                day_diff = (rdts[j] - pes[i]).days
                if day_diff < proximity:
                    # Only activate slack if the release date is proximal to the period end
                    prob += proximity2_slack_vars[i, j] >= (proximity - day_diff) * x[i, j]
        obj_avoid_tight_assigns = pulp.lpSum(proximity2_slack_vars[i, j] for i in range(len(pes)) for j in range(len(rdts)))

        prob += 10*obj_assigns + 0.5*penalty_overlap + 0.5*obj_min_delay + obj_space_out_release_dts + obj_avoid_tight_assigns

        # Solve the problem
        # prob.solve()
        prob.solve(pulp.PULP_CBC_CMD(msg=False))  # silent

        # Cluster the delays
        delays = []
        for i in range(len(pes)):
            for j in range(len(rdts)):
                if pulp.value(x[i,j]) == 1:
                    delay = rdts[j] - pes[i]
                    if debug:
                        print(f"pulp: period-end {pes[i]} -> {rdts[j]} release (delay = {delay.days} days)")
                    delays.append((delay.days, releases[i].is_end_of_year()))
        for r in releases:
            if r.release_date is not None:
                d = r.release_date - r.period_end
                if hasattr(d, "confidence"):
                    d = d.td
                delays.append((d.days, r.is_end_of_year()))
        delays = sorted(delays, key=lambda x: x[0])
        clusters = []
        std_pct_threshold = 0.4
        for i in range(len(delays)):
            d = delays[i]
            dd = d[0]
            if not clusters:
                clusters.append([d])
            else:
                added_to_cluster = False
                for c in clusters:
                    c_values = np.array([cx[0] for cx in c])
                    std_pct_pre = np.std(c_values) / np.mean(c_values)
                    c_values = np.append(c_values, dd)
                    std_pct_post = np.std(c_values) / np.mean(c_values)
                    if (std_pct_pre == 0.0 or (std_pct_post < 30*std_pct_pre)) and (np.std(c_values) / np.mean(c_values)) < std_pct_threshold:
                        # Append for real
                        c.append(d)
                        added_to_cluster = True
                        break
                if not added_to_cluster:
                    clusters.append([d])

        # Prune clusters
        interim_clusters = []
        fy_clusters = []
        for c in clusters:
            if all([x[1] for x in c]):
                fy_clusters.append(c)
            else:
                interim_clusters.append(c)
        if debug:
            print("- clusters before pruning:")
            if any(interim_clusters):
                print("  - interims:")
                for c in interim_clusters:
                    print(f"    {c}")
            if any(fy_clusters):
                print("  - FY:")
                for c in fy_clusters:
                    print(f"    {c}")
        if len(fy_clusters) > 1:
            if any(len(c)>1 for c in fy_clusters):
                # Discard any with length 1
                for i in range(len(fy_clusters)-1, -1, -1):
                    if len(fy_clusters[i]) == 1:
                        del fy_clusters[i]
            elif len(fy_clusters) == 2:
                # 2x clusters of length 1: just combine. Probably one date is real and other Yahoo estimate.
                fy_clusters = [ [fy_clusters[0][0], fy_clusters[1][0]] ]
        if len(fy_clusters) > 1:
            # Fuck, need to prune again. This time, any with mean delay < 40 days
            for i in range(len(fy_clusters)-1, -1, -1):
                m = mean([d[0] for d in fy_clusters[i]])
                if m < 40:
                    del fy_clusters[i]
        if len(fy_clusters) > 1:
            for c in fy_clusters:
                print(c)
            print("- releases:")
            for r in releases:
                print(r)
            raise Exception(f'Have more than 1x FY cluster (see above) (period={period})')
        if debug:
            print("- clusters after pruning:")
            if any(interim_clusters):
                print("  - interims:")
                for c in interim_clusters:
                    print(f"    {c}")
            if any(fy_clusters):
                print("  - FY:")
                for c in fy_clusters:
                    print(f"    {c}")
        fy_cluster = [] if len(fy_clusters) == 0 else fy_clusters[0]
        # Can now discard end-of-year information:
        for i in range(len(interim_clusters)):
            interim_clusters[i] = [x[0] for x in interim_clusters[i]]
        fy_cluster = [x[0] for x in fy_cluster]
        if debug:
            print("- clusters after discarding EoY info:")
            if any(interim_clusters):
                print("  - interim_clusters:")
                for c in interim_clusters:
                    print(f"    {c}")
            if any(fy_cluster):
                print(f"- fy_cluster: {fy_cluster}")
        if any(len(c)>1 for c in interim_clusters):
            for i in range(len(interim_clusters)-1, -1, -1):
                # Discard any with length 1
                if len(interim_clusters[i]) == 1:
                    del interim_clusters[i]
            if debug:
                print("- interims after discarding single-length interims:")
                if any(interim_clusters):
                    for c in interim_clusters:
                        print(f"    {c}")
        if len(interim_clusters) == 0:
            interim_clusters = None
        # Keep longest interim delay // discard assignments not in interim nor FY cluster
        if interim_clusters is None:
            interim_cluster = None
        else:
            if isinstance(interim_clusters[0], int):
                interim_cluster = interim_clusters
            else:
                longest_i = 0
                longest_delay = mean(interim_clusters[0])
                for i in range(1, len(interim_clusters)):
                    m = mean(interim_clusters[i])
                    if m > longest_delay:
                        longest_i = i
                        longest_delay = m
                interim_cluster = interim_clusters[longest_i]
        for i in range(len(pes)):
            for j in range(len(rdts)):
                if pulp.value(x[i,j]) == 1:
                    delay = rdts[j] - pes[i]
                    # if delay.days not in interim_cluster:
                    # if delay.days not in interim_cluster and delay.days not in fy_cluster:
                    if delay.days not in fy_cluster and (interim_cluster is None or delay.days not in interim_cluster):
                        if debug:
                            if interim_cluster is None:
                                msg = f"discard pulp assignment {rdts[j]} -> {pes[i]} (delay={delay.days}) for not being in FY cluster {fy_cluster} (and no interim clusters detected)"
                            else:
                                msg = f"discard pulp assignment {rdts[j]} -> {pes[i]} (delay={delay.days}) for not being in chosen interim cluster {interim_cluster} nor in FY cluster {fy_cluster}"
                            print(msg)
                        x[i,j] = 0

        # Output results
        for i in range(len(pes)):
            for j in range(len(rdts)):
                if pulp.value(x[i,j]) == 1:
                    releases[i].release_date = release_dates[j]

        if debug:
            releases.sort()
            print("> releases with pulp-assigned release dates:")
            for r in releases:
                print(r)
            print(f"- {len(release_dates)}x release dates , {len(releases)}x period-end dates")

        # For any releases still without release dates, estimate with the following heuristics:
        # 1 - if release 12 months before/after has a date (or a multiple of 12), use that +/- 12 months
        # 2 - else used previous release + interval
        if debug:
            print("# Estimating release dates from other releases at similar time-of-year")
        report_delay = None
        releases.sort()
        if any([r.release_date is None for r in releases]):
            for try_interval in [365, 365//2, 365//4]:
                itd = timedelta(days=try_interval)
                for i in range(len(releases)):
                    if releases[i].release_date is None:
                        # Need to find a similar release to extrapolate date from
                        r = releases[i]
                        date_set = False
                        candidates = []
                        for i2 in range(len(releases)):
                            if i2==i:
                                continue
                            if releases[i2].release_date is not None:
                                if period == yfcd.ReportingPeriod.Full:
                                    tolerance = timedelta(days=40)
                                else:
                                    tolerance = timedelta(days=10)
                                if releases[i2].period_end > releases[i].period_end:
                                    rem = (releases[i2].period_end - releases[i].period_end) % itd
                                else:
                                    rem = (releases[i].period_end - releases[i2].period_end) % itd
                                try:
                                    m1 = rem < tolerance
                                except yfcd.AmbiguousComparisonException:
                                    m1 = rem.prob_lt(tolerance) > 0.9
                                try:
                                    m2 = abs(rem-itd) < tolerance
                                except yfcd.AmbiguousComparisonException:
                                    m2 = abs(rem-itd).prob_lt(tolerance) > 0.9
                                match = m1 or m2
                                if match:
                                    candidates.append(releases[i2])

                        if len(candidates) > 0:
                            # Pick closest one
                            closest_r = candidates[0]
                            for i2 in range(1, len(candidates)):
                                try:
                                    if abs(candidates[i2].period_end-r.period_end) < abs(closest_r.period_end-r.period_end):
                                        closest_r = candidates[i2]
                                except yfcd.AmbiguousComparisonException:
                                    pass
                            if debug:
                                print(f"- matching '{releases[i]}' with '{closest_r}' for interval '{try_interval}'")
                            delay = closest_r.release_date - closest_r.period_end
                            dt = delay + releases[i].period_end

                            r_is_end_of_year = releases[i].is_end_of_year()
                            if r_is_end_of_year and try_interval != 365:
                                # Annual reports take longer than interims, so add on some more days
                                dt += timedelta(days=28)

                            if not hasattr(dt, 'confidence'):
                                if r_is_end_of_year and try_interval != 365:
                                    confidence = yfcd.Confidence.Low
                                else:
                                    confidence = yfcd.Confidence.Medium
                                if isinstance(dt, date):
                                    dt = yfcd.DateEstimate(dt, confidence)
                                elif isinstance(dt, yfcd.DateRange):
                                    dt = yfcd.DateRangeEstimate(dt.start, dt.end, confidence)
                                else:
                                    raise Exception('Need to ensure this value has confidence:', dt)
                            else:
                                if r_is_end_of_year and try_interval != 365:
                                    confidences = [yfcd.Confidence.Low]
                                else:
                                    confidences = [yfcd.Confidence.Medium]
                                if isinstance(closest_r.period_end, (yfcd.DateEstimate, yfcd.DateRangeEstimate)):
                                    confidences.append(closest_r.period_end.confidence)
                                if isinstance(closest_r.release_date, (yfcd.DateEstimate, yfcd.DateRangeEstimate)):
                                    confidences.append(closest_r.release_date.confidence)
                                dt.confidence = min(confidences)

                            if i > 0 and (releases[i-1].release_date is not None):
                                too_close_to_previous = False
                                try:
                                    if isinstance(releases[i-1].release_date, yfcd.DateEstimate):
                                        too_close_to_previous = releases[i-1].release_date.isclose(dt)
                                    else:
                                        if releases[i-1].is_end_of_year():
                                            threshold = timedelta(days=1)
                                        else:
                                            threshold = timedelta(days=30)
                                        if debug:
                                            diff = dt-releases[i-1].release_date
                                        too_close_to_previous = (dt-releases[i-1].release_date) < threshold
                                except yfcd.AmbiguousComparisonException:
                                    p = (dt-releases[i-1].release_date).prob_lt(threshold)
                                    too_close_to_previous = p > 0.9
                                if too_close_to_previous:
                                    if debug:
                                        print(f"  - dt '{dt}' would be too close to previous release date '{releases[i-1]}'")
                                    # Too close to last release date
                                    continue
                            releases[i].release_date = dt
                            date_set = True

                        if date_set and (report_delay is not None):
                            releases[i].release_date.date += report_delay
        if debug:
            print("> releases after estimating release dates:")
            for r in releases:
                print(r)

        any_release_has_date = False
        for r in releases:
            if r.release_date is not None:
                any_release_has_date = True
                break
        if not any_release_has_date:
            if debug:
                print(f"- unable to map any {period} financials to release dates")
            return None

        # Check for any releases still missing a release date that could be the Last earnings release:
        if any([r.release_date is None for r in releases]):
            for i in range(len(releases)):
                r = releases[i]
                if r.release_date is None:
                    problem = False
                    if i == len(releases)-1:
                        problem = True
                    else:
                        r2 = releases[i+1]
                        if (r2.release_date is not None) and (r2.release_date > d_today):
                            problem = True
                    if problem:
                        print(r)
                        raise Exception("A release that could be last is missing release date")

        if check:
            self._check_release_dates(releases, finType, period, refresh)

        return releases

    def _check_release_dates(self, releases, finType, period, refresh):
        # if period == yfcd.ReportingPeriod.Full:
        #     interval_td = interval_str_to_days['ANNUAL']
        # else:
        #     interval_td = self._get_interval(finType, refresh)

        # Ignore releases with no date:
        # - can happen with nonsense financials dates from Yahoo that
        #   even my prune function couldn't safely remove
        releases = [r for r in releases if r.release_date is not None]

        for i0 in range(len(releases)-1):
            r0 = releases[i0]
            r0rd = r0.release_date
            if hasattr(r0rd, 'confidence') and r0rd.confidence == yfcd.Confidence.Low:
                continue
            for i1 in range(i0+1, len(releases)):
                r1 = releases[i1]
                r1rd = r1.release_date
                if hasattr(r1rd, 'confidence') and r1rd.confidence == yfcd.Confidence.Low:
                    continue
                #
                if isinstance(r0rd, date) and isinstance(r1rd, date):
                    isclose = r0rd == r1rd
                elif isinstance(r0rd, date):
                    isclose = r1rd.isclose(r0rd)
                else:
                    isclose = r0rd.isclose(r1rd)
                if isclose:
                    print(r0)
                    print(r1)
                    raise Exception(f'{self.ticker} Release dates have been assigned multiple times')

        #
        for r in releases:
            try:
                is_negative = r.release_date < r.period_end
            except yfcd.AmbiguousComparisonException:
                p = r.release_date.prob_lt(r.period_end)
                is_negative = p > 0.9
            if is_negative:
                diff = r.release_date - r.period_end
                print("- rd =", r.release_date, type(r.release_date))
                print("- pe =", r.period_end, type(r.period_end))
                print("- diff =", diff, type(diff))
                print(r)
                raise Exception('Release dates contains negative delays')

    def _prune_yf_financial_df(self, df):
        debug = False
        # debug = True

        if df is None or df.empty:
            return df

        ## Fiddly to put dates into a list and sort without reordering dataframe and without down-casting the date types!
        dates = [d for d in df.columns]
        dates.sort()

        cache_key = tuple([df.index[0]] + dates)
        if cache_key in self._pruned_tbl_cache:
            return self._pruned_tbl_cache[cache_key]

        # Drop duplicated columns
        if len(set(dates)) != len(dates):
            ## Search for duplicated columns
            df = df.T.drop_duplicates().T
            dates = [d for d in df.columns]
            dates.sort()

        # Drop mostly-NaN duplicated dates:
        df_modified = False
        if len(set(dates)) != len(dates):
            for dt in set(dates):
                dff = df[dt]
                if len(dff.shape) == 2 and dff.shape[1] == 2:
                    # This date is duplicated, so count NaNs:
                    n_dups = dff.shape[1]
                    dt_indices = np.where(df.columns == dt)[0]
                    is_mostly_nans = np.array([False]*n_dups)
                    for i in range(n_dups):
                        dt_idx = dt_indices[i]
                        is_mostly_nans[i] = df.iloc[:,dt_idx].isnull().sum() > int(df.shape[0]*0.75)
                    if is_mostly_nans.sum() == n_dups-1:
                        ## All but one column are mostly nans, perfect!
                        drop_indices = dt_indices[is_mostly_nans]
                        indices = np.array(range(df.shape[1]))
                        keep_indices = indices[~np.isin(indices, drop_indices)]
                        df = df.iloc[:,keep_indices].copy()
                        df_modified = True

                dff = df[dt]
                if len(dff.shape) == 2 and dff.shape[1] == 2:
                    # Date still duplicated. 
                    # Find instance with most non-nan values; if 
                    # all other instances are equal or nan then drop.

                    n_dups = dff.shape[1]
                    dt_indices = np.where(df.columns == dt)[0]
                    nan_counts = np.zeros(n_dups)
                    for i in range(n_dups):
                        dt_idx = dt_indices[i]
                        nan_counts[i] = df.iloc[:,dt_idx].isnull().sum()
                    idx_min_na = 0
                    for i in range(1,n_dups):
                        if nan_counts[i] < nan_counts[idx_min_na]:
                            idx_min_na = i
                    drop_indices = []
                    for i in range(n_dups):
                        if i == idx_min_na:
                            continue
                        min_idx = dt_indices[idx_min_na]
                        dt_idx = dt_indices[i]
                        f_match = df.iloc[:,dt_idx].isnull() | (df.iloc[:,dt_idx]==df.iloc[:,min_idx])
                        if f_match.all():
                            drop_indices.append(dt_idx)
                    if len(drop_indices)>0:
                        indices = np.array(range(df.shape[1]))
                        keep_indices = indices[~np.isin(indices, drop_indices)]
                        df = df.iloc[:,keep_indices].copy()
                        df_modified = True
        if df_modified:
            dates = [d for d in df.columns]
            dates.sort()

        # If duplicated date columns is very similar, then drop right-most:
        df_modified = False
        if len(set(dates)) != len(dates):
            for dt in set(dates):
                dff = df[dt]
                if len(dff.shape) == 2 and dff.shape[1] == 2:
                    dff.columns = [str(dff.columns[i])+str(i) for i in range(dff.shape[1])]
                    # r = dff.diff(axis=1)
                    r = (dff[dff.columns[0]] - dff[dff.columns[1]]).abs() / dff[dff.columns[0]]
                    r = r.sum()
                    if r < 0.15:
                        df = df.drop(dt, axis=1)
                        df[dt] = dff[dff.columns[0]]
                        df_modified = True
        if df_modified:
            dates = [d for d in df.columns]
            dates.sort()

        if len(set(dates)) != len(dates):
            print(df)
            print("Dates: {}".format(dates))
            raise Exception("Duplicate dates found in financial df")

        # Search for mostly-nan columns, where the non-nan values are exact match to an adjacent column.
        # Replace those nans with adjacent column values.
        # Optimise:
        df_isnull = df.isnull()
        df_isnull_sums = df_isnull.sum()
        nan_threshold = int(df.shape[0]*0.75)
        for i1 in range(1, len(dates)):
            d1 = dates[i1]
            d0 = dates[i1-1]
            d0_mostly_nans = df_isnull_sums[d0] > nan_threshold
            d1_mostly_nans = df_isnull_sums[d1] > nan_threshold
            if d0_mostly_nans and not d1_mostly_nans:
                f = (~df_isnull[d0]) & (~df_isnull[d1])
                if np.sum(f) >= 2:
                    # At least two actual values
                    if np.array_equal(df.loc[f,d0], df.loc[f,d1]):
                        # and those values match
                        df[d0] = df[d1].copy()
            elif d1_mostly_nans and not d0_mostly_nans:
                f = (~df_isnull[d1]) & (~df_isnull[d0])
                if np.sum(f) >= 2:
                    # At least two actual values
                    if np.array_equal(df.loc[f,d1], df.loc[f,d0]):
                        # and those values match
                        df[d1] = df[d0].copy()

        # Drop mostly-nan columns:
        df_modified = False
        for i in range(len(dates)-1, -1, -1):
            d = dates[i]
            # if df[d].isnull().sum() == df.shape[0]:
            #   # Full of nans, drop column:
            if np.sum(df[d].isnull()) > nan_threshold:
                # Mostly nans, drop column
                if debug:
                    print(f"_prune_yf_financial_df(): column {d} is mostly NaNs")
                df = df.drop(d, axis=1)
                df_modified = True
        if df_modified:
            dates = [d for d in df.columns]
            dates.sort()

        # # Then drop all columns devoid of data (NaN and 0.0):
        # for i in range(len(dates)-1, -1, -1):
        #     d = dates[i]
        #     fnan = df[d].isnull()
        #     fzero = df[d]==0.0
        #     if sum(np_or(fnan, fzero)) == df.shape[0]:
        #         # Completely devoid of data, drop column
        #         df = df.drop(d, axis=1)

        # Search for populated columns, where values are very similar.
        similarity_pct_threshold = 0.8
        for i in range(len(dates)-2, -1, -1):
            d1 = dates[i+1]
            d0 = dates[i]
            delta = d1 - d0
            similarity_pct = np.sum(df[d0] == df[d1]) / df.shape[0]
            if df.shape[0] > 10 and delta < timedelta(days=45) and similarity_pct > similarity_pct_threshold:
                if debug:
                    print(f"{d0.date()} very similar & close to {d1.date()}, discarding later")
                # df = df.drop(d1, axis=1)
                # Instead of arbitrarily dropping one date, be smart.
                # Keep the one that makes most sense relative to distances to other dates
                diffs0 = [] ; diffs1 = []
                if i > 0:
                    diffs0.append((dates[i] - dates[i-1]).days)
                    diffs1.append((dates[i+1] - dates[i-1]).days)
                if i < (len(dates)-2):
                    diffs0.append((dates[i+2] - dates[i]).days)
                    diffs1.append((dates[i+2] - dates[i+1]).days)
                diffs0 = [min(abs(d-91), abs(d-182), abs(d-365)) for d in diffs0]
                diffs1 = [min(abs(d-91), abs(d-182), abs(d-365)) for d in diffs1]
                if mean(diffs0) < mean(diffs1):
                    df = df.drop(d1, axis=1)
                else:
                    df = df.drop(d0, axis=1)
                dates = [d for d in df.columns]
                dates.sort()

        if len(set(dates)) != len(dates):
            print(f"Dates: {dates}")
            raise Exception("Duplicate dates found in financial df")

        # Remove columns which YF created by backfilling
        df = df[df.columns.sort_values(ascending=False)]
        dates = [d for d in df.columns]
        for i1 in range(1, len(dates)):
            d0 = dates[i1-1]
            d1 = dates[i1]
            d0_values = df[d0].copy()
            d1_values = df[d1].copy()
            d0_values.loc[d0_values.isna()] = 0.0
            d1_values.loc[d1_values.isna()] = 0.0
            if np.array_equal(d0_values.values, d1_values.values):
                if debug:
                    print(f"_prune_yf_financial_df(): column {d0} appears backfilled by Yahoo")
                df = df.drop(d0, axis=1)
        df = df[df.columns.sort_values(ascending=True)]

        if df.empty:
            raise Exception("_prune_yf_financial_df() has removed all columns")

        self._pruned_tbl_cache[cache_key] = df

        return df

    def _earnings_interval(self, with_report, refresh=True):
        # Use cached data to deduce interval regardless of 'refresh'.
        # If refresh=True, only refresh if cached data not good enough.

        yfcu.TypeCheckBool(with_report, 'with_report')
        yfcu.TypeCheckBool(refresh, 'refresh')

        debug = False
        # debug = True

        if debug:
            print(f'_earnings_interval(with_report={with_report}, refresh={refresh})')

        interval = None
        inference_successful = False

        if not with_report:
            edf = self.get_earnings_dates(start=d_today-timedelta(days=730), refresh=False)
            if (edf is None or edf.shape[0] <= 3) and refresh:
                edf = self.get_earnings_dates(start=d_today-timedelta(days=730), refresh=refresh)
            if edf is not None and edf.shape[0] > 3:
                # First, remove duplicates:
                deltas = np.flip((np.diff(np.flip(edf.index.date)) / pd.Timedelta(1, unit='D')))
                f = np.append(deltas > 0.5, True)
                edf = edf[f].copy()

                edf_old = edf[edf.index.date < date.today()]
                if edf_old.shape[0] > 3:
                    edf = edf_old.copy()
                deltas = (np.diff(np.flip(edf.index.date)) / pd.Timedelta(1, unit='D'))
                if (deltas == deltas[0]).all():
                    # Identical, perfect
                    interval_days = deltas[0]
                    std_pct_mean = 0.0
                else:
                    # Discard large outliers
                    z_scores = np.abs(stats.zscore(deltas))
                    deltas_pruned = deltas[z_scores < 1.4]
                    # Discard small deltas
                    deltas_pruned = deltas_pruned[deltas_pruned > 10.0]

                    std_pct_mean = np.std(deltas) / np.mean(deltas)
                    interval_days = np.mean(deltas_pruned)
                if debug:
                    print("- interval_days:", interval_days)
                if std_pct_mean < 0.68:
                    tol = 20
                    if abs(interval_days-365) < tol:
                        interval = 'ANNUAL'
                    elif abs(interval_days-182) < tol:
                        interval = 'HALF'
                    elif abs(interval_days-91) < tol:
                        interval = 'QUART'
                    if interval is not None:
                        return interval_str_to_days[interval]

        if debug:
            print("- insufficient data in earnings_dates, analysing financials columns")

        tbl_bs = self.get_quarterly_balance_sheet(refresh=False)
        tbl_fi = self.get_quarterly_income_stmt(refresh=False)
        tbl_cf = self.get_quarterly_cashflow(refresh=False)
        if refresh:
            if tbl_bs is None:
                tbl_bs = self.get_quarterly_balance_sheet(refresh)
            if tbl_fi is None:
                tbl_fi = self.get_quarterly_income_stmt(refresh)
            if tbl_cf is None:
                tbl_cf = self.get_quarterly_cashflow(refresh)
        tbl_bs = self._prune_yf_financial_df(tbl_bs)
        tbl_fi = self._prune_yf_financial_df(tbl_fi)
        tbl_cf = self._prune_yf_financial_df(tbl_cf)
        if with_report:
            # Expect all 3x financials present
            if tbl_bs is None or tbl_bs.empty or tbl_fi is None or tbl_fi.empty or tbl_cf is None or tbl_cf.empty:
                # Cannot be sure, but can estimate from any present table
                if tbl_bs is not None and not tbl_bs.empty:
                    tbl = tbl_bs
                elif tbl_fi is not None and not tbl_fi.empty:
                    tbl = tbl_fi
                else:
                    tbl = tbl_cf
            else:
                tbl = tbl_bs
        else:
            # Use whichever is available with most columns
            tbl = tbl_bs
            if tbl_fi is not None and len(tbl_fi.columns) > len(tbl.columns):
                tbl = tbl_fi
            if tbl_cf is not None and len(tbl_cf.columns) > len(tbl.columns):
                tbl = tbl_cf

        if debug:
            print("- tbl:") ; print(tbl)

        if tbl is not None and not tbl.empty and tbl.shape[0] > 1:
            return self._get_interval_from_table(tbl)

        if not inference_successful:
            interval = yfcd.TimedeltaEstimate(interval_str_to_days['HALF'], yfcd.Confidence.Medium)

        return interval

    def get_earnings_dates(self, start, refresh=True, clean=True):
        start_dt, start = yfcu.ProcessUserDt(start, self.tzName)
        yfcu.TypeCheckDateStrict(start, 'start')
        yfcu.TypeCheckBool(refresh, 'refresh')
        yfcu.TypeCheckBool(clean, 'clean')

        debug = False
        # debug = True

        if debug:
            print(f"get_earnings_dates(start={start}, refresh={refresh})")

        dt_now = pd.Timestamp.utcnow().tz_convert(self.tzName)

        last_fetch = None
        if self._earnings_dates is None:
            if yfcm.IsDatumCached(self.ticker, "earnings_dates"):
                if debug:
                    print("- retrieving earnings dates from cache")
                self._earnings_dates, md = yfcm.ReadCacheDatum(self.ticker, "earnings_dates", True)
                if md is None:
                    md = {}
                if self._earnings_dates is None:
                    # Fine, just means last call failed to get earnings_dates
                    pass
                else:
                    if 'LastFetch' not in md:
                        raise Exception("f{self.ticker}: Why earnings_dates metadata missing 'LastFetch'?")
                        fp = yfcm.GetFilepath(self.ticker, "earnings_dates")
                        last_fetch = datetime.fromtimestamp(os.path.getmtime(fp)).astimezone()
                        md['LastFetch'] = last_fetch
                        yfcm.WriteCacheMetadata(self.ticker, "earnings_dates", 'LastFetch', md['LastFetch'])
                    if self._earnings_dates.empty:
                        self._earnings_dates = None
                    else:
                        edf_clean = self._clean_earnings_dates(self._earnings_dates, refresh)
                        if len(edf_clean) < len(self._earnings_dates):
                            # This is ok, because since the last fetch, the calendar can be updated which then allows resolving a 
                            # near-duplication in earnings_dates.
                            yfcm.StoreCacheDatum(self.ticker, "earnings_dates", edf_clean)
                        self._earnings_dates = edf_clean

        last_fetch = yfcm.ReadCacheMetadata(self.ticker, "earnings_dates", "LastFetch")
        if debug:
            print("- last_fetch =", last_fetch)

        # Ensure column 'Date confirmed?' is present, and update with calendar
        df_modified = False
        if self._earnings_dates is not None:
            if 'Date confirmed?' not in self._earnings_dates.columns:
                self._earnings_dates['Date confirmed?'] = False
                df_modified = True
            cal = self.get_calendar(refresh)
            if cal is not None and len(cal['Earnings Date']) == 1:
                x = cal['Earnings Date'][0]
                for dt in self._earnings_dates.index:
                    if abs(dt.date() - x) < timedelta(days=7):
                        # Assume same release
                        try:
                            if not self._earnings_dates['Date confirmed?'].loc[dt]:
                                self._earnings_dates.loc[dt, 'Date confirmed?'] = True
                                df_modified = True
                                break
                        except Exception:
                            print("- dt:", dt)
                            print("- edf:") ; print(self._earnings_dates)
                            raise

        if not refresh:
            if df_modified:
                yfcm.StoreCacheDatum(self.ticker, "earnings_dates", self._earnings_dates)

            if debug:
                print("get_earnings_dates() returning")
            if self._earnings_dates is not None:
                if start_dt > self._earnings_dates.index[-1]:
                    return self._earnings_dates.sort_index().loc[start_dt:].sort_index(ascending=False).copy()
                else:
                    return self._earnings_dates.copy()
            else:
                return None

        # Limit spam:
        yf_start_date = yfcm.ReadCacheMetadata(self.ticker, 'earnings_dates', 'start_date')
        if debug:
            print("- yf_start_date =", yf_start_date)
        if last_fetch is not None:
            if (last_fetch + pd.Timedelta('14d')) > dt_now:
                # Avoid spamming Yahoo for data it doesn't have (empty earnings_dates).
                if self._earnings_dates is None:
                    # Already attempted a fetch recently, Yahoo has nothing.
                    if debug:
                        print("avoiding refetch")
                    refresh = False

                # Avoid spamming Yahoo for new future dates
                if self._earnings_dates is not None:
                    if yf_start_date is not None:
                        # Cache has all previous earnings dates
                        refresh = False
                    elif start > self._earnings_dates.index.date[-1]:
                        refresh = False
            if debug:
                print("- refresh =", refresh)

        if refresh:
            ei = self._earnings_interval(with_report=False, refresh=False)
            if isinstance(ei, yfcd.TimedeltaEstimate):
                # Don't care about confidence
                ei = ei.td
            elif isinstance(ei, yfcd.TimedeltaRangeEstimate):
                ei = mean([ei.td1, ei.td2])
            if isinstance(ei, (yfcd.ComparableRelativedelta, relativedelta)):
                # Convert to normal Timedelta, don't need 100% precision
                if ei.months == 3:
                    ei = pd.Timedelta('91d')
                elif ei.months == 6:
                    ei = pd.Timedelta('182d')
                elif ei.months == 12 or ei.years==1:
                    # ei = pd.Timedelta('365d')
                    # Don't believe it
                    ei = pd.Timedelta('182d')
                else:
                    raise Exception(ei, type(ei))

            lookahead_dt = dt_now + pd.Timedelta('365d')
            if debug:
                print("- ei =", ei)
                print("- lookahead_dt =", lookahead_dt)

            next_rd = None
            if self._earnings_dates is None or (start_dt < self._earnings_dates.index[-1] and yf_start_date is None):
                total_refetch = True
                n_intervals_to_fetch = int(math.floor(Decimal(1.25*(lookahead_dt - start_dt) / ei)))
            else:
                total_refetch = False
                df = self._earnings_dates.copy()
                f_na = df['Reported EPS'].isna().to_numpy()
                f_nna = ~f_na
                f_expired = f_na & (df.index < dt_now) & ((dt_now - df['FetchDate']) > pd.Timedelta('7d')).to_numpy()
                n = df.shape[0]
                if debug:
                    print("- n =", n)

                n_intervals_missing_after = int(math.floor(Decimal((lookahead_dt - df.index[0]) / ei)))
                any_expired = f_expired.any()
                if debug:
                    print("- n_intervals_missing_after =", n_intervals_missing_after)
                    print("- any_expired =", any_expired)
                if not any_expired:
                    # ToDo: avoid refetching if next earnings after last fetch is (far) in future.
                    if f_nna.any():
                        if debug:
                            print("- checking against release dates ...")
                        rds = self.get_release_dates(yfcd.ReportingPeriod.Interim, as_df=False, refresh=False)
                        if rds is not None:
                            rds = sorted(rds)
                            latest_certain_dt = df.index[np.where(f_nna)[0][0]].date()
                            for i in range(len(rds)):
                                try:
                                    in_future = rds[i].release_date > latest_certain_dt
                                except yfcd.AmbiguousComparisonException:
                                    p = rds[i].release_date.prob_gt(latest_certain_dt)
                                    in_future = p > 0.9
                                if in_future:
                                    next_rd = rds[i]
                                    break
                            try: 
                                next_rd_in_future = next_rd.release_date > (d_today + timedelta(days=7))
                            except yfcd.AmbiguousComparisonException:
                                p = next_rd.release_date.prob_gt(d_today + timedelta(days=7))
                                next_rd_in_future = p > 0.9
                            if next_rd_in_future:
                                # Avoid fetching while far from next earnings release
                                n_intervals_missing_after = 0
                    n_intervals_to_fetch = n_intervals_missing_after
                else:
                    earliest_expired_idx = np.where(f_expired)[0][-1]
                    n_intervals_expired = earliest_expired_idx + 1
                    n_intervals_to_fetch = n_intervals_expired + n_intervals_missing_after

            if n_intervals_to_fetch > 0:
                # Ensure always fetching more than necessary
                n_intervals_to_fetch += 8
            if debug:
                print("- n_intervals_to_fetch =", n_intervals_to_fetch)

            if n_intervals_to_fetch > 0:
                if debug:
                    print("- total_refetch =", total_refetch)
                try:
                    new_df = self._fetch_earnings_dates(n_intervals_to_fetch, refresh)
                except Exception:
                    print("- self._earnings_dates:") ; print(self._earnings_dates)
                    print("- start:", start)
                    print("- yf_start_date:", yf_start_date)
                    print("- last_fetch:", last_fetch)
                    print("- ei:", ei)
                    print("- next_rd:", next_rd)
                    print("- n_intervals_to_fetch:", n_intervals_to_fetch)
                    raise
                # Sanity test:
                if new_df is not None and not new_df.empty:
                    edf_clean = self._clean_earnings_dates(new_df, refresh)
                    if len(edf_clean) < len(new_df):
                        print("- edf:") ; print(new_df[['EPS Estimate', 'Reported EPS', 'FetchDate']])
                        print("- after clean:") ; print(edf_clean[['EPS Estimate', 'Reported EPS', 'FetchDate']])
                        raise Exception(f'{self.ticker}: We literally just fetched earnings dates, why not cleaned?')
                        yfcm.StoreCacheDatum(self.ticker, "earnings_dates", edf_clean)

                yfcm.WriteCacheMetadata(self.ticker, "earnings_dates", 'LastFetch', dt_now)
                if debug:
                    print("- new_df:") ; print(new_df)
                if new_df is not None and not new_df.empty:
                    if self._earnings_dates is not None:
                        df_old = self._earnings_dates[self._earnings_dates.index < (new_df.index[-1]-timedelta(days=14))]
                        if not df_old.empty:
                            new_df = pd.concat([new_df, df_old])
                        if debug:
                            print("- new_df:") ; print(new_df)
                    self._earnings_dates = new_df
                    df_modified = True

        if df_modified:
            if self._earnings_dates is None:
                yfcm.StoreCacheDatum(self.ticker, "earnings_dates", pd.DataFrame())
            else:
                yfcm.StoreCacheDatum(self.ticker, "earnings_dates", self._earnings_dates)

        df = None
        if debug:
            print("get_earnings_dates() returning")
        if self._earnings_dates is not None:
            if start_dt > self._earnings_dates.index[-1]:
                df = self._earnings_dates.sort_index().loc[start_dt:].sort_index(ascending=False)
            else:
                df = self._earnings_dates
            if clean:
                df = df.drop(["FetchDate", "Date confirmed?"], axis=1, errors='ignore')
            return df.copy()
        else:
            return None

    def _clean_earnings_dates(self, edf, refresh=True):
        edf = edf.sort_index(ascending=False)

        # In rare cases, Yahoo has duplicated a date with different company name.
        # Retain the row with most data.
        for i in range(len(edf)-1, 0, -1):
            if edf.index[i-1] == edf.index[i]:
                mask = np.ones(len(edf), dtype=bool)
                if edf.iloc[i-1].isna().sum() > edf.iloc[i].isna().sum():
                    # Discard row i-1
                    mask[i-1] = False
                else:
                    # Discard row i
                    mask[i] = False
                edf = edf[mask].copy()

        for i in range(len(edf)-2, -1, -1):
            if (edf.index[i]-edf.index[i+1]) < timedelta(days=7):
                # One must go
                if edf['FetchDate'].iloc[i] > edf['FetchDate'].iloc[i+1]:
                    edf = edf.drop(edf.index[i+1])
                elif edf['FetchDate'].iloc[i+1] > edf['FetchDate'].iloc[i]:
                    edf = edf.drop(edf.index[i])
                else:
                    cal = self.get_calendar(refresh)
                    if cal is None:
                        # print(edf.iloc[i:i+2])
                        # raise Exception('Review how to handle 2x almost-equal earnings dates.')
                        # pass  # Can't do anything with certainty
                        # Keep earlier
                        if edf.index[i] < edf.index[i+1]:
                            edf = edf.drop(edf.index[i+1])
                        else:
                            edf = edf.drop(edf.index[i])
                    else:
                        # Cross-check against calendar
                        dts = cal['Earnings Date']
                        if len(dts) == 1 and dts[0] in [edf.index[i].date(), edf.index[i+1].date()]:
                            if edf.index[i].date() == dts[0]:
                                edf = edf.drop(edf.index[i+1])
                            else:
                                edf = edf.drop(edf.index[i])
                        else:
                            # print(edf.iloc[i:i+2])
                            # raise Exception('Review how to handle 2x almost-equal earnings dates.')
                            # pass  # Can't do anything with certainty
                            # Keep earlier
                            if edf.index[i] < edf.index[i+1]:
                                edf = edf.drop(edf.index[i+1])
                            else:
                                edf = edf.drop(edf.index[i])

        return edf

    def _fetch_earnings_dates(self, limit, refresh=True):
        yfcu.TypeCheckInt(limit, "limit")
        yfcu.TypeCheckBool(refresh, "refresh")
        
        debug = False
        # debug = True

        if debug:
            print(f"{self.ticker}: _fetch_earnings_dates(limit={limit}, refresh={refresh})")
        elif print_fetches:
            print(f"{self.ticker}: fetching {limit} earnings dates")

        repeat_fetch = False
        try:
            df = self.dat.get_earnings_dates(limit)
        except KeyError as e:
            if "Earnings Date" in str(e):
                # Rarely, Yahoo returns a completely different table for earnings dates.
                # Try again.
                repeat_fetch = True
            else:
                raise
        if repeat_fetch:
            sleep(1)
            # Avoid cache this time, but add sleeps to maintain rate-limiting
            df = yf.Ticker(self.ticker).get_earnings_dates(limit)
            sleep(1)
        if df is None or df.empty:
            if debug:
                print("- Yahoo returned None")
            return None
        df['FetchDate'] = pd.Timestamp.utcnow().tz_convert(self.tzName)

        if df.shape[0] < limit:
            if debug:
                print("- detected earnings_dates start at", df.index.min())
            yfcm.WriteCacheMetadata(self.ticker, 'earnings_dates', 'start_date', df.index.min())

        cal = self.get_calendar(refresh)
        df['Date confirmed?'] = False
        if cal is not None and len(cal['Earnings Date']) == 1:
            x = cal['Earnings Date'][0]
            for dt in df.index:
                if abs(dt.date() - x) < timedelta(days=7):
                    # Assume same release
                    df.loc[dt, 'Date confirmed?'] = True
                    break

        df = self._clean_earnings_dates(df, refresh)

        return df

    def get_calendar(self, refresh=True):
        yfcu.TypeCheckBool(refresh, 'refresh')

        max_age = pd.Timedelta(yfcm._option_manager.max_ages.calendar)

        if self._calendar is None:
            if yfcm.IsDatumCached(self.ticker, "calendar"):
                self._calendar = yfcm.ReadCacheDatum(self.ticker, "calendar")

                self._calendar_clean = dict(self._calendar)
                del self._calendar_clean['FetchDate']
                if len(self._calendar_clean.keys()) == 0:
                    self._calendar_clean = None

        if (self._calendar is not None) and (self._calendar["FetchDate"] + max_age) > pd.Timestamp.now():
            return self._calendar_clean

        if not refresh:
            return self._calendar_clean

        if print_fetches:
            print(f"{self.ticker}: Fetching calendar (last fetch = {self._calendar['FetchDate'].date()})")

        c = self.dat.calendar
        c["FetchDate"] = pd.Timestamp.now()

        if self._calendar is not None:
            # Check calendar is not downgrade
            diff = len(c) - len(self._calendar)
            if diff < -1:
                # More than 1 element disappeared
                msg = "When fetching new calendar, data has disappeared\n"
                msg += "- cached calendar:\n"
                msg += f"{self._calendar}" + "\n"
                msg += "- new calendar:\n"
                msg += f"{c}" + "\n"
                raise Exception(msg)

        if c is not None:
            yfcm.StoreCacheDatum(self.ticker, "calendar", c)
        self._calendar = c
        self._calendar_clean = dict(self._calendar)
        del self._calendar_clean['FetchDate']
        if len(self._calendar_clean.keys()) == 0:
            self._calendar_clean = None
        return self._calendar_clean

    def _get_calendar_dates(self, refresh=True):
        yfcu.TypeCheckBool(refresh, 'refresh')

        debug = False
        # debug = True

        if debug:
            print(f"_get_calendar_dates(refresh={refresh})")

        cal = self.get_calendar(refresh)
        if cal is None or len(cal) == 0:
            return None
        if debug:
            print(f"- cal = {cal}")

        cal_release_dates = []
        cal_release_dates.sort()
        last = None
        for d in cal["Earnings Date"]:
            if last is None:
                last = d
            else:
                diff = d - last
                if debug:
                    print(f"- diff = {diff}")
                if diff <= timedelta(days=15):
                    # Looks like a date range so tag last-added date as estimate. And change data to be middle of range
                    last = yfcd.DateRange(last, d)
                    cal_release_dates.append(last)
                    last = None
                else:
                    print("- cal_release_dates:") ; print(cal_release_dates)
                    print("- diff =", diff)
                    raise Exception(f"Implement/rejig this execution path (tkr={self.ticker})")
        if last is not None:
            cal_release_dates.append(last)
        if debug:
            print(f"- cal_release_dates = {cal_release_dates}")
        if debug:
            if len(cal_release_dates) == 0:
                print("- cal_release_dates: EMPTY")
            else:
                print("- cal_release_dates:")
                for e in cal_release_dates:
                    print(e)

        return cal_release_dates
