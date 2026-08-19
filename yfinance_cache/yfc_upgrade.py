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
    actions = ["have-recommended-verify"]

    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    if not os.path.isdir(yfc_dp):
        return
    for f in os.listdir(yfc_dp):
        if f not in actions:
            os.remove(os.path.join(yfc_dp, f))


def fix_timezone_column(df, column_name, target_tz, debug=False):
    """
    Automatically detect and fix timezone issues in a column.
    
    Handles:
    - Corrupted timezone metadata
    - Wrong timezone offsets (LMT)
    - NaT values
    - Already-correct timezones
    
    Returns: Fixed series with correct timezone
    """
    
    if debug:
        print(f"\n{'='*80}")
        print(f"Diagnosing column: {column_name}")
        print(f"{'='*80}")
    
    col = df[column_name]
    
    # Check 1: What's the dtype?
    if debug:
        print(f"dtype: {col.dtype}")
    
    # Check 2: Can we access .dt.tz?
    can_access_dt = False
    current_tz = None
    try:
        current_tz = col.dt.tz
        can_access_dt = True
        if debug:
            print(f"✓ Can access .dt.tz: {current_tz}")
    except AttributeError as e:
        if debug:
            print(f"✗ Cannot access .dt.tz: {e}")
        can_access_dt = False
    except Exception as e:
        if debug:
            print(f"✗ Error accessing .dt.tz: {type(e).__name__}: {e}")
        can_access_dt = False
    
    # Check 3: Sample a value to see what it looks like
    first_valid_idx = col.first_valid_index()
    if first_valid_idx is not None:
        sample_value = col.loc[first_valid_idx]
        if debug:
            print(f"Sample value: {sample_value}")
            print(f"Sample value type: {type(sample_value)}")
    else:
        if debug:
            print("No valid values found - all NaT")
        return pd.Series(pd.NaT, index=df.index, dtype=f'datetime64[us, {target_tz}]')
    
    # DECISION TREE:
    
    # Case 1: .dt works and timezone is already correct
    if can_access_dt and current_tz is not None:
        if str(current_tz) == target_tz:
            if debug:
                print(f"✓ Already correct timezone: {target_tz}")
            return col
        else:
            # Timezone is accessible but wrong - try simple convert
            if debug:
                print(f"Attempting .dt.tz_convert('{target_tz}')...")
            try:
                result = col.dt.tz_convert(target_tz)
                if debug:
                    print(f"✓ SUCCESS with .dt.tz_convert()")
                return result
            except Exception as e:
                if debug:
                    print(f"✗ .dt.tz_convert() failed: {e}")
                    print("Falling through to nuclear option...")
    
    # Case 2: .dt doesn't work - nuclear option
    if debug:
        print(f"\nUsing nuclear option: rebuild from scratch")
    
    # Get the underlying array
    try:
        # Try to get values as int64
        if hasattr(col, 'array'):
            arr = col.array
            if hasattr(arr, '_ndarray'):
                raw_int64 = arr._ndarray.view('int64')
            else:
                raw_int64 = col.values.view('int64')
        else:
            raw_int64 = col.values.view('int64')
        
        if debug:
            print(f"Extracted raw int64 values")
            print(f"Sample raw value: {raw_int64[0] if len(raw_int64) > 0 else 'none'}")
        
        # Check if values are reasonable for nanoseconds or microseconds
        sample = raw_int64[0] if len(raw_int64) > 0 else 0
        
        # Detect unit based on magnitude
        # Nanoseconds since epoch: ~1.7e18 for year 2025
        # Microseconds since epoch: ~1.7e15 for year 2025
        # Milliseconds since epoch: ~1.7e12 for year 2025
        
        abs_sample = abs(sample)
        
        if abs_sample > 1e17:
            unit = 'ns'
            if debug:
                print(f"Detected unit: nanoseconds (value ~{abs_sample:.2e})")
        elif abs_sample > 1e14:
            unit = 'us'
            if debug:
                print(f"Detected unit: microseconds (value ~{abs_sample:.2e})")
        elif abs_sample > 1e11:
            unit = 'ms'
            if debug:
                print(f"Detected unit: milliseconds (value ~{abs_sample:.2e})")
        else:
            unit = 's'
            if debug:
                print(f"Detected unit: seconds (value ~{abs_sample:.2e})")
        
        # Convert with error handling for invalid values
        result = pd.to_datetime(raw_int64, unit=unit, errors='coerce')
        result = pd.Series(result, index=df.index)
        
        # Localize to UTC first, then convert
        result = result.dt.tz_localize('UTC')
        result = result.dt.tz_convert(target_tz)
        
        if debug:
            print(f"✓ SUCCESS - rebuilt from {unit}")
        return result
        
    except Exception as e:
        if debug:
            print(f"✗ Nuclear option failed: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        raise


_RECOMMEND_VERIFY = """\
!!! YFC user: I highly recommend you run
!!! a full verification of cached prices:
!!!
!!!     yfc.verify_cached_tickers_prices(correct='all')
!!!
!!! Reason: yfinance 1.6.0 price repair has big improvements,
!!!         and YFC cache probably has a lot of bad price data.
!!!
!!! If you interrupt verification, resume with `resume_from_ticker`:
!!!
!!!     yfc.verify_cached_tickers_prices(
!!!         correct='all',
!!!         resume_from_ticker='AAPL')
!!!
!!! This message stops once you begin a full verification.
"""


def _recommend_verify():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-recommended-verify")
    if os.path.isfile(state_fp):
        return
    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    import logging
    logger = logging.getLogger(__name__)
    logger.warning(_RECOMMEND_VERIFY)


def _migrate_dfs_to_pandas3():
    d = yfcm.GetCacheDirpath()
    yfc_dp = os.path.join(d, "_YFC_")
    state_fp = os.path.join(yfc_dp, "have-fixed-df-tz-for-pandas3")
    if os.path.isfile(state_fp):
        return
    if not os.path.isdir(d):
        if not os.path.isdir(yfc_dp):
            os.makedirs(yfc_dp)
        with open(state_fp, 'w'):
            pass
        return

    debug = False

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

    # Somehow, some dataframe timezones are old format.
    # Ensure they are in modern timezones for Pandas 3.

    # contents = ['BDX']
    contents = ['COST']
    debug = True

    for d in contents:
        print(d)
        dpd = os.path.join(dp, d)
        if d.startswith("exchange-"):
            xcal_fp = os.path.join(dpd, "cal.pkl")
            if os.path.isfile(xcal_fp):
                # print(xcal_fp)
                try:
                    with open(xcal_fp, 'rb') as F:
                        data = pkl.load(F)
                except AttributeError as e:
                    if "__nat_unpickle" in str(e):
                        # Pandas 3 does not like df pickled with Pandas 2
                        # print("Delete: " + xcal_fp)
                        continue
                except NotImplementedError as e:
                    if 'datetime64' in str(e) and 'array' in str(e):
                        # Pandas 2 does not like df pickled with Pandas 3
                        # print("Delete: " + xcal_fp)
                        continue

                xcal = data['data']
                df = xcal.schedule
                tz = df.index[0].tzinfo
                index2 = df.index.tz_convert(str(tz))
                if (index2 != index).any():
                    xcal.schedule.index = index2
                    data['data'] = xcal
                    with open(fp, 'wb') as F:
                        pkl.dump(data, F, 4)

        else:
            # handle ticker
            contents2 = os.listdir(dpd)

            for f in contents2:
                if not f.endswith('.pkl'):
                    continue
                elif f in ['annuals.pkl', 'quarterlys.pkl']:
                    continue

                if debug:
                    print("- checking:", f)

                fp = os.path.join(dpd, f)
                with open(fp, 'rb') as bb:
                    data = pkl.load(bb)
                df = data['data']
                if not isinstance(df, pd.DataFrame):
                    continue
                if df.empty:
                    continue

                changed = False

                if hasattr(df.index[0], 'tzinfo'):
                    tz = df.index[0].tzinfo
                    index2 = df.index.tz_convert(str(tz))
                    # if (index2 != df.index).any():
                    # shift = index2[0] - df.index[0]
                    # if shift != pd.Timedelta(0):
                    if index2.tzinfo != tz:
                        if debug:
                            print("- index tz changed")
                        df.index = index2
                        changed = True
                for c in df.columns:
                    if debug:
                        print("- - column:", c)
                    if hasattr(df[c].iloc[0], 'tzinfo'):
                        # tz = df[c].iloc[0].tzinfo
                        try:
                            tz = df[c].dropna().iloc[0].tzinfo
                        except IndexError as e:
                            if str(e) == 'single positional indexer is out-of-bounds':
                                # No true values
                                continue
                            else:
                                raise
                        if debug:
                            print("- - - tz:", tz, type(tz))
                        if tz is None:
                            # whoops, local system time. Lets get a tz on it
                            from datetime import datetime
                            now = datetime.now().astimezone()
                            tz = str(now.tzinfo)
                            # print("- - - tz:", tz)
                            if debug:
                                print(f"- - '{c}' set missing tz")
                            try:
                                df[c] = df[c].dt.tz_localize(tz)
                                # c2 = df[c].dt.tz_localize(tz)
                                changed = True
                            except:
                                if debug:
                                    print(df)
                                raise
                        else:
                            dt0 = df[c].iloc[0]
                            if debug:
                                print("- - - dt0:", dt0)
                            try:
                                c2 = df[c].dt.tz_convert(str(tz))
                                # print(df[c])
                                # df[c] = df[c].dt.tz_localize(None).dt.tz_localize('America/New_York')
                                # print(df[c])
                                # raise Exception('review')
                            except AttributeError as e:
                                if str(e) == "'NoneType' object has no attribute 'timezone'":
                                    if debug:
                                        print(f"- - '{c}' tz was corrupt")
                                        print(df[c])
                                    # The underlying tz info is corrupt. Rebuild
                                    # Extract raw int64 values (bypasses ALL timezone logic)
                                    # # Create naive datetime from raw values
                                    # # c2 = pd.to_datetime(raw_values, unit='us')
                                    # c2 = pd.to_datetime(raw_values, unit='us', errors='coerce')

                                    # # raw_values = df[c].values.view('int64')
                                    # raw_values = df[c].values.view('int64')
                                    # # Check for valid values
                                    # nat_mask = raw_values == np.iinfo(np.int64).min
                                    # min_valid = pd.Timestamp('1677-09-22').value
                                    # max_valid = pd.Timestamp('2262-04-11').value
                                    # valid_mask = (raw_values >= min_valid) & (raw_values <= max_valid) & ~nat_mask
                                    # # Create series, invalid → NaT
                                    # c2 = pd.Series(pd.NaT, index=df.index, dtype='datetime64[us]')
                                    # if valid_mask.any():
                                    #     c2.loc[valid_mask] = pd.to_datetime(raw_values[valid_mask], unit='us')

                                    # # Add clean timezone
                                    # c2 = c2.tz_localize('UTC')
                                    # c2 = c2.tz_convert(str(tz))
                                    # c2 = pd.Series(c2)
                                    # # print(df[c])
                                    # print(c2)
                                    # c2 = pd.Series(c2, index=df.index)
                                    # print(c2)
                                    # # df[c] = c2
                                    # # print(df[c])
                                    # raise Exception('review corruption fix')
                                    
                                    c2 = fix_timezone_column(df, c, str(tz))

                                    # print(c2)
                                    # raise Exception('review c2')

                                    if debug:
                                        print("- - corruption fixed")
                                else:
                                    raise
                            # print("# c:") ; print(df[c])
                            # print("# c2:") ; print(c2)
                            # if changed or (c2 != df[c]).any():
                            # shift = df[c].iloc[0] - c2.iloc[0]
                            # if shift != pd.Timedelta(0):
                            if debug:
                                print("- - - checking if tz changed")
                            if c2.dropna().iloc[0].tzinfo != tz:
                                if debug:
                                    print(f"- - '{c}' tz changed")
                                df[c] = c2
                                changed = True

                if changed:
                    if debug:
                        print("- - changed so writing out")
                    # print(df)
                    data['data'] = df
                    with open(fp, 'wb') as F:
                        pkl.dump(data, F, 4)


            # calendars
            # earnings_dates

            # shares

            # history-*

    # raise Exception('review')

    if not os.path.isdir(yfc_dp):
        os.makedirs(yfc_dp)
    with open(state_fp, 'w'):
        pass


#
