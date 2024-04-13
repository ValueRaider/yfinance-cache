import numpy as np
import unittest
from pprint import pprint

import os
import time
def take_directory_snapshot(directory_path):
    snapshot = {}
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                stats = os.stat(file_path)
                snapshot[file_path] = {
                    'size': stats.st_size,
                    'modification_time': time.ctime(stats.st_mtime)
                }
            except FileNotFoundError:
                # The file might have been deleted between os.walk and os.stat
                continue
    return snapshot


class Test_Base(unittest.TestCase):
    def verify_df(self, df, answer, rtol=None, different=False):
        if (df is None or df.shape[0]==0) and (answer is None or answer.shape[0]==0):
            return

        if df is None:
            print("df is None but answer is:")
            print(answer)
            raise Exception("df is None")

        if df.shape[0] != answer.shape[0]:
            # print("df:")
            # print(df)
            # print("answer:")
            # print(answer)

            missing_from_answer = sorted(list(set(df.index)-set(answer.index)))
            print("missing_from_answer: #",len(missing_from_answer))
            if len(missing_from_answer) <= 5:
                pprint(missing_from_answer)
            else:
                pprint("{} ...".format(missing_from_answer[0:5]))

            if len(missing_from_answer) > 0:
                print("First day missing/different from answer:")
                print("df:")
                print(df[df.index.date==missing_from_answer[0].date()])
                print("answer:")
                print(answer[answer.index.date==missing_from_answer[0].date()])

            missing_from_df = sorted(list(set(answer.index)-set(df.index)))
            print("missing_from_df: #",len(missing_from_df))
            if len(missing_from_df) <= 5:
                pprint(missing_from_df)
            else:
                pprint("{} ...".format(missing_from_df[0:5]))

            if len(missing_from_df) > 0:
                print("First day missing/different from df:")
                print("df:")
                print(df[df.index.date==missing_from_df[0].date()])
                print("answer:")
                print(answer[answer.index.date==missing_from_df[0].date()])

            raise Exception("Different #rows: df={}, answer={}".format(df.shape[0], answer.shape[0]))

        if "Final?" in df.columns:
            last_row_final = df["Final?"].values[-1]
        else:
            last_row_final = True

        dcs = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
        dcs += ['CDF', 'CSF']
        for dc in dcs:
            if not (dc in df.columns and dc in answer.columns):
                continue

            # if (rtol is None) or (rtol == 0):
            #     f = np.array_equal(df[dc], answer[dc], equal_nan=True)
            # else:
            if rtol is None:
                rtol = 0.0
            if dc == 'Volume':
                f = np.abs(df[dc].values - answer[dc].values) <= 1
            else:
                f = np.isclose(df[dc].values, answer[dc].values, equal_nan=True, rtol=rtol)

            if different:
                # Test requires difference, not equality
                f = ~f
            else:
                if not last_row_final:
                    ## Ignore last row because data is live, can change between YF and YFC calls
                    f[-1] = True

            try:
                self.assertTrue(f.all())
            except:
                f = ~f
                debug_cols_to_print = [dc]
                if not dc in ['CSF', 'CDF']:
                    debug_cols_to_print += [c for c in ["CSF","CDF"] if c in df.columns]
                if sum(f) < 20:
                    if different:
                        print("{}/{} matches in column {} (expected differences):".format(sum(f), df.shape[0], dc))
                    else:
                        print("{}/{} differences in column {}:".format(sum(f), df.shape[0], dc))
                    print("- answer:")
                    print(answer[f][[dc]])
                    print("- result:")
                    print(df[f][debug_cols_to_print])
                else:
                    if different:
                        print("{}/{} matches in column {} (expected differences)".format(sum(f), df.shape[0], dc))
                    else:
                        print("{}/{} diffs in column {}".format(sum(f), df.shape[0], dc))

                last_diff_idx = np.where(f)[0][-1]
                x = df[dc].iloc[last_diff_idx]
                y = answer[dc].iloc[last_diff_idx]
                last_diff = x - y
                print("- last_diff: {} - {} = {}".format(x, y, last_diff))
                print("- answer:")
                print(answer.iloc[last_diff_idx])
                print("- result:")
                print(df.iloc[last_diff_idx])
                raise
