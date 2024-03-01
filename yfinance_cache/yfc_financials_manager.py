import yfinance as yf

from . import yfc_cache_manager as yfcm
from . import yfc_dat as yfcd
from . import yfc_utils as yfcu

import numpy as np
import pandas as pd
import scipy.stats as stats
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
# import calendar
import os
from statistics import mean, stdev
from decimal import Decimal
from pprint import pprint


d_today = date.today()


class ComparableRelativedelta(relativedelta):
    def _have_same_attributes(self, other):
        attrs = ['years', 'months', 'days', 'leapdays', 'hours', 'minutes', 'seconds', 'microseconds', 'year', 'month', 'day', 'weekday']

        for a in attrs:
            if getattr(self, a, 0) == 0:
                if getattr(other, a, 0) != 0:
                    return False
        return True

    def __eq__(self, other):
        if isinstance(other, relativedelta):
            attrs = ['years', 'months', 'days', 'leapdays', 'hours', 'minutes', 'seconds', 'microseconds', 'year', 'month', 'day', 'weekday']
            return all(getattr(self, attr, 0) == getattr(other, attr, 0) for attr in attrs)

        raise NotImplementedError(f'Not implemented ComparableRelativedelta={self} == {type(other)}={other}')

    def __lt__(self, other):
        if isinstance(other, (TimedeltaEstimate, TimedeltaRangeEstimate)):
            return other.__gt__(self)

        elif isinstance(other, (relativedelta, timedelta)):
            # Reference date for comparison
            reference_date = date(2000, 1, 1)

            # Apply each relativedelta to the reference date
            result_date_self = reference_date + self
            result_date_other = reference_date + other

            if not self._have_same_attributes(other):
                # Threshold for undefined comparison
                threshold = timedelta(days=7)
                # threshold = timedelta(days=3)
                # Check if the difference is within the threshold
                if abs(result_date_self - result_date_other) < threshold:
                    raise ValueError(f"Comparison is undefined due to small difference ({self} - {other})")

            return result_date_self < result_date_other

        raise NotImplementedError(f'Not implemented ComparableRelativedelta={self} < {type(other)}={other}')

    def __le__(self, other):
        return (not self.__gt__(other))

    def __gt__(self, other):
        if isinstance(other, (TimedeltaEstimate, TimedeltaRangeEstimate)):
            return other.__lt__(self)

        elif isinstance(other, (relativedelta, timedelta)):
            # Reference date for comparison
            reference_date = date(2000, 1, 1)

            # Apply each relativedelta to the reference date
            result_date_self = reference_date + self
            result_date_other = reference_date + other

            if not self._have_same_attributes(other):
                # Threshold for undefined comparison
                threshold = timedelta(days=7)
                # threshold = timedelta(days=3)
                # Check if the difference is within the threshold
                if abs(result_date_self - result_date_other) < threshold:
                    raise ValueError("Comparison is undefined due to small difference")

            return result_date_self > result_date_other
        raise NotImplementedError(f'Not implemented ComparableRelativedelta={self} > {type(other)}={other}')

    def __ge__(self, other):
        return (not self.__lt__(other))


interval_str_to_days = {}
interval_str_to_days['ANNUAL'] = ComparableRelativedelta(years=1)
interval_str_to_days['HALF'] = ComparableRelativedelta(months=6)
interval_str_to_days['QUART'] = ComparableRelativedelta(months=3)


confidence_to_buffer = {}
confidence_to_buffer[yfcd.Confidence.Medium] = timedelta(days=7)
confidence_to_buffer[yfcd.Confidence.Low] = timedelta(days=45)


class TimedeltaEstimate():
    def __init__(self, td, confidence):
        if not isinstance(confidence, yfcd.Confidence):
            raise Exception("'confidence' must be a 'Confidence' object, not {0}".format(type(confidence)))
        if not isinstance(td, (timedelta, pd.Timedelta, ComparableRelativedelta)):
            raise Exception("'td' must be a 'timedelta' object or None, not {0}".format(type(td)))
        if isinstance(td, ComparableRelativedelta) and confidence != yfcd.Confidence.High:
            # raise Exception('Actually, TimedeltaEstimate with a ComparableRelativedelta is a problem')
            # Convert to normal timedelta
            td = timedelta(days=td.years*365 +td.months*30 +td.days)
        self.td = td
        self.confidence = confidence
        self.uncertainty = confidence_to_buffer[confidence]

    def copy(self):
        return TimedeltaEstimate(self.td, self.confidence)

    def __str__(self):
        s = f"{self.td} ({self.confidence})"
        return s

    def __repr__(self):
        return self.__str__()

    def __abs__(self):
        return TimedeltaEstimate(abs(self.td), self.confidence)

    def __eq__(self, other):
        raise NotImplementedError(f'Not implemented TimedeltaEstimate={self} == {type(other)}={other}')

    def isclose(self, other):
        if isinstance(other, TimedeltaEstimate):
            conf = min(self.confidence, other.confidence)
            return abs(self.td - other.td) < confidence_to_buffer[conf]

        elif isinstance(other, (timedelta, pd.Timedelta, ComparableRelativedelta)):
            return ((self.td - self.uncertainty) < other) and (other < (self.td + self.uncertainty))
        
        raise NotImplementedError(f'Not implemented TimedeltaEstimate={self} is-close-to {type(other)}={other}')

    def __iadd__(self, other):
        if isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            self.td += other
            return self
        raise NotImplementedError(f'Not implemented TimedeltaEstimate={self} += {type(other)}={other}')
    def __add__(self, other):
        if isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            return TimedeltaEstimate(self.td + other, self.confidence)
        elif isinstance(other, date):
            return DateEstimate(self.td + other, self.confidence)
        elif isinstance(other, DateEstimate):
            return DateEstimate(self.td + other.date, min(self.confidence, other.confidence))
        raise NotImplementedError(f'Not implemented TimedeltaEstimate={self} + {type(other)}={other}')

    def __sub__(self, other):
        if isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            return TimedeltaEstimate(self.td - other, self.confidence)
        elif isinstance(other, date):
            return DateEstimate(self.td - other, self.confidence)
        raise NotImplementedError(f'Not implemented TimedeltaEstimate={self} - {type(other)}={other}')

    def __imul__(self, other):
        if isinstance(other, int):
            self.td *= other
            return self
        raise NotImplementedError(f'Not implemented TimedeltaEstimate={self} *= {type(other)}={other}')

    def __lt__(self, other):
        if isinstance(other, TimedeltaRangeEstimate):
            return other.__gt__(self)

        elif isinstance(other, TimedeltaEstimate):
            if self.td + self.uncertainty < other.td - other.uncertainty:
                return True
            elif other.td + other.uncertainty <= self.td - self.uncertainty:
                return False
            else:
                raise Exception(f'Ambiguous whether {self} < {other}')

        elif isinstance(other, (relativedelta, timedelta, ComparableRelativedelta)):
            if self.td + self.uncertainty < other:
                return True
            elif other <= self.td - self.uncertainty:
                return False
            else:
                raise Exception(f'Ambiguous whether {self} < {other}')

        raise NotImplementedError(f'Not implemented TimedeltaEstimate={self} < {type(other)}={other}')

    def __gt__(self, other):
        if isinstance(other, TimedeltaRangeEstimate):
            return other.__lt__(self)

        elif isinstance(other, TimedeltaEstimate):
            if self.td - self.uncertainty > other.td + other.uncertainty:
                return True
            elif self.td + self.uncertainty <= other.td - other.uncertainty:
                return False
            else:
                raise Exception(f'Ambiguous whether {self} is > {other}')

        elif isinstance(other, (relativedelta, timedelta, ComparableRelativedelta)):
            if self.td - self.uncertainty > other:
                return True
            elif self.td + self.uncertainty < other:
                return False
            else:
                raise Exception(f'Ambiguous whether {self} is > {other}')

        raise NotImplementedError(f'Not implemented TimedeltaEstimate={self} > {type(other)}={other}')

    def __ge__(self, other):
        # return self.isclose(other) or self.__gt__(other)
        return self.__gt__(other)

    def __le__(self, other):
        # return self.isclose(other) or self.__lt__(other)
        return self.__lt__(other)

    def __mod__(self, other):
        if isinstance(other, timedelta):
            if self.td < timedelta(0):
                raise NotImplementedError('Not implemented modulus of negative TimedeltaEstimate')
            else:
                td = self.td
                while td > other:
                    td -= other
                return TimedeltaEstimate(td, self.confidence)
        raise NotImplementedError(f'Not implemented TimedeltaEstimate={self} modulus-of {type(other)}={other}')

    def __truediv__(self, other):
        if isinstance(other, (int, float)):
            return TimedeltaEstimate(self.td / other, self.confidence)
        raise NotImplementedError(f'Not implemented TimedeltaEstimate={self} / {type(other)}={other}')


class TimedeltaRangeEstimate():
    def __init__(self, td1, td2, confidence):
        if not isinstance(confidence, yfcd.Confidence):
            raise Exception("'confidence' must be a 'Confidence' object, not {0}".format(type(confidence)))
        if (td1 is not None) and not isinstance(td1, (timedelta, pd.Timedelta)):
            raise Exception("'td1' must be a 'timedelta' object or None, not {0}".format(type(td1)))
        if (td2 is not None) and not isinstance(td2, (timedelta, pd.Timedelta)):
            raise Exception("'td2' must be a 'timedelta' object or None, not {0}".format(type(td2)))
        if td2 <= td1:
            swap = td1 ; td1 = td2 ; td2 = swap
        self.td1 = td1
        self.td2 = td2
        self.confidence = confidence
        self.uncertainty = confidence_to_buffer[confidence]

    def __str__(self):
        s = f"TimedeltaRangeEstimate {self.td1} -> {self.td2} ({self.confidence})"
        return s

    def __repr__(self):
        return self.__str__()

    def isclose(self, other):
        # if isinstance(other, TimedeltaEstimate):
        #     conf = min(self.confidence, other.confidence)
        #     # return abs(self.td - other.td) < confidence_to_buffer[conf]
        # elif isinstance(other, (timedelta, pd.Timedelta, ComparableRelativedelta)):
        #     b = self.uncertainty
        #     return ((self.td - b) < other) and (other < (self.td + b))
        raise NotImplementedError(f'Not implemented TimedeltaRangeEstimate={self} is-close-to {type(other)}={other}')

    def __add__(self, other):
        if isinstance(other, date):
            return DateRangeEstimate(self.td1 + other, self.td2 + other, self.confidence)
        raise NotImplementedError(f'Not implemented TimedeltaRangeEstimate={self} + {type(other)}={other}')

    def __lt__(self, other):
        if isinstance(other, (timedelta, pd.Timedelta, ComparableRelativedelta)):
            if self.td2 + self.uncertainty < other:
                return True
            elif self.td2 - self.uncertainty >= other:
                return True
            else:
                raise Exception(f"Ambigious whether {self} < {other}")

        elif isinstance(other, TimedeltaEstimate):
            if self.td2 + self.uncertainty < other.td - other.uncertainty:
                return True
            elif self.td2 - self.uncertainty >= other.td + other.uncertainty:
                return False
            else:
                raise Exception(f"Ambigious whether {self} < {other}")

        raise NotImplementedError(f'Not implemented TimedeltaRangeEstimate={self} < {type(other)}={other}')

    def __gt__(self, other):
        if isinstance(other, (timedelta, pd.Timedelta, ComparableRelativedelta)):
            if self.td1 - self.uncertainty > other:
                return True
            elif self.td1 + self.uncertainty <= other:
                return True
            else:
                raise Exception(f"Ambigious whether {self} > {other}")
                
        elif isinstance(other, TimedeltaEstimate):
            if self.td1 - self.uncertainty > other.td + other.uncertainty:
                return True
            elif self.td1 + self.uncertainty <= other.td - other.uncertainty:
                return False
            else:
                raise Exception(f"Ambigious whether {self} > {other}")

        raise NotImplementedError(f'Not implemented TimedeltaRangeEstimate={self} > {type(other)}={other}')

    def __le__(self, other):
        if self.__lt__(other):
            return True
        elif self.__gt__(other):
            return False
        raise NotImplementedError(f'Not implemented TimedeltaRangeEstimate={self} <= {type(other)}={other}')
        
    def __ge__(self, other):
        if self.__gt__(other):
            return True
        elif self.__lt__(other):
            return False
        raise NotImplementedError(f'Not implemented TimedeltaRangeEstimate={self} >= {type(other)}={other}')
        
    def __abs__(self):
        return TimedeltaRangeEstimate(abs(self.td1), abs(self.td2), self.confidence)


class TimedeltaRange():
    def __init__(self, td1, td2):
        if (td1 is not None) and not isinstance(td1, (timedelta, pd.Timedelta)):
            raise Exception("'td1' must be a 'timedelta' object or None, not {0}".format(type(td1)))
        if (td2 is not None) and not isinstance(td2, (timedelta, pd.Timedelta)):
            raise Exception("'td2' must be a 'timedelta' object or None, not {0}".format(type(td2)))
        if td2 <= td1:
            swap = td1 ; td1 = td2 ; td2 = swap
        self.td1 = td1
        self.td2 = td2

    def __str__(self):
        s = f"TimedeltaRange {self.td1} -> {self.td2}"
        return s

    def __repr__(self):
        return self.__str__()

    def __le__(self, other):
        if isinstance(other, (timedelta, pd.Timedelta)):
            return self.td2 <= other
        raise NotImplementedError(f'Not implemented TimedeltaRange={self} <= {type(other)}={other}')

    def __ge__(self, other):
        if isinstance(other, (timedelta, pd.Timedelta)):
            return self.td2 >= other
        raise NotImplementedError(f'Not implemented TimedeltaRange={self} >= {type(other)}={other}')


class DateRangeEstimate():
    def __init__(self, start, end, confidence):
        if (start is not None) and not isinstance(start, date):
            raise Exception("'start' must be a 'date' object or None, not {0}".format(type(start)))
        if (end is not None) and not isinstance(end, date):
            raise Exception("'end' must be a 'date' object or None, not {0}".format(type(end)))
        if not isinstance(confidence, yfcd.Confidence):
            raise Exception("'confidence' must be a 'Confidence' object, not {0}".format(type(confidence)))
        self.start = start
        self.end = end
        self.confidence = confidence
        self.uncertainty = confidence_to_buffer[confidence]

    def copy(self):
        return DateRangeEstimate(self.start, self.end, self.confidence)

    def __str__(self):
        s = f"{self.start} -> {self.end} ({self.confidence})"
        return s

    def __repr__(self):
        return self.__str__()

    def __lt__(self, other):
        if isinstance(other, date):
            if self.end + self.uncertainty < other:
                return True
            elif self.start - self.uncertainty >= other:
                return False
            else:
                raise Exception(f"Ambigious whether {self} < {other}")

        elif isinstance(other, DateEstimate):
            if self.end + self.uncertainty < other - other.uncertainty:
                return True
            elif self.start - self.uncertainty >= other + other.uncertainty:
                return False
            else:
                raise Exception(f"Ambigious whether {self} < {other}")

        elif isinstance(other, DateRangeEstimate):
            if self.end + self.uncertainty < other.start - other.uncertainty:
                return True
            elif self.start - self.uncertainty >= other.end + other.uncertainty:
                return False
            else:
                raise Exception(f"Ambigious whether {self} < {other}")

        raise NotImplementedError(f'Not implemented DateRangeEstimate={self} < {type(other)}={other}')

    def __le__(self, other):
        raise NotImplementedError(f'Not implemented DateRangeEstimate={self} <= {type(other)}={other}')

    def __gt__(self, other):
        if isinstance(other, DateEstimate):
            return other.__lt__(self)
        raise NotImplementedError(f'Not implemented DateRangeEstimate={self} > {type(other)}={other}')

    def __ge__(self, other):
        if isinstance(other, DateEstimate):
            return other.__le__(self)
        raise NotImplementedError(f'Not implemented DateRangeEstimate={self} >= {type(other)}={other}')

    def __eq__(self, other):
        raise NotImplementedError(f'Not implemented DateRangeEstimate={self} == {type(other)}={other}')

    def __iadd__(self, other):
        raise NotImplementedError(f'Not implemented DateRangeEstimate={self} += {type(other)}={other}')
    def __add__(self, other):
        raise NotImplementedError(f'Not implemented DateRangeEstimate={self} + {type(other)}={other}')
    def __radd__(self, other):
        raise NotImplementedError(f'Not implemented DateRangeEstimate={self} radd {type(other)}={other}')

    def __isub__(self, other):
        raise NotImplementedError(f'Not implemented DateRangeEstimate={self} -= {type(other)}={other}')
    def __sub__(self, other):
        if isinstance(other, DateEstimate):
            conf = min(self.confidence, other.confidence)
            return TimedeltaRangeEstimate(self.start - other, self.end - other, conf)
        raise NotImplementedError(f'Not implemented DateRangeEstimate={self} - {type(other)}={other}')
    def __rsub__(self, other):
        raise NotImplementedError(f'Not implemented DateRangeEstimate={self} rsub {type(other)}={other}')

    def __neg__(self):
        raise NotImplementedError(f'Not implemented DateRangeEstimate={self} negate')
    def __invert__(self):
        raise NotImplementedError(f'Not implemented DateRangeEstimate={self} invert')


class DateRange():
    def __init__(self, start, end):
        if (start is not None) and not isinstance(start, date):
            raise Exception("'start' must be a 'date' object or None, not {0}".format(type(start)))
        if (end is not None) and not isinstance(end, date):
            raise Exception("'end' must be a 'date' object or None, not {0}".format(type(end)))
        self.start = start
        self.end = end

    def copy(self):
        return DateRange(self.start, self.end)

    def __str__(self):
        s = f"({self.start} -> {self.end})"
        return s

    def __repr__(self):
        return self.__str__()

    def __lt__(self, other):
        if isinstance(other, date):
            if self.end <= other:
                return True
            elif other < self.start:
                return False
            raise NotImplementedError(f"Not sure how to implement date < date-range when date is inside range (date={other}, range={self}")

        elif isinstance(other, DateEstimate):
            return self.end <= other

        raise NotImplementedError(f'Not implemented DateRange={self} < {type(other)}={other}')

    def __le__(self, other):
        if isinstance(other, date):
            if self.end <= other:
                return True
            elif other < self.start:
                return False
            else:
                raise NotImplementedError(f"Not sure how to implement date < date-range when date is inside range (date={other}, range={self}")

        raise NotImplementedError(f'Not implemented DateRange={self} <= {type(other)}={other}')

    def __gt__(self, other):
        if isinstance(other, date):
            if self.start > other:
                return True
            elif other >= self.end:
                return False
            raise NotImplementedError(f"Not sure how to implement date > date-range when date is inside range (date={other}, range={self}")
        raise NotImplementedError(f'Not implemented DateRange={self} > {type(other)}={other}')

    def __ge__(self, other):
        if isinstance(other, date):
            return not self.__lt__(other)
        elif isinstance(other, DateEstimate):
            return self.start >= other

        raise NotImplementedError(f'Not implemented DateRange={self} >= {type(other)}={other}')

    def __eq__(self, other):
        raise NotImplementedError(f'Not implemented DateRange={self} == {type(other)}={other}')

    def __iadd__(self, other):
        raise NotImplementedError(f'Not implemented DateRange={self} += {type(other)}={other}')
    def __add__(self, other):
        raise NotImplementedError(f'Not implemented DateRange={self} + {type(other)}={other}')
    def __radd__(self, other):
        raise NotImplementedError(f'Not implemented DateRange={self} radd {type(other)}={other}')

    def __isub__(self, other):
        raise NotImplementedError(f'Not implemented DateRange={self} -= {type(other)}={other}')
    def __sub__(self, other):
        if isinstance(other, date):
            return TimedeltaRange(self.start - other, self.end - other)
        elif isinstance(other, DateEstimate):
            return TimedeltaRangeEstimate(self.start - other, self.end - other, other.confidence)
        raise NotImplementedError(f'Not implemented DateRange={self} - {type(other)}={other}')
    def __rsub__(self, other):
        raise NotImplementedError(f'Not implemented DateRange={self} rsub {type(other)}={other}')

    def __neg__(self):
        raise NotImplementedError(f'Not implemented DateRange={self} negate')
    def __invert__(self):
        raise NotImplementedError(f'Not implemented DateRange={self} invert')


class DateEstimate():
    def __init__(self, dt, confidence):
        if not isinstance(confidence, yfcd.Confidence):
            raise Exception("'confidence' must be a 'Confidence' object, not {0}".format(type(confidence)))
        if not isinstance(dt, (date, DateEstimate)):
            raise Exception("'dt' must be a 'date' object or None, not {0}".format(type(dt)))
        if isinstance(dt, DateEstimate):
            self.date = dt.date
            self.confidence = min(dt.confidence, confidence)
        else:
            self.date = dt
            self.confidence = confidence
        self.uncertainty = confidence_to_buffer[confidence]

    def copy(self):
        return DateEstimate(self.date, self.confidence)

    def __str__(self):
        s = f"{self.date} ({self.confidence})"
        return s

    def __repr__(self):
        return self.__str__()

    def __lt__(self, other):
        date_cautious = self.date + self.uncertainty
        if isinstance(other, date):
            if self.date < other:
                if date_cautious < other:
                    return True
                raise Exception(f'Ambiguous whether {self} is less-than {other}')
            elif other <= self.date:
                if other <= date_cautious:
                    return False
                raise Exception(f'Ambiguous whether {self} is less-than {other}')
            raise NotImplementedError('Code path')
        elif isinstance(other, DateEstimate):
            if self.date < other.date:
                other_date_cautious = other.date - self.uncertainty
                if date_cautious < other_date_cautious:
                    return True
                raise Exception(f'Ambiguous whether {self} is less-than {other}')
            elif self.date >= other.date:
                other_date_cautious = other.date + self.uncertainty
                if date_cautious >= other_date_cautious:
                    return False
                raise Exception(f'Ambiguous whether {self} is less-than {other}')
        elif isinstance(other, (DateRangeEstimate, DateRange)):
            return other.__ge__(self)

        raise NotImplementedError(f'Not implemented DateEstimate={self} < {type(other)}={other}')

    def __le__(self, other):
        # raise NotImplementedError(f'other={other}, {type(other)}')
        return not self.__gt__(other)

    def __gt__(self, other):
        if isinstance(other, date):
            if self.date > other:
                if self.date - self.uncertainty > other:
                    return True
                raise Exception(f'Ambiguous whether {self} is greater-than {other}')
            elif other >= self.date + self.uncertainty:
                if other >= self.date:
                    return False
                raise Exception(f'Ambiguous whether {self} is greater-than {other}')
            raise NotImplementedError('Code path')
        elif isinstance(other, DateEstimate):
            if self.date > other.date:
                if self.date - self.uncertainty > other.date + other.uncertainty:
                    return True
                raise Exception(f'Ambiguous whether {self} is greater-than {other}')
            elif other.date >= self.date:
                if other.date - other.uncertainty >= self.date + self.uncertainty:
                    return False
                raise Exception(f'Ambiguous whether {self} is greater-than {other}')

        elif isinstance(other, (DateRangeEstimate, DateRange)):
            return other.__le__(self)
        raise NotImplementedError(f'Not implemented DateEstimate={self} > {type(other)}={other}')

    def __ge__(self, other):
        # return other.__le__(self)
        return not self.__lt__(other)

    def __eq__(self, other):
        self_cautious_lower = self.date - self.uncertainty
        self_cautious_upper = self.date + self.uncertainty +timedelta(days=1)

        if isinstance(other, DateEstimate):
            other_cautious_lower = other.date - other.uncertainty
            other_cautious_upper = other.date + other.uncertainty +timedelta(days=1)
        else:
            other_cautious_lower = other
            other_cautious_upper = other +timedelta(days=1)

        if self_cautious_upper <= other_cautious_lower or other_cautious_upper <= self_cautious_lower:
            # Definitely not overlapping
            return False
        raise NotImplementedError(f'Not implemented DateEstimate={self} = {type(other)}={other}')

    def isclose(self, other):
        if isinstance(other, DateEstimate):
            return abs(self.date - other.date) < min(self.uncertainty, other.uncertainty)
        else:
            return abs(self.date - other) < self.uncertainty
        raise NotImplementedError(f'Not implemented DateEstimate={self} is-close-to {type(other)}={other}')

    def __iadd__(self, other):
        if isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            self.date += other
            return self
        raise NotImplementedError(f'Not implemented DateEstimate={self} += {type(other)}={other}')
    def __add__(self, other):
        if isinstance(other, TimedeltaEstimate):
            return DateEstimate(self.date+other.td, min(self.confidence, other.confidence))
        elif isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            return DateEstimate(self.date+other, self.confidence)
        raise NotImplementedError(f'Not implemented DateEstimate={self} + {type(other)}={other}')
    def __radd__(self, other):
        return self.__add__(other)

    def __isub__(self, other):
        if isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            self.date -= other
            return self
        raise NotImplementedError(f'Not implemented DateEstimate={self} -= {type(other)}={other}')
    def __sub__(self, other):
        if isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            return DateEstimate(self.date-other, self.confidence)
        elif isinstance(other, DateEstimate):
            td = self.date - other.date
            c0 = self.confidence
            c1 = other.confidence
            if min(c0, c1) == yfcd.Confidence.Medium:
                return TimedeltaEstimate(td, min(c0, c1))
            else:
                print(c0, type(c0))
                print(c1, type(c1))
                raise NotImplementedError(f"__sub__ not implemented between DateEstimates with confidences {c0} and {c1}")
        elif isinstance(other, date):
            # return self.date - other
            return TimedeltaEstimate(self.date-other, self.confidence)
        raise NotImplementedError(f'Not implemented DateEstimate={self} - {type(other)}={other}')
    def __rsub__(self, other):
        if isinstance(other, DateEstimate):
            return other.date - self.date
        elif isinstance(other, date):
            return other - self.date
        raise NotImplementedError(f'Not implemented DateEstimate={self} rsub {type(other)}={other}')

    def __neg__(self):
        raise NotImplementedError(f'Not implemented DateEstimate={self} negate')
    def __invert__(self):
        raise NotImplementedError(f'Not implemented DateEstimate={self} invert')


class EarningsRelease():
    def __init__(self, period_end, release_date):
        if (period_end is not None) and not isinstance(period_end, (date, DateEstimate)):
            raise Exception("'period_end' must be a 'DateEstimate' or date object or None, not {0}".format(type(period_end)))
        if (release_date is not None):
            if not isinstance(release_date, (date, DateEstimate)):
                raise Exception("'release_date' must be a 'DateEstimate' or date object or None, not {0}".format(type(release_date)))
            if release_date < period_end:
                raise Exception("release_date={0} cannot occur before period_end={1}".format(release_date, period_end))
            if release_date > (period_end + timedelta(days=90)):
                raise Exception("release_date={0} shouldn't occur 90 days after period_end={1}".format(release_date, period_end))
        self.period_end = period_end
        self.release_date = release_date

    def __str__(self):
        s = "Released at unknown date" if self.release_date is None else "Released on {0}".format(self.release_date)
        s += ": "
        s += " for period ending {0}".format(self.period_end)
        return s

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

        # self.logger = None

        self._earnings = None
        self._quarterly_earnings = None
        self._income_stmt = None
        self._quarterly_income_stmt = None
        self._balance_sheet = None
        self._quarterly_balance_sheet = None
        self._cashflow = None
        self._quarterly_cashflow = None

        self._earnings_dates = None
        self._calendar = None

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
            print(f"_get_fin_table({finType}, {period}, refresh={refresh})")

        if not isinstance(finType, yfcd.Financials):
            raise Exception('Argument finType must be type Financials')
        if not isinstance(period, yfcd.ReportingPeriod):
            raise Exception('Argument period must be type ReportingPeriod')

        # if period == 'quarterly':
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
                dates = self._get_release_dates(finType, period, refresh=False)
                if debug:
                    print("- dates:") ; print(dates)
                if dates is None:
                    # Use crude logic to estimate when to re-fetch
                    if 'LastFetch' in md.keys():
                        do_fetch = md['LastFetch'] < (dt_now - td_1d*30)
                    else:
                        do_fetch = True
                else:
                    idx = dates.index.get_loc(df.columns.max().date())
                    if idx == dates.shape[0] - 1:
                        raise Exception('Function _get_release_dates() failed to return estimations of future earnings releases')
                    next_release = dates.iloc[idx+1]
                    if debug:
                        print("- next_release:", next_release.to_dict(), 'period ending =', next_release.name)
                    rd = next_release['Release date']
                    rdc = yfcd.Confidence(int(next_release['RD confidence']))
                    if rdc != yfcd.Confidence.High:
                        rde = DateEstimate(rd, rdc)
                        if rde.isclose(d_today):
                            next_release_in_future = False
                        else:
                            next_release_in_future = rde > d_today
                    else:
                        next_release_in_future = rd > d_today
                    if debug:
                        print("- next_release_in_future =", next_release_in_future)
                    if not next_release_in_future:
                        if debug:
                            print("- expect new release, but did we already fetch recently?")
                        rd_delta = rd - dt_now.date()
                        if rdc == yfcd.Confidence.Medium and rd_delta > pd.Timedelta(7, unit='d'):
                            if 'LastFetch' in md.keys() and md['LastFetch'] < dt_now - td_1d*3:
                                do_fetch = True
                        elif rdc == yfcd.Confidence.Low and rd_delta > pd.Timedelta(30, unit='d'):
                            if 'LastFetch' in md.keys() and md['LastFetch'] < dt_now - td_1d*7:
                                do_fetch = True

        if debug:
            print("- do_fetch =", do_fetch)
        if do_fetch:
            if df is not None:
                raise Exception('Review whether should be re-fetching financials')
            df_new = getattr(self.dat, name)
            fetch_dt = pd.Timestamp.utcnow().tz_convert(self.tzName)
            if md is None:
                md_old = None
                md = {'FetchDates':{}}
            else:
                md_old = dict(md)
            for dt in df_new.columns:
                md['FetchDates'][dt] = fetch_dt
            md['LastFetch'] = fetch_dt
            if df is None or df.empty:
                df = df_new
            elif df_new is not None and not df_new.empty:
                df_pruned = df.drop([c for c in df.columns if c in df_new], axis=1)
                if df_pruned.empty:
                    print("- df:", df.columns, df.shape)
                    print("- df_new:", df_new.columns, df_new.shape)
                    print("- metadata old:") ; pprint(md_old)
                    print(" - release dates:") ; print(dates)
                    raise Exception('Why asking Yahoo for financials when nothing new ready?')
                    df = df_new
                else:
                    print("- df:", df.columns) ; print(df)
                    print("- df_new:", df_new.columns) ; print(df_new)
                    print("- df_pruned:") ; print(df_pruned)
                    raise Exception('need to implement merging 2x financials tables, review, particularly row names')
            yfcm.StoreCacheDatum(self.ticker, name, df, metadata=md)

        return df

    def _get_interval_from_table(self, tbl):
        debug = False
        # debug = True

        if debug:
            print(f"_get_interval_from_table()")

        dates = tbl.columns

        # Ensure only well-populated columns are retained, corresponding to 
        # report releases
        tbl = self._prune_yf_financial_df(tbl)
        tbl = tbl[tbl.columns.sort_values(ascending=False)]
        dates = tbl.columns
        if debug:
            print("- tbl:") ; print(tbl)
        if len(dates) <= 1:
            return TimedeltaEstimate(ComparableRelativedelta(months=6), yfcd.Confidence.Medium)

        interval = None
        intervals = [(dates[i-1] - dates[i]).days for i in range(1,len(dates))]
        if len(intervals) == 1:
            interval = intervals[0]
        else:
            avg = mean(intervals)
            sdm = stdev(intervals) / avg
            if sdm > 0.02:
                intervals = np.array(intervals)
                intervals = intervals[intervals<avg]
                if len(intervals) == 1:
                    interval = intervals[0]
                else:
                    avg = mean(intervals)
                    sdm = stdev(intervals) / avg
                    if sdm > 0.02:
                        print("- raw tbl periods:") ; print(self._get_fin_table(finType, yfcd.ReportingPeriod.Interim, refresh).columns)
                        print("- intervals =", intervals)
                        raise Exception("{0}: earnings interval inference failed - variance too high (sdm={1:.1f}%)".format(self.ticker, sdm*100))
            else:
                interval = int(avg)
        if debug:
            print("- interval:", interval)
        # tol = 30
        # tol = 40
        tol = 20
        confident = True
        if abs(interval-365) < tol:
            return ComparableRelativedelta(years=1)
        elif abs(interval-182) < tol:
            return ComparableRelativedelta(months=6)
        elif abs(interval-91) < tol:
            return ComparableRelativedelta(months=3)
        elif abs(interval-274) < tol:
            # 9 months, nonsense, but implies quarterly
            return TimedeltaEstimate(ComparableRelativedelta(months=3), yfcd.confidence.Medium)
        else:
            raise Exception(f"{self.ticker}: interval = {interval} doesn't fit standard intervals")

    def _get_interval(self, finType, refresh=True):
        debug = False
        # debug = True

        if debug:
            print(f"_get_interval({finType})")

        if not isinstance(finType, yfcd.Financials):
            raise Exception('Argument finType must be type Financials')

        tbl = self._get_fin_table(finType, yfcd.ReportingPeriod.Interim, refresh)

        return self._get_interval_from_table(tbl)

    def _get_release_dates(self, finType, period, refresh=True):
        debug = False
        # debug = True

        if debug:
            print(f"_get_release_dates({finType}, {period}, refresh={refresh})")

        if not isinstance(finType, yfcd.Financials):
            raise Exception('Argument finType must be type Financials')
        if not isinstance(period, yfcd.ReportingPeriod):
            raise Exception('Argument period must be type ReportingPeriod')

        tbl = self._get_fin_table(finType, period, refresh)
        if tbl is None or tbl.empty:
            return None

        tbl_cols = tbl.columns
        if isinstance(tbl_cols[0], (datetime, pd.Timestamp)):
            tbl_cols = [c.date() for c in tbl_cols]

        tbl = self._prune_yf_financial_df(tbl)
        tbl_cols = tbl.columns
        if isinstance(tbl_cols[0], (datetime, pd.Timestamp)):
            tbl_cols = [c.date() for c in tbl_cols]

        period_ends = [d.date() for d in tbl.columns if d.date() <= d_today]
        period_ends.sort(reverse=True)
        if debug:
            print("- period_ends:")
            for x in period_ends:
                print(x)

        cal_release_dates_tagged = self._get_tagged_calendar_dates(refresh)
        if debug:
            print("- cal_release_dates_tagged:")
            for x in cal_release_dates_tagged:
                print(x)

        release_dates_tagged = []
        limit = 16
        try:
            edf = self.get_earnings_dates(limit=limit, refresh=refresh, clean=False)
        except Exception as ex:
            if "No data found, symbol may be delisted" in str(ex):
                edf = None
            else:
                print("Ticker ", self.ticker)
                raise
        if edf is not None:
            while edf.index[-1] > tbl.columns.min().tz_localize(self.tzName):
                limit += 4
                edf_old = edf ; edf = self.get_earnings_dates(limit=limit, refresh=refresh, clean=False)
                if edf.equals(edf_old):
                    # Yahoo has no more data
                    break
        if edf is not None and edf.empty:
            raise Exception("Investigate why edf empty")
        if debug:
            print("- edf:")
            print(edf)

        if (edf is None) or (edf.shape[0]==0):
            if debug:
                print("- earnings_dates table is empty")
            release_dates = cal_release_dates_tagged
        else:
            # Prune old dates
            f_old = edf.index.date < period_ends[-1]
            if f_old.any():
                edf = edf[~f_old]
            if debug:
                print("- edf after prune-old:")
                print(edf)

            if edf.shape[0] > 1:
                # Drop dates that occurred just before another
                edf = edf.sort_index(ascending=True)
                d = edf.index.to_series().diff()
                d[0] = pd.Timedelta(999, unit='d')
                x_near = np.abs(d) < pd.Timedelta(5, "days")
                if x_near.any():
                    edf = edf[~x_near]
                # if debug:
                #     print("- edf after prune-near-dups:")
                #     print(edf)
                edf = edf.sort_index(ascending=False)

            if debug:
                print("- edf pruned:")
                print(edf)

            for i in range(edf.shape[0]):
                dt = edf.index[i].date()
                r = edf.iloc[i]

                td = None

                if td is None:
                    if pd.isnull(r["Reported EPS"]) and pd.isnull(r["Surprise(%)"]) and not r['Date confirmed?']:
                    # if pd.isnull(r["EPS Estimate"]) and pd.isnull(r["Reported EPS"]) and pd.isnull(r["Surprise(%)"]) and not r['Date confirmed?']:
                        # td = DateEstimate(dt, True)
                        td = DateEstimate(dt, yfcd.Confidence.Medium)
                    else:
                        td = dt

                if td not in release_dates_tagged:
                    # Protect again duplicate rows
                    release_dates_tagged.append(td)
            if debug:
                if len(release_dates_tagged) == 0:
                    print("- release_dates_tagged: EMPTY")
                else:
                    print("- release_dates_tagged:")
                    for e in release_dates_tagged:
                        print(e)
            release_dates = release_dates_tagged

            for x in cal_release_dates_tagged:
                duplicate = False
                for y in release_dates:
                    # if abs(x - y) < timedelta(days=10):
                    if abs(x - y) < timedelta(days=20):
                        duplicate = True
                        break
                if not duplicate:
                    release_dates.append(x)
            release_dates.sort(reverse=True)
        if len(release_dates) == 0:
            # raise Exception("Failed to generate any earnings release date for ", self.ticker)
            return None
        if debug:
            print("- release_dates:")
            for x in release_dates:
                print(x)

        # Now combine known dates into 'Earnings Releases':
        if debug:
            print("# Now combine known dates into 'Earnings Releases':")
        releases = []
        for d in period_ends:
            # r = EarningsRelease(d, False, None)
            r = EarningsRelease(d, None)
            releases.append(r)
        if debug:
            releases.sort()
            print("> releases with known period-end-dates:")
            for r in releases:
                print(r)

        if period == yfcd.ReportingPeriod.Full:
            interval, confident = 'ANNUAL', True
            interval_td = interval_str_to_days[interval]
        else:
            interval_td = self._get_interval(finType, refresh)
        if debug:
            print(f"- interval_td={interval_td}")

        if debug:
            print("- interval_td =", interval_td)
        interval_td_half = interval_td / 2

        # Fill gap between last release and now with estimated releases
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

            if debug:
                print(f"- loop {ct}")

            next_period_end = DateEstimate(interval_td + last_release.period_end, yfcd.Confidence.Medium)
            if debug:
                print("  - next_period_end:", next_period_end)
            # # days_since_last = next_period_end - last_report.period_end
            # days_since_last = next_period_end - last_release.period_end
            # if debug:
            #     print("  - days_since_last:", days_since_last)

            # try:
            #     report_expected = (days_since_last + timedelta(days=10)) > interval_td
            # except Exception:
            #     print("- interval_td =", interval_td, type(interval_td))
            #     print("- next_period_end =", next_period_end, type(next_period_end))
            #     print("- days_since_last =", days_since_last, type(days_since_last))
            #     raise
            # r = EarningsRelease(next_period_end, report_expected, None)
            r = EarningsRelease(next_period_end, None)

            releases.insert(0, r)
            last_release = r
            if debug:
                print("  - inserted:", r)

            try:
                if r.period_end > d_today:
                    break
            except Exception as e:
                if "Ambiguous" in str(e):
                    break
                else:
                    raise
        if debug:
            releases.sort()
            print("> releases with expected period-end-dates:")
            for r in releases:
                print(r)

        # Add more releases to ensure their date range fully overlaps with release dates
        release_dates.sort()
        releases.sort()
        ct = 0
        while releases[0].period_end > release_dates[0]:
            ct += 1
            if ct > 100:
                raise Exception("Infinite loop detected while adding release objects")
            prev_period_end = releases[0].period_end.date - interval_td
            prev_period_end = DateEstimate(prev_period_end, yfcd.Confidence.Medium)

            # # report_expected = abs((releases[0].period_end - prev_period_end) -report_interval) < timedelta(days=10)
            # # r = EarningsRelease(prev_period_end, report_expected, None)
            # r = EarningsRelease(prev_period_end, False, None)
            r = EarningsRelease(prev_period_end, None)

            releases.insert(0, r)
            if debug:
                print("Inserting: ", r)
        ct = 0
        # while releases[-1].period_end+earnings_interval < release_dates[-1]:
        while releases[-1].period_end+interval_td < release_dates[-1]:
            ct += 1
            if ct > 20:
                raise Exception("Infinite loop detected while adding release objects")
            next_period_end = interval_td + releases[-1].period_end.date
            next_period_end = DateEstimate(next_period_end, yfcd.Confidence.Medium)

            # # report_expected = abs((next_period_end-releases[-1].period_end) -report_interval) < timedelta(days=10)
            # # r = EarningsRelease(next_period_end, report_expected, None)
            # r = EarningsRelease(next_period_end, False, None)r = EarningsRelease(next_period_end, False, None)
            r = EarningsRelease(next_period_end, None)
            releases.append(r)
            if debug:
                print("Appending: ", r)
        if debug:
            releases.sort()
            print("> releases with additional releases to cover date range:")
            for r in releases:
                print(r)

        # Assign known dates to appropriate release(s) without dates
        if debug:
            print("# Assigning known dates to releases ...")
        releases.sort(reverse=True)
        release_dates.sort(reverse=True)
        # New method: find nearest date that doesn't exceed max delay:
        release_delay_min = timedelta(days=3)
        # release_delay_min = relativedelta(days=3)
        # release_delay_min = ComparableRelativedelta(days=3)
        # release_delay_max = interval_td + release_delay_min
        # release_delay_max = interval_td_half + release_delay_min
        year_end = self._get_fin_table(finType, yfcd.ReportingPeriod.Full, refresh).columns.max().date()
        interim_interval = self._earnings_interval(with_report=False, refresh=refresh)
        if debug:
            print("- year_end =", year_end)
            print("- interim_interval =", interim_interval)
        interim_release_delay_max = interim_interval
        annual_release_delay_max = interim_interval + timedelta(days=10)
        if debug:
            print(f"- interim release_delay range = {release_delay_min} -> {interim_release_delay_max}")
            print(f"- annual release_delay range = {release_delay_min} -> {annual_release_delay_max}")
        nearest_match = None  # tuple(i, j, delay)
        release_dates_assigned = np.full(len(release_dates), False)
        ctr = len(release_dates)*len(releases)
        while nearest_match is None:
            for i in range(len(releases)):
                r = releases[i]

                r_is_end_of_year = False
                rpe = r.period_end
                diff = (rpe - year_end)
                if diff < timedelta(days=-10):
                    diff *= -1
                if debug:
                    print("- diff =", diff)
                diff = diff % timedelta(days=365)
                if debug:
                    print("- diff modulus =", diff)
                if diff > timedelta(days=-15) and diff < timedelta(days=15):
                    # Aligns wit annual release date
                    r_is_end_of_year = True
                if debug:
                    print("- r_is_end_of_year =", r_is_end_of_year)
                if r_is_end_of_year:
                    release_delay_max = annual_release_delay_max
                else:
                    release_delay_max = interim_release_delay_max
                if debug:
                    print("- release_delay_max =", release_delay_max)

                if debug:
                    print(f"- releases[{i}] = {r}")
                if r.release_date is not None:
                    continue
                for j in range(len(release_dates)):
                    if release_dates_assigned[j]:
                        continue
                    if debug:
                        print("  - release_dates[j] =", release_dates[j])
                    dt = release_dates[j]
                    delay = dt - r.period_end
                    if debug:
                        print(f"    - delay={delay}")
                    if isinstance(delay, (relativedelta, ComparableRelativedelta)):
                        raise Exception(f'How is delay a {type(delay)}?')
                    if isinstance(delay, relativedelta):
                        delay = ComparableRelativedelta(days=delay.days)
                    if debug:
                        print(f"    - delay={delay}")
                    if delay <= timedelta(0):
                        continue
                    if delay >= release_delay_min and delay <= release_delay_max and \
                        ( (nearest_match is None) or (delay < nearest_match[2]) ):
                        if debug:
                            print(f"      - - matching i={i} with j={j}")
                        nearest_match = (i, j, delay)
                    elif nearest_match is not None and delay > nearest_match[2]:
                        # Dates are sorted, so a larger delay mean have already found closest
                        break
            if debug:
                print(f"nearest_match: {nearest_match}")
            if nearest_match is not None:
                i = nearest_match[0]
                j = nearest_match[1]
                releases[i].release_date = release_dates[j]
                release_dates_assigned[j] = True
                nearest_match = None
            else:
                break
            ctr -= 1
            if ctr <= 0:
                raise Exception("infinite loop detected")
        if debug:
            releases.sort()
            print("> releases with known release dates:")
            for r in releases:
                print(r)
        # For any releases still without release dates, estimate with the following heuristics:
        # 1 - if release 12 months before/after has a date (or a multiple of 12), use that +/- 12 months
        # 2 - else used previous release + interval
        report_delay = None
        releases.sort()
        if any([r.release_date is None for r in releases]):
            for interval in [365, 365//2, 365//4]:
                interval_td = timedelta(days=interval)
                for i in range(len(releases)):
                    if releases[i].release_date is None:
                        # Need to find a similar release to extrapolate date from
                        date_set = False

                        for i2 in range(len(releases)):
                            if i2==i:
                                continue
                            if releases[i2].release_date is not None:
                                # # rem = abs(releases[i].period_end - releases[i2].period_end).days % interval
                                # rem = (releases[i].period_end - releases[i2].period_end) % interval_td
                                if releases[i2].period_end > releases[i].period_end:
                                    rem = (releases[i2].period_end - releases[i].period_end) % interval_td
                                else:
                                    rem = (releases[i].period_end - releases[i2].period_end) % interval_td
                                try:
                                    match = (rem < timedelta(days=10)) or abs(rem-interval_td) < timedelta(days=10)
                                except Exception as e:
                                    if "Ambiguous" in str(e):
                                        match = True
                                    else:
                                        raise
                                if match:
                                    if debug:
                                        print(f"- matching {releases[i]} with {releases[i2]} for interval {interval}")
                                    delay = releases[i2].release_date - releases[i2].period_end
                                    dt = delay + releases[i].period_end
                                    if isinstance(dt, date):
                                        dt = DateEstimate(dt, yfcd.Confidence.Medium)
                                    else:
                                        confidences = [yfcd.Confidence.Medium]
                                        if isinstance(releases[i2].period_end, (DateEstimate, DateRangeEstimate)):
                                            confidences.append(releases[i2].period_end.confidence)
                                        if isinstance(releases[i2].release_date, (DateEstimate, DateRangeEstimate)):
                                            confidences.append(releases[i2].release_date.confidence)
                                        dt.confidence = min(confidences)
                                    if debug:
                                        print("- dt =", dt)

                                    if i > 0 and (releases[i-1].release_date is not None):
                                        too_close_to_previous = False
                                        if isinstance(releases[i-1].release_date, DateEstimate):
                                            too_close_to_previous = releases[i-1].release_date.isclose(dt)
                                        else:
                                            too_close_to_previous = (dt-releases[i-1].release_date) < timedelta(days=30)
                                        if too_close_to_previous:
                                            if debug:
                                                print("- would be too close to previous release date")
                                            # Too close to last release date
                                            continue
                                    releases[i].release_date = dt
                                    date_set = True
                                    if debug:
                                        print("- estimated release date {} of period-end {} from period-end {}".format(releases[i].release_date, releases[i].period_end, releases[i2].period_end))
                                    break



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
            # raise Exception(f"Unable to map all {period} financials to release dates")
            return None

        # Check for any releases still missing a release date that could be the Last earnings release:
        if any([r.release_date is None for r in releases]):
            for i in range(len(releases)):
                r = releases[i]
                # print("Analysing release: {0}".format(r))
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
        if debug:
            print("> releases after estimating release dates:")
            for r in releases:
                print(r)

        period_ends = []
        period_ends_est = []
        release_dates = []
        release_dates_est = []
        delays = []
        for r in releases:
            rpe = r.period_end ; rrd = r.release_date
            period_ends.append(rpe if isinstance(rpe, date) else rpe.date)
            period_ends_est.append(rpe.confidence if isinstance(rpe, DateEstimate) else yfcd.Confidence.High)
            release_dates.append(rrd if isinstance(rrd, date) else rrd.date)
            release_dates_est.append(rrd.confidence if isinstance(rrd, DateEstimate) else yfcd.Confidence.High)
            dt1 = rpe if isinstance(rpe, date) else rpe.date
            dt2 = rrd if isinstance(rrd, date) else rrd.date
            delays.append(dt2 - dt1)
        df = pd.DataFrame({'Period end':period_ends, 'PE confidence':period_ends_est, 'Release date':release_dates, 'RD confidence':release_dates_est, 'Delay':delays})
        df = df.set_index('Period end')

        # Final sanity tests
        if (df['Delay'] < pd.Timedelta(0)).any():
            print(df)
            raise Exception('Release dates contains negative delays')
        std_pct_mean = df['Delay'].std() / df['Delay'].mean()
        if std_pct_mean > 0.5:
            print(df)
            raise Exception(f'Release date delays have high variance ({std_pct_mean*100:.1}%)')
        if ((df['Release date'].iloc[:-1] - df.index[1:]) > pd.Timedelta(0)).any():
            print(df)
            raise Exception('Some releases dates are after next period ends')
        if ((df['Release date'].iloc[:-1] - df['Release date'].iloc[1:]) > pd.Timedelta(0)).any():
            print(df)
            raise Exception('Some releases dates are after next release date')

        return df

    def _prune_yf_financial_df(self, df):
        # Rarely table contain duplicate data at different timepoints.
        # - instances: OPG.L, ROG.SW
        # Possibly YF is at fault. It may be backfilling 6-monthly data to populate 3-monthly table
        # Keep the latest of each duplicate

        if df is None or df.empty:
            return df

        # dates = df.columns
        ## Fiddly to put dates into a list and sort without reordering dataframe and without down-casting the date types!
        dates = [d for d in df.columns]
        dates.sort()

        # Drop duplicated columns
        if len(set(dates)) != len(dates):
            ## Search for duplicated columns
            df = df.T.drop_duplicates().T
            dates = [d for d in df.columns]
            dates.sort()


        # Drop mostly-NaN duplicated dates:
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
                    # if is_mostly_nans.sum() == 1:
                    if is_mostly_nans.sum() == n_dups-1:
                        ## All but one column are mostly nans, perfect!
                        drop_indices = dt_indices[is_mostly_nans]
                        indices = np.array(range(df.shape[1]))
                        keep_indices = indices[~np.isin(indices, drop_indices)]
                        df = df.iloc[:,keep_indices].copy()

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

            dates = [d for d in df.columns]
            dates.sort()


        # If duplicated date columns is very similar, then drop right-most:
        if len(set(dates)) != len(dates):
            for dt in set(dates):
                dff = df[dt]
                if len(dff.shape) == 2 and dff.shape[1] == 2:
                    dff.columns = [str(dff.columns[i])+str(i) for i in range(dff.shape[1])]
                    # r = dff.diff(axis=1)
                    r = (dff[dff.columns[0]] - dff[dff.columns[1]]).abs() / dff[dff.columns[0]]
                    # r = r.mean()
                    r = r.sum()
                    # print(r)
                    # print(f"- r = {r}")
                    # print(dff[dff.columns[0]])
                    # raise Exception("here")
                    if r < 0.15:
                        # num_na1 = 
                        df = df.drop(dt, axis=1)
                        df[dt] = dff[dff.columns[0]]
            dates = [d for d in df.columns]
            dates.sort()


        if len(set(dates)) != len(dates):
            print(df)
            print("Dates: {}".format(dates))
            raise Exception("Duplicate dates found in financial df")

        # Search for mostly-nan columns, where the non-nan values are exact match to an adjacent column.
        # Replace those nans with adjacent column values.
        for i1 in range(1, len(dates)):
            d1 = dates[i1]
            d0 = dates[i1-1]
            d0_mostly_nans = df[d0].isnull().sum() > int(df.shape[0]*0.75)
            d1_mostly_nans = df[d1].isnull().sum() > int(df.shape[0]*0.75)
            if d0_mostly_nans and not d1_mostly_nans:
                # f = np_and(np_not(df[d0].isnull()), np_not(df[d1].isnull()))
                f = (~df[d0].isnull()) & (~df[d1].isnull())
                if sum(f) >= 2:
                    # At least two actual values
                    if np.array_equal(df.loc[f,d0], df.loc[f,d1]):
                        # and those values match
                        df[d0] = df[d1].copy()
            elif d1_mostly_nans and not d0_mostly_nans:
                # f = np_and(np_not(df[d1].isnull()), np_not(df[d0].isnull()))
                f = (~df[d1].isnull()) & (~df[d0].isnull())
                if sum(f) >= 2:
                    # At least two actual values
                    if np.array_equal(df.loc[f,d1], df.loc[f,d0]):
                        # and those values match
                        df[d1] = df[d0].copy()

        # Drop mostly-nan columns:
        for i in range(len(dates)-1, -1, -1):
            d = dates[i]
            # if df[d].isnull().sum() == df.shape[0]:
            #   # Full of nans, drop column:
            if df[d].isnull().sum() > int(df.shape[0]*0.75):
                # Mostly nans, drop column
                df = df.drop(d, axis=1)
        # # Then drop all columns devoid of data (NaN and 0.0):
        # for i in range(len(dates)-1, -1, -1):
        #   d = dates[i]
        #   fnan = df[d].isnull()
        #   fzero = df[d]==0.0
        #   if sum(np_or(fnan, fzero)) == df.shape[0]:
        #       # Completely devoid of data, drop column
        #       df = df.drop(d, axis=1)

        dates = [d for d in df.columns]
        dates.sort()

        if len(set(dates)) != len(dates):
            print(f"Dates: {dates}")
            raise Exception("Duplicate dates found in financial df")

        # Remove columns which YF created by backfilling, e.g. with AI.PA and KOD.L
        df = df[df.columns.sort_values(ascending=False)]
        dates = [d for d in df.columns]
        for i1 in range(1, len(dates)):
            d0 = dates[i1-1]
            d1 = dates[i1]
            df.loc[df[d0].isna(), d0] = 0.0
            df.loc[df[d1].isna(), d1] = 0.0
            if np.array_equal(df[d0].values, df[d1].values):
                df = df.drop(d0, axis=1)
        df = df[df.columns.sort_values(ascending=True)]

        if df.empty:
            raise Exception("_prune_yf_financial_df() has removed all columns")
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
        inference_accurate_enough = False

        if not with_report:
            edf = self.get_earnings_dates(refresh=False)
            if (edf is None or edf.shape[0] <= 3) and refresh:
                edf = self.get_earnings_dates(refresh=refresh)
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
                    deltas_pruned = deltas[z_scores < 1.5]
                    # Discard small deltas
                    deltas_pruned = deltas_pruned[deltas_pruned > 10.0]

                    std_pct_mean = np.std(deltas) / np.mean(deltas)
                    interval_days = np.mean(deltas_pruned)
                if debug:
                    print("- interval_days:", interval_days)
                if std_pct_mean < 0.68:
                    # tol = 30
                    # tol = 40
                    tol = 20
                    if abs(interval_days-365) < tol:
                        interval = 'ANNUAL'
                    elif abs(interval_days-182) < tol:
                        interval = 'HALF'
                    elif abs(interval_days-91) < tol:
                        interval = 'QUART'
                    else:
                        print("- edf:") ; print(edf)
                        print("- deltas:") ; print(deltas)
                        print("- z_scores:") ; print(z_scores)
                        print("- deltas_pruned:") ; print(deltas_pruned)
                        print("- std_pct_mean:", std_pct_mean)
                        raise Exception(f"{self.ticker}: interval_days = {interval_days} doesn't fit standard intervals")
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
                inference_accurate_enough = False
                if tbl_bs is not None and not tbl_bs.empty:
                    tbl = tbl_bs
                elif tbl_fi is not None and not tbl_fi.empty:
                    tbl = tbl_fi
                else:
                    tbl = tbl_cf
            else:
                inference_accurate_enough = True
                tbl = tbl_bs
        else:
            # Use whichever is available with most columns
            inference_accurate_enough = True
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
            interval = TimedeltaEstimate(interval_str_to_days['HALF'], yfcd.Confidence.Medium)

        return interval

    def get_earnings_dates(self, limit=12, refresh=True, clean=True):
        yfcu.TypeCheckInt(limit, 'limit')
        yfcu.TypeCheckBool(refresh, 'refresh')
        yfcu.TypeCheckBool(clean, 'clean')

        debug = False
        # debug = True

        if debug:
            print(f"get_earnings_dates(limit={limit}, refresh={refresh})")

        if self._earnings_dates is None:
            if yfcm.IsDatumCached(self.ticker, "earnings_dates"):
                self._earnings_dates = yfcm.ReadCacheDatum(self.ticker, "earnings_dates")
                self._earnings_dates = self._earnings_dates.drop_duplicates()
            elif refresh:
                self._earnings_dates = self._fetch_earnings_dates(limit, refresh)
                yfcm.StoreCacheDatum(self.ticker, "earnings_dates", self._earnings_dates)
                return self._earnings_dates

        df_modified = False
        if self._earnings_dates is not None:
            # Ensure column 'Date confirmed?' is present, and update with calendar
            if 'Date confirmed?' not in self._earnings_dates.columns:
                self._earnings_dates['Date confirmed?'] = False
                df_modified = True
            cal = self.get_calendar(refresh)
            if cal is not None and len(cal['Earnings Date']) == 1:
                x = cal['Earnings Date'][0]
                for dt in self._earnings_dates.index:
                    if abs(dt.date() - x) < timedelta(days=7):
                        # Assume same release
                        if not self._earnings_dates['Date confirmed?'].loc[dt]:
                            self._earnings_dates.loc[dt, 'Date confirmed?'] = True
                            df_modified = True
                            break

        if not refresh and self._earnings_dates is None:
            return None

        dt_now = pd.Timestamp.utcnow().tz_convert(self.tzName)
        max_age = pd.Timedelta('7d')
        df = self._earnings_dates.copy()
        if debug:
            print(f'- cached earnings_dates has {df.shape[0]} rows')

        last_fetch = self._earnings_dates['FetchDate'].max()
        if debug:
            print("- last_fetch =", last_fetch)
        if (dt_now - last_fetch) < pd.Timedelta('7d'):
            refresh = False

        if refresh:
            # f_na = df['Reported EPS'].isna()
            f_na = df['Reported EPS'].isna().to_numpy()
            f_nna = ~f_na
            # df['Expired?'] = f_na & (df.index < dt_now) & ((dt_now - df['FetchDate']) > max_age)
            # f_expired = df['Expired?'].to_numpy()
            # f_expired = f_na & (df.index < dt_now) & ((dt_now - df['FetchDate']) > max_age)
            f_expired = f_na & (df.index < dt_now) & ((dt_now - df['FetchDate']) > max_age).to_numpy()
            # f_final = (df['FetchDate'] - pd.Timedelta(days=30)) > df.index
            n = df.shape[0]
            if debug:
                print("- n =", n)

            ei = self._earnings_interval(with_report=False, refresh=False)
            if isinstance(ei, TimedeltaEstimate):
                # Don't care about confidence
                ei = ei.td
            elif isinstance(ei, TimedeltaRangeEstimate):
                ei = mean([ei.td1, ei.td2])
            if isinstance(ei, (ComparableRelativedelta, relativedelta)):
                # Convert to normal Timedelta, don't need 100% precision
                if ei.months == 3:
                    ei = pd.Timedelta('91d')
                elif ei.months == 6:
                    ei = pd.Timedelta('180d')
                else:
                    raise Exception(ei)

            if debug:
                print("- cached df:") ; print(df)

            lookahead_dt = dt_now + pd.Timedelta('365d')
            if debug:
                print("- lookahead_dt =", lookahead_dt)

            # n_intervals_missing_after = int(round((lookahead_dt - df.index[0]) / ei))
            n_intervals_missing_after = int(round(Decimal((lookahead_dt - df.index[0]) / ei)))
            any_expired = f_expired.any()
            if not any_expired:
                n_intervals_to_refresh_expired = 0
                n_before_expired = n
            else:
                earliest_expired_dt = df.index[f_expired][-1]
                if debug:
                    print("- earliest_expired_dt =", earliest_expired_dt)
                f_certain = f_nna & (~f_expired)
                # f_certain = f_certain | df['Date confirmed?'].to_numpy()
                # latest_certain_dt_idx = np.where(f_certain)[0]
                certain_idxs = np.where(f_certain)[0]
                if len(certain_idxs) == 0:
                    n_intervals_to_refresh_expired = (lookahead_dt - earliest_expired_dt) / ei +1.0
                    n_before_expired = 0
                else:
                    latest_certain_dt_idx = np.where(f_certain)[0][0]
                    n_before_expired = n - latest_certain_dt_idx - 1
                    if debug:
                        print("- latest_certain_dt_idx =", latest_certain_dt_idx)
                    latest_certain_dt = df.index[latest_certain_dt_idx]
                    if debug:
                        print("- latest_certain_dt =", latest_certain_dt)
                    n_intervals_to_refresh_expired = (lookahead_dt - latest_certain_dt) / ei
                n_intervals_to_refresh_expired *= 1.2  # allow for Yahoo randomly duplicating releases
                if debug:
                    print("- n_intervals_to_refresh_expired =", n_intervals_to_refresh_expired)
                n_intervals_to_refresh_expired = int(round(n_intervals_to_refresh_expired+0.95))
                if debug:
                    print("- n_intervals_to_refresh_expired =", n_intervals_to_refresh_expired)
            n_intervals_to_fetch = n_intervals_missing_after + n_intervals_to_refresh_expired
            if debug:
                print("- n_intervals_to_fetch =", n_intervals_to_fetch)
            if debug:
                print("- n_before_expired =", n_before_expired)

            # latest_known_dt = df.index[f_nna][0]
            latest_known_dt = df.index[~f_expired][0]
            if debug:
                print("- latest_known_dt =", latest_known_dt)
            latest_known_dt_idx = df.index.get_loc(latest_known_dt)
            if debug:
                print("- latest_known_dt_idx =", latest_known_dt_idx)
            total_refetch = False
            start_date = yfcm.ReadCacheMetadata(self.ticker, 'earnings_dates', 'start_date')
            if any_expired:
                if limit > (n_intervals_to_refresh_expired + n_before_expired):
                    if start_date is None or start_date < self._earnings_dates.index[-1]:
                        total_refetch = True
            elif limit > (n_intervals_missing_after + n):
                if start_date is None or start_date < self._earnings_dates.index[-1]:
                    total_refetch = True
            if debug:
                print("- total_refetch =", total_refetch)
            if total_refetch:
                # Just do a total refetch and replace
                if debug:
                    print("DEBUG: Total refetch")
                self._earnings_dates = self._fetch_earnings_dates(limit, refresh)
                df_modified = True
                if self._earnings_dates.shape[0] < limit:
                    yfcm.WriteCacheMetadata(self.ticker, 'earnings_dates', 'start_date', self._earnings_dates.index.min())
            elif n_intervals_to_refresh_expired > 0:
                # Just update cached table, then return requested subset
                if debug:
                    print("DEBUG: Smart fetch of expired data")
                new_df = self._fetch_earnings_dates(n_intervals_to_refresh_expired, refresh)
                if debug:
                    print("- new_df:") ; print(new_df)
                if new_df.shape[0] < n_intervals_to_refresh_expired:
                    yfcm.WriteCacheMetadata(self.ticker, 'earnings_dates', 'start_date', new_df.index.min())
                df2 = self._earnings_dates[self._earnings_dates.index < new_df.index[-1]]
                df3 = pd.concat([new_df, df2])
                if debug:
                    print("- df3:") ; print(df3)
                self._earnings_dates = df3
                df_modified = True

        if df_modified:
            yfcm.StoreCacheDatum(self.ticker, "earnings_dates", self._earnings_dates)

        # if limit > self._earnings_dates.shape[0]:
        #     raise Exception(f"After updating earnings_dates, don't have enough rows! nrows={self._earnings_dates.shape[0]}, limit={limit}")
        #     # No more history to fetch.

        if limit < self._earnings_dates.shape[0]:
            df = self._earnings_dates.iloc[:limit].copy()
        else:
            df = self._earnings_dates.copy()
        if clean:
            df = df.drop(["FetchDate", "Date confirmed?"], axis=1)
        return df

    def _fetch_earnings_dates(self, limit, refresh=True):
        debug = False
        # debug = True

        if debug:
            print(f"_fetch_earnings_dates(limit={limit}, refresh={refresh})")

        yfcu.TypeCheckInt(limit, "limit")
        yfcu.TypeCheckBool(refresh, "refresh")
        
        df = self.dat.get_earnings_dates(limit)
        if df is None:
            if debug:
                print("- Yahoo returned None")
            return df
        if debug:
            print(f"- Yahoo returned {df.shape[0]} rows")
        if df.empty:
            return None
        df['FetchDate'] = pd.Timestamp.utcnow().tz_convert(self.tzName)

        cal = self.get_calendar(refresh)
        df['Date confirmed?'] = False
        if cal is not None and len(cal['Earnings Date']) == 1:
            x = cal['Earnings Date'][0]
            for dt in df.index:
                if abs(dt.date() - x) < timedelta(days=7):
                    # Assume same release
                    df.loc[dt, 'Date confirmed?'] = True
                    break

        df = df.drop_duplicates()

        return df

    def get_calendar(self, refresh=True):
        yfcu.TypeCheckBool(refresh, 'refresh')

        max_age = pd.Timedelta(yfcm._option_manager.max_ages.calendar)

        if self._calendar is None:
            if yfcm.IsDatumCached(self.ticker, "calendar"):
                self._calendar = yfcm.ReadCacheDatum(self.ticker, "calendar")
                if "FetchDate" not in self._calendar.keys():
                    fp = yfcm.GetFilepath(self.ticker, "calendar")
                    mod_dt = datetime.datetime.fromtimestamp(os.path.getmtime(fp))
                    self._calendar["FetchDate"] = mod_dt

        if (self._calendar is not None) and (self._calendar["FetchDate"] + max_age) > pd.Timestamp.now():
            return self._calendar

        if not refresh:
            return self._calendar

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

        yfcm.StoreCacheDatum(self.ticker, "calendar", c)
        self._calendar = c
        return self._calendar

    def _get_tagged_calendar_dates(self, refresh=True):
        yfcu.TypeCheckBool(refresh, 'refresh')

        debug = False
        # debug = True

        cal = self.get_calendar(refresh)
        if cal is not None and len(cal) == 0:
            cal = None
        cal_release_dates = None if cal is None else cal["Earnings Date"]

        # Tag earnings dates in calendar with whether was estimated:
        if cal_release_dates is None:
            cal_release_dates_tagged = []
        else:
            # cal_release_dates = [d.date() for d in cal_release_dates]  # update: already date
            cal_release_dates_tagged = []
            cal_release_dates.sort()
            last = None
            for d in cal_release_dates:
                if last is None:
                    last = d
                else:
                    diff = d - last
                    if diff <= timedelta(days=15):
                        # Looks like a date range so tag last-added date as estimate. And change data to be middle of range
                        last = DateRange(last, d)
                        cal_release_dates_tagged.append(last)
                        last = None
                    else:
                        print("- cal_release_dates:") ; print(cal_release_dates)
                        print("- diff =", diff)
                        raise Exception(f"Implement/rejig this execution path (tkr={self.ticker})")
            if last is not None:
                cal_release_dates_tagged.append(last)
        if debug:
            if len(cal_release_dates_tagged) == 0:
                print("- cal_release_dates_tagged: EMPTY")
            else:
                print("- cal_release_dates_tagged:")
                for e in cal_release_dates_tagged:
                    print(e)

        return cal_release_dates_tagged
