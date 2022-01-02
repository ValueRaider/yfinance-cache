import yfinance as yf

# from .yfc_cache_manager import *
from yfc_cache_manager import *

# from .yfc_utils import *
from yfc_utils import *

class Ticker:
	def __init__(self, ticker):
		self.ticker = ticker.upper()

		self._history = None

		self._info = None

		self._splits = None

		self._financials = None
		self._quarterly_financials = None

		self._major_holders = None

		self._institutional_holders = None

		self._balance_sheet = None
		self._quarterly_balance_sheet = None

		self._cashflow = None
		self._quarterly_cashflow = None

		self._earnings = None
		self._quarterly_earnings = None

		self._sustainability = None

		self._recommendations = None

		self._calendar = None

		self._isin = None

		self._options = None

		self._news = None

	def history(self, period=Period.Months1, interval=Interval.Days1,
				start=None, end=None, prepost=False, actions=True,
				auto_adjust=True, back_adjust=False,
				proxy=None, rounding=False, tz=None, **kwargs):

		# 'prepost' not doing anything in yfinance

		if self._history is None:
			dat = yf.Ticker(self.ticker)

			# Intercept these arguments:
			# - actions - always store 'dividends' and 'stock splits', drop columns on retrieval
			# - auto_adjust - call yfinance auto_adjust() on DataFrame
			# - back_adjust - call yfinance back_adjust() on DataFrame
			# - rounding - round on retrieval, using _np.round()
			# - tz - call Pandas tz_localize() on DataFrame

			## Left with one data dimension: interval

			h = dat.history(period=periodToString[period], 
							interval=intervalToString[interval],
							start=start, end=end, prepost=prepost, actions=actions,
							auto_adjust=auto_adjust, back_adjust=back_adjust,
							proxy=proxy, rounding=rounding, tz=tz, kwargs=kwargs)

			self._history = h

		return self._history

	@property
	def info(self):
		if not self._info is None:
			return self._info

		if IsDatumCached(self.ticker, "info"):
			self._info = ReadCacheDatum(self.ticker, "info")
			return self._info

		dat = yf.Ticker(self.ticker)
		self._info = dat.info
		StoreCacheDatum(self.ticker, "info", self._info)
		return self._info

	@property
	def splits(self):
		if not self._splits is None:
			return self._splits

		if IsDatumCached(self.ticker, "splits"):
			self._splits = ReadCacheDatum(self.ticker, "splits")
			return self._splits

		dat = yf.Ticker(self.ticker)
		self._splits = dat.splits
		StoreCacheDatum(self.ticker, "splits", self._splits)
		return self._splits

	@property
	def financials(self):
		if not self._financials is None:
			return self._financials

		if IsDatumCached(self.ticker, "financials"):
			self._financials = ReadCacheDatum(self.ticker, "financials")
			return self._financials

		dat = yf.Ticker(self.ticker)
		self._financials = dat.financials
		StoreCacheDatum(self.ticker, "financials", self._financials)
		return self._financials

	@property
	def quarterly_financials(self):
		if not self._quarterly_financials is None:
			return self._quarterly_financials

		if IsDatumCached(self.ticker, "quarterly_financials"):
			self._quarterly_financials = ReadCacheDatum(self.ticker, "quarterly_financials")
			return self._quarterly_financials

		dat = yf.Ticker(self.ticker)
		self._quarterly_financials = dat.quarterly_financials
		StoreCacheDatum(self.ticker, "quarterly_financials", self._quarterly_financials)
		return self._quarterly_financials

	@property
	def major_holders(self):
		if not self._major_holders is None:
			return self._major_holders

		if IsDatumCached(self.ticker, "major_holders"):
			self._major_holders = ReadCacheDatum(self.ticker, "major_holders")
			return self._major_holders

		dat = yf.Ticker(self.ticker)
		self._major_holders = dat.major_holders
		StoreCacheDatum(self.ticker, "major_holders", self._major_holders)
		return self._major_holders

	@property
	def institutional_holders(self):
		if not self._institutional_holders is None:
			return self._institutional_holders

		if IsDatumCached(self.ticker, "institutional_holders"):
			self._institutional_holders = ReadCacheDatum(self.ticker, "institutional_holders")
			return self._institutional_holders

		dat = yf.Ticker(self.ticker)
		self._institutional_holders = dat.institutional_holders
		StoreCacheDatum(self.ticker, "institutional_holders", self._institutional_holders)
		return self._institutional_holders

	@property
	def balance_sheet(self):
		if not self._balance_sheet is None:
			return self._balance_sheet

		if IsDatumCached(self.ticker, "balance_sheet"):
			self._balance_sheet = ReadCacheDatum(self.ticker, "balance_sheet")
			return self._balance_sheet

		dat = yf.Ticker(self.ticker)
		self._balance_sheet = dat.balance_sheet
		StoreCacheDatum(self.ticker, "balance_sheet", self._balance_sheet)
		return self._balance_sheet

	@property
	def quarterly_balance_sheet(self):
		if not self._quarterly_balance_sheet is None:
			return self._quarterly_balance_sheet

		if IsDatumCached(self.ticker, "quarterly_balance_sheet"):
			self._quarterly_balance_sheet = ReadCacheDatum(self.ticker, "quarterly_balance_sheet")
			return self._quarterly_balance_sheet

		dat = yf.Ticker(self.ticker)
		self._quarterly_balance_sheet = dat.quarterly_balance_sheet
		StoreCacheDatum(self.ticker, "quarterly_balance_sheet", self._quarterly_balance_sheet)
		return self._quarterly_balance_sheet

	@property
	def cashflow(self):
		if not self._cashflow is None:
			return self._cashflow

		if IsDatumCached(self.ticker, "cashflow"):
			self._cashflow = ReadCacheDatum(self.ticker, "cashflow")
			return self._cashflow

		dat = yf.Ticker(self.ticker)
		self._cashflow = dat.cashflow
		StoreCacheDatum(self.ticker, "cashflow", self._cashflow)
		return self._cashflow

	@property
	def quarterly_cashflow(self):
		if not self._quarterly_cashflow is None:
			return self._quarterly_cashflow

		if IsDatumCached(self.ticker, "quarterly_cashflow"):
			self._quarterly_cashflow = ReadCacheDatum(self.ticker, "quarterly_cashflow")
			return self._quarterly_cashflow

		dat = yf.Ticker(self.ticker)
		self._quarterly_cashflow = dat.quarterly_cashflow
		StoreCacheDatum(self.ticker, "quarterly_cashflow", self._quarterly_cashflow)
		return self._quarterly_cashflow

	@property
	def earnings(self):
		if not self._earnings is None:
			return self._earnings

		if IsDatumCached(self.ticker, "earnings"):
			self._earnings = ReadCacheDatum(self.ticker, "earnings")
			return self._earnings

		dat = yf.Ticker(self.ticker)
		self._earnings = dat.earnings
		StoreCacheDatum(self.ticker, "earnings", self._earnings)
		return self._earnings

	@property
	def quarterly_earnings(self):
		if not self._quarterly_earnings is None:
			return self._quarterly_earnings

		if IsDatumCached(self.ticker, "quarterly_earnings"):
			self._quarterly_earnings = ReadCacheDatum(self.ticker, "quarterly_earnings")
			return self._quarterly_earnings

		dat = yf.Ticker(self.ticker)
		self._quarterly_earnings = dat.quarterly_earnings
		StoreCacheDatum(self.ticker, "quarterly_earnings", self._quarterly_earnings)
		return self._quarterly_earnings

	@property
	def sustainability(self):
		if not self._sustainability is None:
			return self._sustainability

		if IsDatumCached(self.ticker, "sustainability"):
			self._sustainability = ReadCacheDatum(self.ticker, "sustainability")
			return self._sustainability

		dat = yf.Ticker(self.ticker)
		self._sustainability = dat.sustainability
		StoreCacheDatum(self.ticker, "sustainability", self._sustainability)
		return self._sustainability

	@property
	def recommendations(self):
		if not self._recommendations is None:
			return self._recommendations

		if IsDatumCached(self.ticker, "recommendations"):
			self._recommendations = ReadCacheDatum(self.ticker, "recommendations")
			return self._recommendations

		dat = yf.Ticker(self.ticker)
		self._recommendations = dat.recommendations
		StoreCacheDatum(self.ticker, "recommendations", self._recommendations)
		return self._recommendations

	@property
	def calendar(self):
		if not self._calendar is None:
			return self._calendar

		if IsDatumCached(self.ticker, "calendar"):
			self._calendar = ReadCacheDatum(self.ticker, "calendar")
			return self._calendar

		dat = yf.Ticker(self.ticker)
		self._calendar = dat.calendar
		StoreCacheDatum(self.ticker, "calendar", self._calendar)
		return self._calendar

	@property
	def inin(self):
		if not self._inin is None:
			return self._inin

		if IsDatumCached(self.ticker, "inin"):
			self._inin = ReadCacheDatum(self.ticker, "inin")
			return self._inin

		dat = yf.Ticker(self.ticker)
		self._inin = dat.inin
		StoreCacheDatum(self.ticker, "inin", self._inin)
		return self._inin

	@property
	def options(self):
		if not self._options is None:
			return self._options

		if IsDatumCached(self.ticker, "options"):
			self._options = ReadCacheDatum(self.ticker, "options")
			return self._options

		dat = yf.Ticker(self.ticker)
		self._options = dat.options
		StoreCacheDatum(self.ticker, "options", self._options)
		return self._options

	@property
	def news(self):
		if not self._news is None:
			return self._news

		if IsDatumCached(self.ticker, "news"):
			self._news = ReadCacheDatum(self.ticker, "news")
			return self._news

		dat = yf.Ticker(self.ticker)
		self._news = dat.news
		StoreCacheDatum(self.ticker, "news", self._news)
		return self._news
