"""
SEL(stock selection part)
Based on the 'Momentum Strategy with Market Cap and EV/EBITDA' strategy introduced by Jing Wu, 6 Feb 2018
adapted and recoded by Jack Simonson, Goldie Yalamanchi, Vladimir, Peter Guenther, and Leandro Maia
https://www.quantconnect.com/forum/discussion/3377/momentum-strategy-with-market-cap-and-ev-ebitda/p1
https://www.quantconnect.com/forum/discussion/9678/quality-companies-in-an-uptrend/p1
https://www.quantconnect.com/forum/discussion/9632/amazing-returns-superior-stock-selection-strategy-superior-in-amp-out-strategy/p1

I/O(in & out part)
The Distilled Bear in & out algo
based on Dan Whitnable's 22 Oct 2020 algo on Quantopian. 
Dan's original notes: 
"This is based on Peter Guenther great “In & Out” algo.
Included Tentor Testivis recommendation to use volatility adaptive calculation of WAIT_DAYS and RET.
Included Vladimir's ideas to eliminate fixed constants
Help from Thomas Chang"

https://www.quantopian.com/posts/new-strategy-in-and-out
https://www.quantconnect.com/forum/discussion/9597/the-in-amp-out-strategy-continued-from-quantopian/
"""

from QuantConnect.Data.UniverseSelection import *
import math
import numpy as np
import pandas as pd
import scipy as sp

class EarningsFactorWithMomentum_InOut(QCAlgorithm):

    def Initialize(self):

        self.SetStartDate(2008, 1, 1)  #Set Start Date
        #self.SetEndDate(2009, 12, 31)  #Set End Date
        self.cap = 100000
        self.SetCash(self.cap)
        
        res = Resolution.Minute
        
        # Holdings
        ### 'Out' holdings and weights
        self.BND1 = self.AddEquity('TLT', res).Symbol #TLT; TMF for 3xlev
        self.HLD_OUT = {self.BND1: 1}
        ### 'In' holdings and weights (static stock selection strategy)
        ##### These are determined flexibly via sorting on fundamentals
        
        ##### In & Out parameters #####
        # Feed-in constants
        self.INI_WAIT_DAYS = 15  # out for 3 trading weeks
        
        # Market and list of signals based on ETFs
        self.MRKT = self.AddEquity('SPY', res).Symbol  # market
        self.GOLD = self.AddEquity('GLD', res).Symbol  # gold
        self.SLVA = self.AddEquity('SLV', res).Symbol  # vs silver
        self.UTIL = self.AddEquity('XLU', res).Symbol  # utilities
        self.INDU = self.AddEquity('XLI', res).Symbol  # vs industrials
        self.METL = self.AddEquity('DBB', res).Symbol  # input prices (metals)
        self.USDX = self.AddEquity('UUP', res).Symbol  # safe haven (USD)
        
        self.FORPAIRS = [self.GOLD, self.SLVA, self.UTIL, self.INDU, self.METL, self.USDX]

        # Specific variables
        self.DISTILLED_BEAR = 999
        self.BE_IN = 999
        self.BE_IN_PRIOR = 999
        self.VOLA_LOOKBACK = 126
        self.WAITD_CONSTANT = 85
        self.DCOUNT = 0 # count of total days since start
        self.OUTDAY = 0 # dcount when self.be_in=0
        
        # set a warm-up period to initialize the indicator
        self.SetWarmUp(timedelta(350))
        
        ##### Momentum & fundamentals strategy parameters #####
        #self.UniverseSettings.Resolution = Resolution.Daily
        self.UniverseSettings.Resolution = Resolution.Minute
        self.AddUniverse(self.UniverseCoarseFilter, self.UniverseFundamentalsFilter)
        self.num_screener = 100
        self.num_stocks = 10
        self.formation_days = 70
        self.lowmom = False
        self.data = {}
        
        # rebalance the universe selection once a month
        self.rebalance_flag = 0
        # make sure to run the universe selection at the start of the algorithm even if it's not the month start
        self.flip_flag = 0
        self.first_month_trade_flag = 1
        self.trade_flag = 0 
        self.symbols = None
        self.month = -1
        self.reb_count = 0
        
        self.Schedule.On(
            self.DateRules.EveryDay(),
            self.TimeRules.AfterMarketOpen('SPY', 120),
            self.rebalance_when_out_of_the_market
        )
        
        self.Schedule.On(
            self.DateRules.EveryDay(), 
            self.TimeRules.BeforeMarketClose('SPY', 0), 
            self.record_vars
        )  
        
        # Setup daily consolidation
        symbols = [self.MRKT] + self.FORPAIRS
        for symbol in symbols:
            self.consolidator = TradeBarConsolidator(timedelta(days=1))
            self.consolidator.DataConsolidated += self.consolidation_handler
            self.SubscriptionManager.AddConsolidator(symbol, self.consolidator)
        
        # Warm up history
        self.lookback = 252
        self.history = self.History(symbols, self.lookback, Resolution.Daily)
        if self.history.empty or 'close' not in self.history.columns:
            return
        self.history = self.history['close'].unstack(level=0).dropna()
        self.update_history_shift()
        
        # Benchmark = record SPY
        self.spy = []

 
    def UniverseCoarseFilter(self, coarse):
        #self.Debug(str(self.Time) + "UniverseCoarseFilter: be_in:" + str(self.be_in) + " flip_flag:" + str(self.flip_flag))
        #if (self.rebalance_flag or self.first_month_trade_flag) and (self.be_in or self.flip_flag):
        if self.month == self.Time.month:
            return Universe.Unchanged
            
        self.month = self.Time.month
            # drop stocks which have no fundamental data or have too low prices
        selected = [x for x in coarse if (x.HasFundamentalData) and (float(x.Price) > 5)]
            # rank the stocks by dollar volume 
        filtered = sorted(selected, key=lambda x: x.DollarVolume, reverse=True)
        return [x.Symbol for x in filtered[:200]]
        #else:
        #    return self.symbols


    def UniverseFundamentalsFilter(self, fundamental):
        #self.Debug(str(self.Time) + "UniverseFundamentalsFilter: be_in:" + str(self.be_in) + " flip_flag:" + str(self.flip_flag))
        #if (self.rebalance_flag or self.first_month_trade_flag) and (self.be_in or self.flip_flag):
            #hist = self.History([i.Symbol for i in fundamental], 1, Resolution.Daily)
        try:
            filtered_fundamental = [x for x in fundamental if (x.ValuationRatios.EVToEBITDA > 0) 
                                                    and (x.EarningReports.BasicAverageShares.ThreeMonths > 0) 
                                                    and float(x.EarningReports.BasicAverageShares.ThreeMonths) * x.Price > 2e9]
                                                    #and float(x.EarningReports.BasicAverageShares.ThreeMonths) * hist.loc[str(x.Symbol)]['close'][0] > 2e9]
                                                    #and x.EarningReports.BasicAverageShares.ThreeMonths * (x.EarningReports.BasicEPS.TwelveMonths*x.ValuationRatios.PERatio) > 2e9]
        except:
            filtered_fundamental = [x for x in fundamental if (x.ValuationRatios.EVToEBITDA > 0) 
                                                and (x.EarningReports.BasicAverageShares.ThreeMonths > 0)] 

        top = sorted(filtered_fundamental, key = lambda x: x.ValuationRatios.EVToEBITDA, reverse=True)[:self.num_screener]
        self.symbols = [x.Symbol for x in top]
        self.rebalance_flag = 0
        self.first_month_trade_flag = 0
        self.trade_flag = 1
        return self.symbols
        #else:
        #    return self.symbols
    
    def OnSecuritiesChanged(self, changes):
        
        for security in changes.RemovedSecurities:
            if security.Symbol in self.data:
                del self.data[security.Symbol]
        
        addedSymbols = []
        for security in changes.AddedSecurities:
            addedSymbols.append(security.Symbol)
            if security.Symbol not in self.data:
                self.data[security.Symbol] = SymbolData(security.Symbol, self.formation_days)
   
        if len(addedSymbols) > 0:
            history = self.History(addedSymbols, 1 + self.formation_days, Resolution.Daily).loc[addedSymbols]
            for symbol in addedSymbols:
                try:
                    self.data[symbol].Warmup(history.loc[symbol])
                except:
                    self.Debug(str(symbol))
                    continue
                self.RegisterIndicator(symbol, self.data[symbol].Roc, Resolution.Daily, Field.Close)
    
    def consolidation_handler(self, sender, consolidated):
        self.history.loc[consolidated.EndTime, consolidated.Symbol] = consolidated.Close
        self.history = self.history.iloc[-self.lookback:]
        self.update_history_shift()
        
    def update_history_shift(self):
        self.history_shift = self.history.rolling(11, center=True).mean().shift(60)

    def derive_vola_waitdays(self):
        volatility = np.log1p(self.history[[self.MRKT]].pct_change()).std() * np.sqrt(252)
        wait_days = int(volatility * self.WAITD_CONSTANT)
        returns_lookback = int((1.0 - volatility) * self.WAITD_CONSTANT)
        return wait_days, returns_lookback
 
        
    def rebalance_when_out_of_the_market(self):
        wait_days, returns_lookback = self.derive_vola_waitdays()
        
        ## Check for Bear
        returns = self.history.pct_change(returns_lookback).iloc[-1]
    
        silver_returns = returns[self.SLVA]
        gold_returns = returns[self.GOLD]
        industrials_returns = returns[self.INDU]
        utilities_returns = returns[self.UTIL]
        metals_returns = returns[self.METL]
        dollar_returns = returns[self.USDX]
        
        self.DISTILLED_BEAR = (((gold_returns > silver_returns) and
                       (utilities_returns > industrials_returns)) and 
                       (metals_returns < dollar_returns)
                       )
        
        # Determine whether 'in' or 'out' of the market
        if self.DISTILLED_BEAR:
            self.BE_IN = False
            self.OUTDAY = self.DCOUNT
            self.trade({**dict.fromkeys(self.Portfolio.Keys, 0), **self.HLD_OUT})
        if self.DCOUNT >= self.OUTDAY + wait_days:
            self.BE_IN = True
        self.DCOUNT += 1
        
        # Only re-shuffle stock allocation when switching from out to in, not in-between
        if not self.BE_IN_PRIOR and self.BE_IN:
            self.flip_flag = 1
            self.rebalance()
            self.reb_count = self.DCOUNT
            self.flip_flag = 0
        
        self.BE_IN_PRIOR = self.BE_IN


    def rebalance(self):
        self.rebalance_flag = 1
        #self.Debug(str(self.Time) + "rebalance: be_in:" + str(self.be_in) + " flip_flag:" + str(self.flip_flag))
            
        if self.symbols is None: return
        chosen_df = self.calc_return(self.symbols)
        chosen_df = chosen_df.iloc[:self.num_stocks]
        
        for symbol in chosen_df.index:
            self.AddEquity(symbol)
        
        weight = 0.99/len(chosen_df)
        self.trade({**dict.fromkeys(chosen_df.index.tolist(), weight), **dict.fromkeys(list(dict.fromkeys(set(self.Portfolio.Keys) - set(chosen_df.index))), 0), **dict.fromkeys(self.HLD_OUT, 0)})
        
        
    def calc_return(self, stocks):
        ret = {}

        for symbol in stocks:
            try:
                ret[symbol] = self.data[symbol].Roc.Current.Value
            except:
                self.Debug(str(symbol))
                continue
            
        df_ret = pd.DataFrame.from_dict(ret, orient='index')
        df_ret.columns = ['return']
        sort_return = df_ret.sort_values(by = ['return'], ascending = self.lowmom)
        
        return sort_return
       
        
    def trade(self, weight_by_sec):
        buys = []
        for sec, weight in weight_by_sec.items():
            # Check that we have data in the algorithm to process a trade
            if not self.CurrentSlice.ContainsKey(sec) or self.CurrentSlice[sec] is None:
                continue
            
            cond1 = weight == 0 and self.Portfolio[sec].IsLong
            cond2 = weight > 0 and not self.Portfolio[sec].Invested
            if cond1 or cond2:
                quantity = self.CalculateOrderQuantity(sec, weight)
                if quantity > 0:
                    buys.append((sec, quantity))
                elif quantity < 0:
                    self.Order(sec, quantity)
        for sec, quantity in buys:
            self.Order(sec, quantity)               
 
        
    def record_vars(self): 
        self.spy.append(self.history[self.MRKT].iloc[-1])
        spy_perf = self.spy[-1] / self.spy[0] * self.cap
        self.Plot('Strategy Equity', 'SPY', spy_perf)
        
        account_leverage = self.Portfolio.TotalHoldingsValue / self.Portfolio.TotalPortfolioValue
        self.Plot('Holdings', 'leverage', round(account_leverage, 2))
    
        
class SymbolData(object):
    def __init__(self, symbol, roc):
        self.Symbol = symbol
        self.Roc = RateOfChange(roc)
   
    def Warmup(self, history):
        for index, row in history.iterrows():
            self.Roc.Update(index, row['close'])
