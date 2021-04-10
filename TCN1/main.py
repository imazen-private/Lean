import clr
clr.AddReference("System")
clr.AddReference("QuantConnect.Algorithm")
clr.AddReference("QuantConnect.Common")

from System import *
from QuantConnect import *
from QuantConnect.Algorithm import *

import json
import numpy as np
import pandas as pd
from io import StringIO
from keras.models import Sequential
from keras.layers import Dense, Activation
from keras.optimizers import SGD
from keras.utils.generic_utils import serialize_keras_object

from Library.tcn.main import TCN, compiled_tcn, tcn_full_summary


class TCN(QCAlgorithm):

    def Initialize(self):
        self.SetStartDate(2015, 1, 1)
        self.SetEndDate(2017, 1, 1)
        self.SetCash(100000)  # Set Strategy Cash

        self.modelBySymbol = {}

        for ticker in ["AMD", "INTC"]:
            symbol = self.AddEquity(ticker).Symbol

            # Read the model saved in the ObjectStore
            if self.ObjectStore.ContainsKey(f'{symbol}_model'):
                modelStr = self.ObjectStore.Read(f'{symbol}_model')
                config = json.loads(modelStr)['config']
                self.modelBySymbol[symbol] = Sequential.from_config(config)
                self.Debug(f'Model for {symbol} successfully retrieved from the ObjectStore')



        self.AddAlpha(RsiAlphaModel(60, Resolution.Minute))

        self.SetExecution(ImmediateExecutionModel())

        self.SetPortfolioConstruction(BlackLittermanOptimizationPortfolioConstructionModel())

        self.SetRiskManagement(MaximumDrawdownPercentPerSecurity(0.01))

        self.SetUniverseSelection(QC500UniverseSelectionModel())

    def OnData(self, data):
        '''OnData event is the primary entry point for your algorithm. Each new data point will be pumped in here.
            Arguments:
                data: Slice object keyed by symbol containing the stock data
        '''

        # if not self.Portfolio.Invested:
        #    self.SetHoldings("SPY", 1)
# Your New Python File
