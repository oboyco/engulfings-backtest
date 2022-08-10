import pandas as pd
import numpy as np

span1 = 20 # number of days (span) for ATR calculation
span2 = 60 # number of days (span) for New High/Low check
span3 = 20 # number of days (span) for New High/Low check
ENG_to_ATR_Ratio_MIN = 1 # lower limit for engulfing candle to ATR ratio
ENG_to_ATR_Ratio_MAX = 2 # higher limit for engulfing candle to ATR ratio
Profit_Loss_Ratio = 3 # X profit(s) vs 1 loss ratio


df = pd.read_csv("WIKI_PRICES.csv").iloc[:,[0,1,9,10,11,12,13]]
df.columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']
df['range'] = df['high'] - df['low']
df['atr'] = df['range'].rolling(window=span1).mean()
df['maxvol'] = df['volume'].rolling(window=span3).max()
df['high/low'] = np.where(df['low']<=df['low'].rolling(span2).min(), 'new low',
                 np.where(df['high']>=df['high'].rolling(span2).max(), 'new high', ''))

conditions = [(df['ticker'] == df['ticker'].shift(1)) &
              (df['high/low'].shift(1) == "new high") &
              (df['close'] < df['open']) & 
              (df['close'] < df['open'].shift(1)) &
              (df['close'] < df['close'].shift(1)) &
              (df['open'] > df['open'].shift(1)) &
              (df['open'] > df['close'].shift(1)) &
              (df['volume'] == df['maxvol']) &
              (df['range']/df['atr'] > ENG_to_ATR_Ratio_MIN) &
              (df['range']/df['atr'] < ENG_to_ATR_Ratio_MAX)
              ,
              (df['ticker'] == df['ticker'].shift(1)) &
              (df['high/low'].shift(1) == "new low") &
              (df['close'] > df['open']) &
              (df['close'] > df['open'].shift(1)) &
              (df['close'] > df['close'].shift(1)) &
              (df['open'] < df['open'].shift(1)) &
              (df['open'] < df['close'].shift(1)) &
              (df['volume'] == df['maxvol']) &
              (df['range']/df['atr'] > ENG_to_ATR_Ratio_MIN) &
              (df['range']/df['atr'] < ENG_to_ATR_Ratio_MAX)
             ]

choices = ["bear", "bull"]

df["engulf_event"] = np.select(conditions, choices, default = '')

df['trade_id']=df.engulf_event.replace('',np.nan).notna().cumsum()

df["trade_type"] = 0
df["trade_type"] = np.where(df['engulf_event'] == 'bull','buy',
                        np.where(df['engulf_event'] == 'bear','sell', pd.NA))
df["trade_type"] = df["trade_type"].fillna(method="ffill")

df["trade_price"] = 0
df["trade_price"] = np.where(df['engulf_event'] != '', df['close'], 0)
df["trade_price"] = df["trade_price"].replace(to_replace=0, method='ffill')

df["stop-loss"] = 0
df["stop-loss"] = np.where(df['engulf_event'] != '', df['open'], 0)
df["stop-loss"] = df["stop-loss"].replace(to_replace=0, method='ffill')

df["take-profit"] = 0
df["take-profit"] = np.where(df['engulf_event'] != '', 
                    np.where(df['trade_type'] != 'buy', df['close']+(df['close']-df['open'])*Profit_Loss_Ratio, 
                    np.where(df['trade_type'] != 'sell', df['close']-(df['open']-df['close'])*Profit_Loss_Ratio, 0)), 0)
df["take-profit"] = df["take-profit"].replace(to_replace=0, method='ffill')

conditions2 = [(df['trade_type'] == 'buy')  & (df["high"] >= df["take-profit"])
               ,
               (df['trade_type'] == 'sell') & (df["low"]  <= df["take-profit"])
               ,
               (df['trade_type'] == 'buy')  & (df["low"]  <= df["stop-loss"])
               ,
               (df['trade_type'] == 'sell') & (df["high"] >= df["stop-loss"])
             ]

choices2 = ["profit", "profit", "loss", "loss"]


df["pre-result"] = np.where(df['trade_id']>df['trade_id'].shift(1),'',np.select(conditions2, choices2, default = ''))


df["pre-result2"] = df.groupby(["trade_id"])["pre-result"].apply(lambda x: x.ne('').cumsum())


df["result"] = np.where((df["pre-result2"] == 1) & (df["pre-result"] != ''),df["pre-result"],'')

df["outcome"] = np.where((df["result"] == 'profit') & (df["trade_type"] == 'buy'),df["take-profit"]-df["trade_price"],
                np.where((df["result"] == 'profit') & (df["trade_type"] == 'sell'),df["trade_price"]-df["take-profit"],
                np.where((df["result"] == 'loss') & (df["trade_type"] == 'buy'),df["stop-loss"]-df["trade_price"],
                np.where((df["result"] == 'loss') & (df["trade_type"] == 'sell'),df["trade_price"]-df["stop-loss"],0))))


df = df[df.result != '']
df.to_excel("output.xlsx")