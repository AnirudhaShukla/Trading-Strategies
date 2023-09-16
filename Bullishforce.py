import pandas as pd
import numpy as np
import talib
from fletch.event_source_models.event_generator_base import EventGeneratorBase
from pyocm.common.utils import setup_logging
from fletch.utils import enforce_max_hold, dedupe_events, match_entries_with_exits

logger = setup_logging(__name__)

class BullishForceEvents(EventGeneratorBase):
    category_id = 150 
    event_id = -1
    period: int = 0
    adx_threshold: int = 0

    def get_event_name(self):
        return "bullish_force_strategy_events"

    def initialize(self):
        super().initialize()
        self.period = self.config.getint("period", self.period)
        self.adx_threshold = self.config.getint("adx_threshold", self.adx_threshold)

    def calculate_heikin_ashi(self, data):
        ha_open = (data['priceopen'].shift() + data['priceclose'].shift()) / 2
        ha_close = (data['priceopen'] + data['pricehigh'] + data['pricelow'] + data['priceclose']) / 4
        ha_high = data[['pricehigh', 'priceopen', 'priceclose']].max(axis=1)
        ha_low = data[['pricelow', 'priceopen', 'priceclose']].min(axis=1)

        # ha_data = pd.DataFrame({'HA_Open': ha_open, 'HA_Close': ha_close, 'HA_High': ha_high, 'HA_Low': ha_low})

        data['HA_Close'] = ha_close 
        data['HA_Open'] = ha_open
        data['HA_High'] = ha_high
        data['HA_Low'] = ha_low

        return data

    def get_bullish_force_signals(self, data_1):
        ha_df = self.calculate_heikin_ashi(data_1)
        
        vwap = talib.SMA(ha_df["HA_Close"], timeperiod=self.period)  
        adx = talib.ADX(ha_df["HA_High"], ha_df["HA_Low"], ha_df["HA_Close"], timeperiod=self.period)
        plus_di = talib.PLUS_DI(ha_df["HA_High"], ha_df["HA_Low"], ha_df["HA_Close"], timeperiod=self.period)
        minus_di = talib.MINUS_DI(ha_df["HA_High"], ha_df["HA_Low"], ha_df["HA_Close"], timeperiod=self.period)
        
        entry_signal = (ha_df["HA_Close"] > vwap) & (adx > self.adx_threshold) & (plus_di > minus_di)

        return entry_signal
    
    def get_bearish_force_signals(self, data_2):
        ha_df = self.calculate_heikin_ashi(data_2)
        
        pct_change = (data_2['priceclose'].shift() - data_2['pricehigh'].shift(1)) / data_2['pricehigh'].shift(1) * 100

        vwap = talib.SMA(ha_df["HA_Close"], timeperiod=self.period)  
        adx = talib.ADX(ha_df["HA_High"], ha_df["HA_Low"], ha_df["HA_Close"], timeperiod=self.period)
        plus_di = talib.PLUS_DI(ha_df["HA_High"], ha_df["HA_Low"], ha_df["HA_Close"], timeperiod=self.period)
        minus_di = talib.MINUS_DI(ha_df["HA_High"], ha_df["HA_Low"], ha_df["HA_Close"], timeperiod=self.period)
        
        exit_signal = (ha_df["HA_Close"] < vwap) & (adx > self.adx_threshold) & (plus_di > minus_di) & (pct_change <= -0.5)

        return exit_signal

    def events_by_osid(self, start_date, end_date, event_info, osid, prices, custom_data, **kwargs):
        if prices.empty:
            logger.warning(f"{osid} has no prices")
            return None

        csf = self.pricing_ds.cum_splitfactor(prices)
        # prices[["priceclose", "pricelow", "pricehigh", "priceopen"]] *= csf
        # prices["volume"] /= csf

        ha_df_1 = self.calculate_heikin_ashi(prices)

        entry_signal = self.get_bullish_force_signals(ha_df_1)
        exit_signal = self.get_bearish_force_signals(ha_df_1)

        buy_events = ha_df_1[entry_signal].reset_index()
        buy_events = buy_events[["trade_date", "HA_Close"]].copy()
        buy_events.insert(0, "event_id", event_info["event_id"])
        buy_events.insert(1, "osid", osid)
        buy_events.insert(2, "signal", "buy")
        buy_events = buy_events.reset_index(drop=True)
        buy_events.rename(columns={"trade_date": "event_date"}, inplace=True)

        sell_events = ha_df_1[exit_signal].reset_index()
        sell_events = sell_events[["trade_date", "HA_Close"]].copy()
        sell_events.insert(0, "event_id", event_info["event_id"])
        sell_events.insert(1, "osid", osid)
        sell_events.insert(2, "signal", "sell")
        sell_events = sell_events.reset_index(drop=True)
        sell_events.rename(columns={"trade_date": "event_date"}, inplace=True)

        return sell_events

    def cache_key(self, start_date_id, end_date_id, *args, **kwargs):
        return f"adx_threshold={self.adx_threshold},period={self.period}"

class BullishForce_20001(BullishForceEvents):
    event_id = 147
    period = 14
    adx_threshold = 24
class BullishForce_20002(BullishForceEvents):
    event_id = 147
    period = 20
    adx_threshold = 24    
class BullishForce_20003(BullishForceEvents):
    event_id = 147
    period = 20
    adx_threshold = 14
class BullishForce_20001(BullishForceEvents):
    event_id = 147
    period = 50
    adx_threshold = 20