import pandas as pd
import numpy as np
import talib
from fletch.event_source_models.event_generator_base import EventGeneratorBase
from pyocm.common.utils import setup_logging
from fletch.utils import enforce_max_hold, dedupe_events, match_entries_with_exits

logger = setup_logging(__name__)

class MACDRSIEvents(EventGeneratorBase):
    category_id = 151  
    event_id = -1
    rsi_period: int = 25
    rsi_upper_threshold: int = 35

    def get_event_name(self):
        return "macd_rsi_strategy_events"

    def initialize(self):
        super().initialize()
        self.rsi_period = self.config.getint("rsi_period", self.rsi_period)
        self.rsi_upper_threshold = self.config.getint("rsi_upper_threshold", self.rsi_upper_threshold) 
    def calculate_heikin_ashi(self, data):
        ha_open = (data['priceopen'].shift() + data['priceclose'].shift()) / 2
        ha_close = (data['priceopen'] + data['pricehigh'] + data['pricelow'] + data['priceclose']) / 4
        ha_high = data[['pricehigh', 'priceopen', 'priceclose']].max(axis=1)
        ha_low = data[['pricelow', 'priceopen', 'priceclose']].min(axis=1)

        data['HA_Close'] = ha_close 
        data['HA_Open'] = ha_open
        data['HA_High'] = ha_high
        data['HA_Low'] = ha_low

        return data
    def get_macd_rsi_signals(self, data):
        ha_df = self.calculate_heikin_ashi(data)

        macd, signal, _ = talib.MACD(ha_df['HA_Close'], fastperiod=7, slowperiod=13, signalperiod=9)

        rsi = talib.RSI(ha_df['HA_Close'], timeperiod=self.rsi_period)

        entry_signal = (macd > signal) & (rsi > self.rsi_upper_threshold) & (rsi.shift(-1) < self.rsi_upper_threshold)

        return entry_signal
    def events_by_osid(self, start_date, end_date, event_info, osid, prices, custom_data, **kwargs):
        if prices.empty:
            logger.warning(f"{osid} has no prices")
            return None

        ha_df = self.calculate_heikin_ashi(prices)
        entry_signal = self.get_macd_rsi_signals(ha_df)

        buy_events = ha_df[entry_signal].reset_index()
        buy_events = buy_events[["trade_date", "HA_Close"]].copy()
        buy_events.insert(0, "event_id", event_info["event_id"])
        buy_events.insert(1, "osid", osid)
        buy_events.insert(2, "signal", "buy")
        buy_events = buy_events.reset_index(drop=True)
        buy_events.rename(columns={"trade_date": "event_date"}, inplace=True)

        return buy_events
    def cache_key(self, start_date_id, end_date_id, *args, **kwargs):
        key = f"rsi_period={self.rsi_period},rsi_upper_threshold={self.rsi_upper_threshold}"
        return key

class MACDRSI_21001(MACDRSIEvents):
    event_id = 148
    rsi_period = 25
    rsi_upper_threshold = 35