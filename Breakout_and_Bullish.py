import pandas as pd
import numpy as np
import talib
from fletch.event_source_models.event_generator_base import EventGeneratorBase
from pyocm.common.utils import setup_logging
from fletch.utils import enforce_max_hold, dedupe_events, match_entries_with_exits

logger = setup_logging(__name__)

class BreakoutBullishTrend(EventGeneratorBase):
    category_id = 140
    event_id = -1
    period: int = 0

    def get_event_name(self):
        return "breakout_bullish_trend_events"

    def initialize(self):
        super().initialize()
        self.period = self.config.getint("period", self.period)

    def calculate_opening_range(self, data, hour=1):
        opening_range_high = data['pricehigh'].rolling(1).max()
        opening_range_low = data['pricelow'].rolling(1).min()
        return opening_range_high, opening_range_low

    def calculate_fractal_chaos_bands(self, data):
        upper_band = talib.MAX(data['pricehigh'], timeperiod=self.period)
        lower_band = talib.MIN(data['pricelow'], timeperiod=self.period)
        return upper_band, lower_band

    def calculate_period_condition(self, data):
        sma_current = talib.SMA(data['priceclose'], timeperiod=self.period)
        sma_previous = talib.SMA(data['priceclose'], timeperiod=self.period - 1)
        period_condition = np.minimum(5, sma_current - sma_previous)
        return period_condition

    def get_breakout_bullish_trend_signals(self, data):
        opening_range_high, opening_range_low = self.calculate_opening_range(data, hour=1)
        upper_band, _ = self.calculate_fractal_chaos_bands(data)
        period_condition = self.calculate_period_condition(data)

        entry_signal=(data['priceclose'].iloc[0]>opening_range_high.iloc[-1])&(data['priceclose'].iloc[0]>upper_band.iloc[0])&(period_condition.iloc[-1] > 0.0)

        return entry_signal
    def events_by_osid(self, start_date, end_date, event_info, osid, prices, custom_data, **kwargs):
        if prices.empty:
            logger.warning(f"{osid} has no prices")
            return None
        
        csf = self.pricing_ds.cum_splitfactor(prices)
        # resampled_prices[["priceclose", "pricelow", "pricehigh", "priceopen"]] *= csf
        # resampled_prices["volume"] /= csf
        prices_1 = prices.copy()
        entry_signal = self.get_breakout_bullish_trend_signals(prices_1)
        prices_1['entry_signal'] = entry_signal

        buy_events = prices_1["entry_signal"].reset_index()
        buy_events = buy_events[["trade_date"]].copy()
        buy_events.insert(0, "event_id", event_info["event_id"])
        buy_events.insert(1, "osid", osid)
        buy_events.insert(2, "signal", "buy")
        buy_events.rename(columns={"trade_date": "event_date"}, inplace=True)

        sell_events = []
        stop_loss_percentage = 2
        target_profit_percentage = 4
        holding_price = None

        for index, row in prices_1.iterrows():
            if holding_price is None:
                if entry_signal:
                    holding_price = row['priceclose']
            else:
                stop_loss_price = holding_price * (1 - stop_loss_percentage / 100)
                target_profit_price = holding_price * (1 + target_profit_percentage / 100)

                if row['priceclose'] <= stop_loss_price or row['priceclose'] >= target_profit_price:
                    sell_events.append({
                        "event_id": event_info["event_id"],
                        "osid": osid,
                        "signal": "sell",
                        "event_date": index,
                        "priceclose": row['priceclose']
                    })
                    holding_price = None

        if sell_events:
            sell_events_df = pd.DataFrame(sell_events)
            return pd.concat([buy_events, sell_events_df])
        else:
            return buy_events

    def cache_key(self, start_date_id, end_date_id, *args, **kwargs):
        return f"period={self.period}"
    
class breakout_and_bullish_22100(BreakoutBullishTrend):
    event_id = 100
    period = 20
class breakout_and_bullish_22101(BreakoutBullishTrend):
    event_id = 101
    period = 14
class breakout_and_bullish_22102(BreakoutBullishTrend):
    event_id = 102
    period = 50