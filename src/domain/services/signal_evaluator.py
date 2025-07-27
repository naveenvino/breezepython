"""
Signal Evaluator Service
Evaluates all 8 trading signals based on weekly zones and bias
"""
import logging
from typing import List, Optional
from datetime import datetime

from ..value_objects.signal_types import (
    SignalType, SignalResult, BarData, WeeklyContext,
    TradeDirection
)


logger = logging.getLogger(__name__)


class SignalEvaluator:
    """Evaluate all 8 trading signals based on weekly zones and bias"""
    
    def evaluate_all_signals(self, current_bar: BarData, context: WeeklyContext) -> SignalResult:
        """
        Evaluate all signals in order of priority
        
        Args:
            current_bar: Current 1H bar
            context: Weekly context with zones, bias, and state
            
        Returns:
            SignalResult with triggered signal or no_signal
        """
        # Don't evaluate if signal already triggered this week
        if context.signal_triggered_this_week:
            return SignalResult.no_signal()
        
        # Get weekly bars for evaluation
        weekly_bars = context.weekly_bars
        if not weekly_bars:
            return SignalResult.no_signal()
        
        first_bar = weekly_bars[0] if len(weekly_bars) > 0 else None
        is_second_bar = len(weekly_bars) == 2
        
        # Evaluate signals in order (S1-S8)
        evaluators = [
            (self._evaluate_s1, (is_second_bar, first_bar, current_bar, context)),
            (self._evaluate_s2, (is_second_bar, first_bar, current_bar, context)),
            (self._evaluate_s3, (is_second_bar, first_bar, current_bar, weekly_bars, context)),
            (self._evaluate_s4, (first_bar, current_bar, weekly_bars, context)),
            (self._evaluate_s5, (first_bar, current_bar, context)),
            (self._evaluate_s6, (is_second_bar, first_bar, current_bar, weekly_bars, context)),
            (self._evaluate_s7, (current_bar, weekly_bars, context)),
            (self._evaluate_s8, (current_bar, weekly_bars, context))
        ]
        
        for evaluator, args in evaluators:
            signal = evaluator(*args)
            if signal.is_triggered:
                # Mark signal as triggered in context
                context.signal_triggered_this_week = True
                context.triggered_signal = signal.signal_type
                context.triggered_at = signal.entry_time
                return signal
        
        return SignalResult.no_signal()
    
    def _evaluate_s1(self, is_second_bar: bool, first_bar: Optional[BarData], 
                     current_bar: BarData, context: WeeklyContext) -> SignalResult:
        """
        S1: Bear Trap (Bullish) - Fake breakdown below support that recovers
        Evaluated on 2nd candle only
        """
        if not is_second_bar or not first_bar:
            return SignalResult.no_signal()
        
        zones = context.zones
        
        # Conditions
        cond1 = first_bar.open >= zones.lower_zone_bottom  # Opened above/on support
        cond2 = first_bar.close < zones.lower_zone_bottom  # Closed below support (fake breakdown)
        cond3 = current_bar.close > first_bar.low         # Recovery above 1st bar low
        
        if cond1 and cond2 and cond3:
            logger.info(f"S1 TRIGGERED! Bear trap detected at {current_bar.timestamp}")
            logger.info(f"  First bar: O={first_bar.open} C={first_bar.close} (below support {zones.lower_zone_bottom})")
            logger.info(f"  Second bar: C={current_bar.close} (recovered above first bar low {first_bar.low})")
            
            # Calculate stop loss
            stop_loss = first_bar.low - first_bar.body_range
            
            return SignalResult.from_signal(
                signal_type=SignalType.S1,
                stop_loss=stop_loss,
                entry_time=current_bar.timestamp,
                entry_price=current_bar.close,
                confidence=0.85
            )
        
        return SignalResult.no_signal()
    
    def _evaluate_s2(self, is_second_bar: bool, first_bar: Optional[BarData],
                     current_bar: BarData, context: WeeklyContext) -> SignalResult:
        """
        S2: Support Hold (Bullish) - Price respects support with bullish bias
        Evaluated on 2nd candle only
        """
        if not is_second_bar or not first_bar:
            return SignalResult.no_signal()
        
        zones = context.zones
        bias = context.bias
        
        # Only for bullish bias
        if not bias.is_bullish:
            return SignalResult.no_signal()
        
        # Proximity checks
        close_to_support_prev = zones.is_near_lower_zone(zones.prev_week_close)
        close_to_support_first = zones.is_near_lower_zone(first_bar.open)
        
        # All conditions
        if (first_bar.open > zones.prev_week_low and
            close_to_support_prev and
            close_to_support_first and
            first_bar.close >= zones.lower_zone_bottom and
            first_bar.close >= zones.prev_week_close and
            current_bar.close >= first_bar.low and
            current_bar.close > zones.prev_week_close and
            current_bar.close > zones.lower_zone_bottom):
            
            logger.info(f"S2 TRIGGERED! Support hold with bullish bias at {current_bar.timestamp}")
            
            stop_loss = zones.lower_zone_bottom
            
            return SignalResult.from_signal(
                signal_type=SignalType.S2,
                stop_loss=stop_loss,
                entry_time=current_bar.timestamp,
                entry_price=current_bar.close,
                confidence=0.80
            )
        
        return SignalResult.no_signal()
    
    def _evaluate_s3(self, is_second_bar: bool, first_bar: Optional[BarData],
                     current_bar: BarData, weekly_bars: List[BarData],
                     context: WeeklyContext) -> SignalResult:
        """
        S3: Resistance Hold (Bearish) - Price fails at resistance with bearish bias
        Has two trigger scenarios
        """
        if not first_bar:
            return SignalResult.no_signal()
        
        zones = context.zones
        bias = context.bias
        
        # Only for bearish bias
        if not bias.is_bearish:
            return SignalResult.no_signal()
        
        # Base conditions
        close_to_resistance_prev = zones.is_near_upper_zone(zones.prev_week_close)
        close_to_resistance_first = zones.is_near_upper_zone(first_bar.open)
        
        if not (close_to_resistance_prev and close_to_resistance_first and 
                first_bar.close <= zones.prev_week_high):
            return SignalResult.no_signal()
        
        # Scenario A: 2nd candle rejection
        if is_second_bar:
            touched_zone = (first_bar.high >= zones.upper_zone_bottom or 
                          current_bar.high >= zones.upper_zone_bottom)
            if (current_bar.close < first_bar.high and
                current_bar.close < zones.upper_zone_bottom and
                touched_zone):
                
                logger.info(f"S3 TRIGGERED! Scenario A - 2nd candle rejection at {current_bar.timestamp}")
                
                return SignalResult.from_signal(
                    signal_type=SignalType.S3,
                    stop_loss=zones.prev_week_high,
                    entry_time=current_bar.timestamp,
                    entry_price=current_bar.close,
                    confidence=0.75
                )
        
        # Scenario B: Breakdown below weekly lows
        # Calculate weekly minimums excluding current bar
        if len(weekly_bars) > 1:
            weekly_min_low = min(bar.low for bar in weekly_bars[:-1])
            weekly_min_close = min(bar.close for bar in weekly_bars[:-1])
            
            if (current_bar.close < first_bar.low and
                current_bar.close < zones.upper_zone_bottom and
                current_bar.close < weekly_min_low and
                current_bar.close < weekly_min_close):
                
                logger.info(f"S3 TRIGGERED! Scenario B - Breakdown below weekly lows at {current_bar.timestamp}")
                
                return SignalResult.from_signal(
                    signal_type=SignalType.S3,
                    stop_loss=zones.prev_week_high,
                    entry_time=current_bar.timestamp,
                    entry_price=current_bar.close,
                    confidence=0.78
                )
        
        return SignalResult.no_signal()
    
    def _evaluate_s4(self, first_bar: Optional[BarData], current_bar: BarData,
                     weekly_bars: List[BarData], context: WeeklyContext) -> SignalResult:
        """
        S4: Bias Failure Bull (Bullish) - Bearish bias fails, price breaks out
        """
        if not first_bar:
            return SignalResult.no_signal()
        
        zones = context.zones
        bias = context.bias
        
        # Only for bearish bias that fails
        if not bias.is_bearish:
            return SignalResult.no_signal()
        
        # First bar must open above resistance
        if first_bar.open <= zones.upper_zone_top:
            return SignalResult.no_signal()
        
        # Check S4 breakout trigger
        s4_triggered = self._check_s4_trigger(weekly_bars, context)
        
        if s4_triggered:
            logger.info(f"S4 TRIGGERED! Bearish bias failure with breakout at {current_bar.timestamp}")
            
            # Use first hour bar for stop loss
            stop_loss = context.first_hour_bar.low if context.first_hour_bar else first_bar.low
            
            return SignalResult.from_signal(
                signal_type=SignalType.S4,
                stop_loss=stop_loss,
                entry_time=current_bar.timestamp,
                entry_price=current_bar.close,
                confidence=0.82
            )
        
        return SignalResult.no_signal()
    
    def _evaluate_s5(self, first_bar: Optional[BarData], current_bar: BarData,
                     context: WeeklyContext) -> SignalResult:
        """
        S5: Bias Failure Bear (Bearish) - Bullish bias fails, price breaks down
        """
        if not first_bar or not context.first_hour_bar:
            return SignalResult.no_signal()
        
        zones = context.zones
        bias = context.bias
        
        # Only for bullish bias that fails
        if not bias.is_bullish:
            return SignalResult.no_signal()
        
        # All conditions
        if (first_bar.open < zones.lower_zone_bottom and
            context.first_hour_bar.close < zones.lower_zone_bottom and
            context.first_hour_bar.close < zones.prev_week_low and
            current_bar.close < context.first_hour_bar.low):
            
            logger.info(f"S5 TRIGGERED! Bullish bias failure with breakdown at {current_bar.timestamp}")
            
            stop_loss = context.first_hour_bar.high
            
            return SignalResult.from_signal(
                signal_type=SignalType.S5,
                stop_loss=stop_loss,
                entry_time=current_bar.timestamp,
                entry_price=current_bar.close,
                confidence=0.80
            )
        
        return SignalResult.no_signal()
    
    def _evaluate_s6(self, is_second_bar: bool, first_bar: Optional[BarData],
                     current_bar: BarData, weekly_bars: List[BarData],
                     context: WeeklyContext) -> SignalResult:
        """
        S6: Weakness Confirmed (Bearish) - Similar to S3 with different entry
        """
        if not first_bar:
            return SignalResult.no_signal()
        
        zones = context.zones
        bias = context.bias
        
        # Only for bearish bias
        if not bias.is_bearish:
            return SignalResult.no_signal()
        
        # Base conditions
        if not (first_bar.high >= zones.upper_zone_bottom and
                first_bar.close <= zones.upper_zone_top and
                first_bar.close <= zones.prev_week_high):
            return SignalResult.no_signal()
        
        # Same trigger scenarios as S3
        # Scenario A
        if is_second_bar:
            if (current_bar.close < first_bar.high and
                current_bar.close < zones.upper_zone_bottom):
                
                logger.info(f"S6 TRIGGERED! Scenario A - Weakness confirmed at {current_bar.timestamp}")
                
                return SignalResult.from_signal(
                    signal_type=SignalType.S6,
                    stop_loss=zones.prev_week_high,
                    entry_time=current_bar.timestamp,
                    entry_price=current_bar.close,
                    confidence=0.73
                )
        
        # Scenario B
        if len(weekly_bars) > 1:
            weekly_min_low = min(bar.low for bar in weekly_bars[:-1])
            weekly_min_close = min(bar.close for bar in weekly_bars[:-1])
            
            if (current_bar.close < first_bar.low and
                current_bar.close < zones.upper_zone_bottom and
                current_bar.close < weekly_min_low and
                current_bar.close < weekly_min_close):
                
                logger.info(f"S6 TRIGGERED! Scenario B - Weakness breakdown at {current_bar.timestamp}")
                
                return SignalResult.from_signal(
                    signal_type=SignalType.S6,
                    stop_loss=zones.prev_week_high,
                    entry_time=current_bar.timestamp,
                    entry_price=current_bar.close,
                    confidence=0.75
                )
        
        return SignalResult.no_signal()
    
    def _evaluate_s7(self, current_bar: BarData, weekly_bars: List[BarData],
                     context: WeeklyContext) -> SignalResult:
        """
        S7: 1H Breakout Confirmed (Bullish) - Strongest breakout signal
        """
        zones = context.zones
        
        # Check S4 trigger first
        s4_triggered = self._check_s4_trigger(weekly_bars, context)
        if not s4_triggered:
            return SignalResult.no_signal()
        
        # Check if too close below prev high
        gap_pct = ((zones.prev_week_high - current_bar.close) / current_bar.close) * 100
        too_close_below = current_bar.close < zones.prev_week_high and gap_pct < 0.40
        
        if too_close_below:
            return SignalResult.no_signal()
        
        # Check strongest breakout
        if len(weekly_bars) > 1:
            weekly_max_high = max(bar.high for bar in weekly_bars[:-1])
            weekly_max_close = max(bar.close for bar in weekly_bars[:-1])
            
            if (current_bar.close > weekly_max_high and
                current_bar.close > weekly_max_close):
                
                logger.info(f"S7 TRIGGERED! Strongest breakout confirmed at {current_bar.timestamp}")
                
                stop_loss = context.first_hour_bar.low if context.first_hour_bar else weekly_bars[0].low
                
                return SignalResult.from_signal(
                    signal_type=SignalType.S7,
                    stop_loss=stop_loss,
                    entry_time=current_bar.timestamp,
                    entry_price=current_bar.close,
                    confidence=0.88
                )
        
        return SignalResult.no_signal()
    
    def _evaluate_s8(self, current_bar: BarData, weekly_bars: List[BarData],
                     context: WeeklyContext) -> SignalResult:
        """
        S8: 1H Breakdown Confirmed (Bearish) - Strongest breakdown signal
        """
        zones = context.zones
        
        # Check all conditions
        s8_triggered = self._check_s8_trigger(weekly_bars, context)
        if not s8_triggered:
            return SignalResult.no_signal()
        
        if not context.has_touched_upper_zone_this_week:
            return SignalResult.no_signal()
        
        if current_bar.close >= zones.upper_zone_bottom:
            return SignalResult.no_signal()
        
        # Check weakest breakdown
        if len(weekly_bars) > 1:
            weekly_min_low = min(bar.low for bar in weekly_bars[:-1])
            weekly_min_close = min(bar.close for bar in weekly_bars[:-1])
            
            if (current_bar.close < weekly_min_low and
                current_bar.close < weekly_min_close):
                
                logger.info(f"S8 TRIGGERED! Strongest breakdown confirmed at {current_bar.timestamp}")
                
                stop_loss = context.first_hour_bar.high if context.first_hour_bar else weekly_bars[0].high
                
                return SignalResult.from_signal(
                    signal_type=SignalType.S8,
                    stop_loss=stop_loss,
                    entry_time=current_bar.timestamp,
                    entry_price=current_bar.close,
                    confidence=0.87
                )
        
        return SignalResult.no_signal()
    
    def _check_s4_trigger(self, weekly_bars: List[BarData], context: WeeklyContext) -> bool:
        """
        Check if S4 breakout trigger is active
        Complex logic tracking breakout progression
        """
        if not weekly_bars or not context.first_hour_bar:
            return False
        
        first_hour_high = context.first_hour_bar.high
        first_hour_day = context.first_hour_bar.timestamp.date()
        
        highest_high = 0.0
        signal_fired = False
        
        for bar in weekly_bars:
            if signal_fired:
                continue
            
            # Track highest high before this bar
            highest_high_before = highest_high
            highest_high = max(highest_high, bar.high)
            
            # Same day as first hour
            if bar.timestamp.date() == first_hour_day:
                if bar.close > first_hour_high:
                    signal_fired = True
            else:
                # Different day logic
                if context.s4_breakout_candle_high is None:
                    # Look for breakout candle
                    if (bar.close > bar.open and
                        bar.close > first_hour_high and
                        bar.high >= highest_high_before):
                        context.s4_breakout_candle_high = bar.high
                else:
                    # Check if close above breakout candle high
                    if bar.close > context.s4_breakout_candle_high:
                        signal_fired = True
        
        # Check if just fired (current vs previous)
        if len(weekly_bars) >= 2:
            prev_bars = weekly_bars[:-1]
            prev_triggered = self._check_s4_trigger_simple(prev_bars, context)
            return signal_fired and not prev_triggered
        
        return signal_fired
    
    def _check_s4_trigger_simple(self, bars: List[BarData], context: WeeklyContext) -> bool:
        """Simplified S4 trigger check for previous bars"""
        if not bars or not context.first_hour_bar:
            return False
        
        # Simplified logic without state modification
        for bar in bars:
            if bar.close > context.first_hour_bar.high:
                return True
        return False
    
    def _check_s8_trigger(self, weekly_bars: List[BarData], context: WeeklyContext) -> bool:
        """
        Check if S8 breakdown trigger is active (mirror of S4)
        """
        if not weekly_bars or not context.first_hour_bar:
            return False
        
        first_hour_low = context.first_hour_bar.low
        first_hour_day = context.first_hour_bar.timestamp.date()
        
        lowest_low = float('inf')
        signal_fired = False
        
        for bar in weekly_bars:
            if signal_fired:
                continue
            
            # Track lowest low before this bar
            lowest_low_before = lowest_low
            lowest_low = min(lowest_low, bar.low)
            
            # Same day as first hour
            if bar.timestamp.date() == first_hour_day:
                if bar.close < first_hour_low:
                    signal_fired = True
            else:
                # Different day logic
                if context.s8_breakdown_candle_low is None:
                    # Look for breakdown candle
                    if (bar.close < bar.open and
                        bar.close < first_hour_low and
                        bar.low <= lowest_low_before):
                        context.s8_breakdown_candle_low = bar.low
                else:
                    # Check if close below breakdown candle low
                    if bar.close < context.s8_breakdown_candle_low:
                        signal_fired = True
        
        # Check if just fired
        if len(weekly_bars) >= 2:
            prev_bars = weekly_bars[:-1]
            prev_triggered = self._check_s8_trigger_simple(prev_bars, context)
            return signal_fired and not prev_triggered
        
        return signal_fired
    
    def _check_s8_trigger_simple(self, bars: List[BarData], context: WeeklyContext) -> bool:
        """Simplified S8 trigger check for previous bars"""
        if not bars or not context.first_hour_bar:
            return False
        
        for bar in bars:
            if bar.close < context.first_hour_bar.low:
                return True
        return False