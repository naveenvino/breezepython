"""
Run Backtest Use Case
Main backtesting logic that orchestrates the entire backtest process
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from decimal import Decimal
import asyncio

from ...domain.value_objects.signal_types import SignalType, BarData
from ...domain.services.signal_evaluator import SignalEvaluator
from ...domain.services.weekly_context_manager import WeeklyContextManager
from ...infrastructure.services.data_collection_service import DataCollectionService
from ...infrastructure.services.option_pricing_service import OptionPricingService
from ...infrastructure.database.models import (
    BacktestRun, BacktestTrade, BacktestPosition, BacktestDailyResult,
    BacktestStatus, NiftyIndexData, TradeOutcome
)
from ...infrastructure.database.database_manager import get_db_manager


logger = logging.getLogger(__name__)


class BacktestParameters:
    """Parameters for running a backtest"""
    def __init__(
        self,
        from_date: datetime,
        to_date: datetime,
        initial_capital: float = 500000,
        lot_size: int = 50,
        signals_to_test: List[str] = None,
        use_hedging: bool = True,
        hedge_offset: int = 500,
        commission_per_lot: float = 40,
        slippage_percent: float = 0.001
    ):
        self.from_date = from_date
        self.to_date = to_date
        self.initial_capital = initial_capital
        self.lot_size = lot_size
        self.signals_to_test = signals_to_test or ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8"]
        self.use_hedging = use_hedging
        self.hedge_offset = hedge_offset
        self.commission_per_lot = commission_per_lot
        self.slippage_percent = slippage_percent


class RunBacktestUseCase:
    """
    Use case for running a complete backtest
    """
    
    def __init__(
        self,
        data_collection_service: DataCollectionService,
        option_pricing_service: OptionPricingService
    ):
        self.data_collection = data_collection_service
        self.option_pricing = option_pricing_service
        self.signal_evaluator = SignalEvaluator()
        self.context_manager = WeeklyContextManager()
        self.db_manager = get_db_manager()
    
    async def execute(self, params: BacktestParameters) -> str:
        """
        Execute backtest and return backtest run ID
        
        Args:
            params: Backtest parameters
            
        Returns:
            Backtest run ID
        """
        logger.info(f"Starting backtest from {params.from_date} to {params.to_date}")
        
        # Create backtest run record
        backtest_run = await self._create_backtest_run(params)
        
        try:
            # Update status to running
            await self._update_backtest_status(backtest_run.id, BacktestStatus.RUNNING)
            
            # Ensure data is available
            await self._ensure_data_available(params.from_date, params.to_date)
            
            # Get NIFTY data for the period
            nifty_data = await self.data_collection.get_nifty_data(
                params.from_date, params.to_date
            )
            
            if not nifty_data:
                raise ValueError("No NIFTY data available for the specified period")
            
            # Run backtest
            results = await self._run_backtest_logic(
                backtest_run, nifty_data, params
            )
            
            # Update backtest run with results
            await self._update_backtest_results(backtest_run.id, results)
            
            # Update status to completed
            await self._update_backtest_status(backtest_run.id, BacktestStatus.COMPLETED)
            
            logger.info(f"Backtest completed successfully. ID: {backtest_run.id}")
            return backtest_run.id
            
        except Exception as e:
            logger.error(f"Backtest failed: {e}")
            await self._update_backtest_status(
                backtest_run.id, BacktestStatus.FAILED, str(e)
            )
            raise
    
    async def _create_backtest_run(self, params: BacktestParameters) -> BacktestRun:
        """Create backtest run record in database"""
        backtest_run = BacktestRun(
            name=f"Backtest {params.from_date.date()} to {params.to_date.date()}",
            from_date=params.from_date,
            to_date=params.to_date,
            initial_capital=Decimal(str(params.initial_capital)),
            lot_size=params.lot_size,
            signals_to_test=",".join(params.signals_to_test),
            use_hedging=params.use_hedging,
            hedge_offset=params.hedge_offset,
            commission_per_lot=Decimal(str(params.commission_per_lot)),
            slippage_percent=Decimal(str(params.slippage_percent)),
            status=BacktestStatus.PENDING
        )
        
        with self.db_manager.get_session() as session:
            session.add(backtest_run)
            session.commit()
            session.refresh(backtest_run)
        
        return backtest_run
    
    async def _ensure_data_available(self, from_date: datetime, to_date: datetime):
        """Ensure all required data is available"""
        # Add buffer for previous week data needed for zone calculation
        buffer_start = from_date - timedelta(days=7)
        
        # Ensure NIFTY data
        added = await self.data_collection.ensure_nifty_data_available(
            buffer_start, to_date
        )
        
        if added > 0:
            logger.info(f"Added {added} NIFTY data records")
        
        # Get all potential expiry dates in the period
        expiry_dates = self._get_expiry_dates(from_date, to_date)
        
        # Get required strikes (we'll fetch a range around ATM)
        # This is simplified - in production, you'd determine strikes dynamically
        strikes = list(range(15000, 30000, 50))  # Adjust based on NIFTY levels
        
        # Ensure options data for all expiries
        for expiry in expiry_dates:
            # Only fetch data up to expiry date
            end_date = min(expiry, to_date)
            added = await self.data_collection.ensure_options_data_available(
                from_date, end_date, strikes, [expiry]
            )
            
            if added > 0:
                logger.info(f"Added {added} options data records for expiry {expiry.date()}")
    
    def _get_expiry_dates(self, from_date: datetime, to_date: datetime) -> List[datetime]:
        """Get all Thursday expiry dates in the period"""
        expiry_dates = []
        current = from_date
        
        while current <= to_date:
            expiry = self.context_manager.get_next_expiry(current)
            if expiry <= to_date and expiry not in expiry_dates:
                expiry_dates.append(expiry)
            current = expiry + timedelta(days=1)
        
        return expiry_dates
    
    async def _run_backtest_logic(
        self,
        backtest_run: BacktestRun,
        nifty_data: List[NiftyIndexData],
        params: BacktestParameters
    ) -> Dict:
        """Main backtest logic"""
        # Initialize tracking variables
        current_capital = float(params.initial_capital)
        open_trades: List[BacktestTrade] = []
        all_trades: List[BacktestTrade] = []
        daily_results: List[BacktestDailyResult] = []
        
        # Track daily P&L
        current_date = None
        daily_starting_capital = current_capital
        
        # Process each hourly bar
        for i, data_point in enumerate(nifty_data):
            current_bar = self.context_manager.create_bar_from_nifty_data(data_point)
            
            # Skip non-market hours
            if not self.context_manager.is_market_hours(current_bar.timestamp):
                continue
            
            # Check for new day
            if current_date != current_bar.timestamp.date():
                # Save previous day's results
                if current_date:
                    daily_result = BacktestDailyResult(
                        backtest_run_id=backtest_run.id,
                        date=datetime.combine(current_date, datetime.min.time()),
                        starting_capital=Decimal(str(daily_starting_capital)),
                        ending_capital=Decimal(str(current_capital)),
                        daily_pnl=Decimal(str(current_capital - daily_starting_capital)),
                        daily_return_percent=Decimal(str(
                            ((current_capital - daily_starting_capital) / daily_starting_capital) * 100
                        )),
                        trades_opened=len([t for t in all_trades if t.entry_time.date() == current_date]),
                        trades_closed=len([t for t in all_trades if t.exit_time and t.exit_time.date() == current_date]),
                        open_positions=len(open_trades)
                    )
                    daily_results.append(daily_result)
                
                current_date = current_bar.timestamp.date()
                daily_starting_capital = current_capital
            
            # Get previous week data for context
            if i < 7 * 6:  # Need at least a week of data
                continue
            
            prev_week_data = self.context_manager.get_previous_week_data(
                current_bar.timestamp, nifty_data[:i]
            )
            
            if not prev_week_data:
                continue
            
            # Update weekly context
            context = self.context_manager.update_context(current_bar, prev_week_data)
            
            # Check for expiry and close positions
            expiry_pnl = await self._check_and_close_expiry_positions(
                open_trades, current_bar, backtest_run.id
            )
            current_capital += expiry_pnl
            
            # Check stop losses
            sl_pnl = await self._check_stop_losses(
                open_trades, current_bar, backtest_run.id
            )
            current_capital += sl_pnl
            
            # Evaluate signals (only if no open position)
            if not open_trades and not context.signal_triggered_this_week:
                signal_result = self.signal_evaluator.evaluate_all_signals(
                    current_bar, context
                )
                
                if signal_result.is_triggered and signal_result.signal_type.value in params.signals_to_test:
                    # Open new trade
                    trade = await self._open_trade(
                        backtest_run.id,
                        signal_result,
                        current_bar,
                        context,
                        params
                    )
                    
                    if trade:
                        open_trades.append(trade)
                        all_trades.append(trade)
                        
                        # Deduct margin/premium
                        position_cost = await self._calculate_position_cost(trade)
                        current_capital -= position_cost
        
        # Close any remaining open trades at end
        for trade in open_trades:
            if trade.outcome == TradeOutcome.OPEN:
                await self._close_trade(
                    trade, 
                    nifty_data[-1].timestamp,
                    float(nifty_data[-1].close),
                    TradeOutcome.EXPIRED,
                    "Backtest ended"
                )
        
        # Calculate final metrics
        total_trades = len(all_trades)
        winning_trades = len([t for t in all_trades if t.outcome == TradeOutcome.WIN])
        losing_trades = len([t for t in all_trades if t.outcome == TradeOutcome.LOSS])
        
        results = {
            'final_capital': current_capital,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': (winning_trades / total_trades * 100) if total_trades > 0 else 0,
            'total_pnl': current_capital - float(params.initial_capital),
            'total_return_percent': ((current_capital - float(params.initial_capital)) / float(params.initial_capital)) * 100,
            'trades': all_trades,
            'daily_results': daily_results
        }
        
        # Calculate max drawdown
        equity_curve = [float(params.initial_capital)]
        for daily in daily_results:
            equity_curve.append(float(daily.ending_capital))
        
        max_drawdown = self._calculate_max_drawdown(equity_curve)
        results['max_drawdown'] = max_drawdown['value']
        results['max_drawdown_percent'] = max_drawdown['percent']
        
        return results
    
    async def _open_trade(
        self,
        backtest_run_id: str,
        signal_result,
        current_bar: BarData,
        context,
        params: BacktestParameters
    ) -> Optional[BacktestTrade]:
        """Open a new trade based on signal"""
        try:
            # Get expiry for current week
            expiry = self.context_manager.get_expiry_for_week(context.current_week_start)
            
            # Get strikes
            main_strike, hedge_strike = self.option_pricing.get_option_strikes_for_signal(
                current_bar.close,
                signal_result.signal_type.value,
                params.hedge_offset
            )
            
            # Create trade record with zone information
            trade = BacktestTrade(
                backtest_run_id=backtest_run_id,
                week_start_date=context.current_week_start,
                signal_type=signal_result.signal_type.value,
                direction=signal_result.direction.value,
                entry_time=current_bar.timestamp,
                index_price_at_entry=Decimal(str(current_bar.close)),
                signal_trigger_price=Decimal(str(signal_result.entry_price)),
                stop_loss_price=Decimal(str(signal_result.stop_loss)),
                outcome=TradeOutcome.OPEN,
                # Zone information
                resistance_zone_top=Decimal(str(context.zones.upper_zone_top)),
                resistance_zone_bottom=Decimal(str(context.zones.upper_zone_bottom)),
                support_zone_top=Decimal(str(context.zones.lower_zone_top)),
                support_zone_bottom=Decimal(str(context.zones.lower_zone_bottom)),
                # Market bias
                bias_direction=context.bias.bias.value,
                bias_strength=Decimal(str(context.bias.strength)),
                # Weekly extremes
                weekly_max_high=Decimal(str(context.weekly_max_high)),
                weekly_min_low=Decimal(str(context.weekly_min_low)),
                # First bar details (if available)
                first_bar_open=Decimal(str(context.first_hour_bar.open)) if context.first_hour_bar else None,
                first_bar_close=Decimal(str(context.first_hour_bar.close)) if context.first_hour_bar else None,
                first_bar_high=Decimal(str(context.first_hour_bar.high)) if context.first_hour_bar else None,
                first_bar_low=Decimal(str(context.first_hour_bar.low)) if context.first_hour_bar else None,
                # Distance metrics
                distance_to_resistance=Decimal(str(context.bias.distance_to_resistance)),
                distance_to_support=Decimal(str(context.bias.distance_to_support))
            )
            
            # Get option prices and create positions
            option_type = signal_result.option_type
            
            # Main position (sell)
            main_price = await self.option_pricing.get_option_price_at_time(
                current_bar.timestamp, main_strike, option_type, expiry
            )
            
            if main_price:
                main_position = BacktestPosition(
                    trade_id=trade.id,
                    position_type="MAIN",
                    option_type=option_type,
                    strike_price=main_strike,
                    expiry_date=expiry,
                    entry_time=current_bar.timestamp,
                    entry_price=Decimal(str(main_price)),
                    quantity=-params.lot_size  # Negative for sell
                )
                trade.positions.append(main_position)
            
            # Hedge position (buy) if enabled
            if params.use_hedging:
                hedge_price = await self.option_pricing.get_option_price_at_time(
                    current_bar.timestamp, hedge_strike, option_type, expiry
                )
                
                if hedge_price:
                    hedge_position = BacktestPosition(
                        trade_id=trade.id,
                        position_type="HEDGE",
                        option_type=option_type,
                        strike_price=hedge_strike,
                        expiry_date=expiry,
                        entry_time=current_bar.timestamp,
                        entry_price=Decimal(str(hedge_price)),
                        quantity=params.lot_size  # Positive for buy
                    )
                    trade.positions.append(hedge_position)
            
            # Save trade to database
            with self.db_manager.get_session() as session:
                session.add(trade)
                session.commit()
                session.refresh(trade)
            
            logger.info(f"Opened trade: {signal_result.signal_type.value} at {current_bar.timestamp}")
            return trade
            
        except Exception as e:
            logger.error(f"Error opening trade: {e}")
            return None
    
    async def _check_and_close_expiry_positions(
        self,
        open_trades: List[BacktestTrade],
        current_bar: BarData,
        backtest_run_id: str
    ) -> float:
        """Check and close positions at expiry"""
        total_pnl = 0.0
        trades_to_remove = []
        
        for trade in open_trades:
            if trade.outcome != TradeOutcome.OPEN:
                continue
            
            # Check if any position has expired
            for position in trade.positions:
                if current_bar.timestamp >= position.expiry_date:
                    # Close at expiry
                    pnl = await self._close_trade(
                        trade,
                        current_bar.timestamp,
                        current_bar.close,
                        TradeOutcome.EXPIRED,
                        "Weekly expiry"
                    )
                    total_pnl += pnl
                    trades_to_remove.append(trade)
                    break
        
        # Remove closed trades
        for trade in trades_to_remove:
            if trade in open_trades:
                open_trades.remove(trade)
        
        return total_pnl
    
    async def _check_stop_losses(
        self,
        open_trades: List[BacktestTrade],
        current_bar: BarData,
        backtest_run_id: str
    ) -> float:
        """Check and trigger stop losses"""
        total_pnl = 0.0
        trades_to_remove = []
        
        for trade in open_trades:
            if trade.outcome != TradeOutcome.OPEN:
                continue
            
            # Check stop loss based on signal direction
            hit_stop_loss = False
            
            if trade.direction == "BULLISH":
                # For bullish trades (sold PUT), stop if price goes below strike
                if current_bar.close <= float(trade.stop_loss_price):
                    hit_stop_loss = True
            else:
                # For bearish trades (sold CALL), stop if price goes above strike
                if current_bar.close >= float(trade.stop_loss_price):
                    hit_stop_loss = True
            
            if hit_stop_loss:
                pnl = await self._close_trade(
                    trade,
                    current_bar.timestamp,
                    current_bar.close,
                    TradeOutcome.STOPPED,
                    "Stop loss hit"
                )
                total_pnl += pnl
                trades_to_remove.append(trade)
        
        # Remove closed trades
        for trade in trades_to_remove:
            if trade in open_trades:
                open_trades.remove(trade)
        
        return total_pnl
    
    async def _close_trade(
        self,
        trade: BacktestTrade,
        exit_time: datetime,
        index_price: float,
        outcome: TradeOutcome,
        reason: str
    ) -> float:
        """Close a trade and calculate P&L"""
        trade.exit_time = exit_time
        trade.index_price_at_exit = Decimal(str(index_price))
        trade.outcome = outcome
        trade.exit_reason = reason
        
        total_pnl = 0.0
        
        # Close all positions
        for position in trade.positions:
            # Get exit price
            exit_price = await self.option_pricing.get_option_price_at_time(
                exit_time,
                position.strike_price,
                position.option_type,
                position.expiry_date
            )
            
            if not exit_price:
                # If at expiry, calculate intrinsic value
                if exit_time >= position.expiry_date:
                    if position.option_type == "CE":
                        exit_price = max(0, index_price - position.strike_price)
                    else:  # PE
                        exit_price = max(0, position.strike_price - index_price)
                else:
                    exit_price = 0
            
            position.exit_time = exit_time
            position.exit_price = Decimal(str(exit_price))
            
            # Calculate P&L
            if position.quantity < 0:  # Sold option
                position.gross_pnl = abs(position.quantity) * (position.entry_price - position.exit_price)
            else:  # Bought option
                position.gross_pnl = position.quantity * (position.exit_price - position.entry_price)
            
            # Commission (entry + exit)
            lots = abs(position.quantity) // 50
            position.commission = Decimal(str(lots * 40 * 2))
            position.net_pnl = position.gross_pnl - position.commission
            
            total_pnl += float(position.net_pnl)
        
        trade.total_pnl = Decimal(str(total_pnl))
        
        # Determine if win or loss
        if total_pnl > 0:
            trade.outcome = TradeOutcome.WIN
        elif total_pnl < 0:
            trade.outcome = TradeOutcome.LOSS
        
        # Update in database
        with self.db_manager.get_session() as session:
            session.merge(trade)
            session.commit()
        
        logger.info(f"Closed trade: {trade.signal_type} - {outcome.value} - P&L: {total_pnl:.2f}")
        return total_pnl
    
    async def _calculate_position_cost(self, trade: BacktestTrade) -> float:
        """Calculate initial cost/margin for positions"""
        total_cost = 0.0
        
        for position in trade.positions:
            if position.quantity > 0:  # Bought option
                # Cost is premium paid
                total_cost += float(position.entry_price) * position.quantity
            # For sold options, margin is handled separately in real trading
            # For backtest, we assume sufficient margin
        
        return total_cost
    
    def _calculate_max_drawdown(self, equity_curve: List[float]) -> Dict:
        """Calculate maximum drawdown from equity curve"""
        if not equity_curve:
            return {'value': 0, 'percent': 0}
        
        peak = equity_curve[0]
        max_dd = 0
        max_dd_pct = 0
        
        for value in equity_curve:
            if value > peak:
                peak = value
            
            drawdown = peak - value
            drawdown_pct = (drawdown / peak) * 100 if peak > 0 else 0
            
            if drawdown > max_dd:
                max_dd = drawdown
                max_dd_pct = drawdown_pct
        
        return {'value': max_dd, 'percent': max_dd_pct}
    
    async def _update_backtest_status(
        self,
        backtest_run_id: str,
        status: BacktestStatus,
        error_message: str = None
    ):
        """Update backtest run status"""
        with self.db_manager.get_session() as session:
            backtest_run = session.query(BacktestRun).filter_by(id=backtest_run_id).first()
            if backtest_run:
                backtest_run.status = status
                if status == BacktestStatus.RUNNING:
                    backtest_run.started_at = datetime.now()
                elif status == BacktestStatus.COMPLETED:
                    backtest_run.completed_at = datetime.now()
                if error_message:
                    backtest_run.error_message = error_message
                session.commit()
    
    async def _update_backtest_results(self, backtest_run_id: str, results: Dict):
        """Update backtest run with final results"""
        with self.db_manager.get_session() as session:
            backtest_run = session.query(BacktestRun).filter_by(id=backtest_run_id).first()
            if backtest_run:
                backtest_run.final_capital = Decimal(str(results['final_capital']))
                backtest_run.total_trades = results['total_trades']
                backtest_run.winning_trades = results['winning_trades']
                backtest_run.losing_trades = results['losing_trades']
                backtest_run.win_rate = Decimal(str(results['win_rate']))
                backtest_run.total_pnl = Decimal(str(results['total_pnl']))
                backtest_run.total_return_percent = Decimal(str(results['total_return_percent']))
                backtest_run.max_drawdown = Decimal(str(results['max_drawdown']))
                backtest_run.max_drawdown_percent = Decimal(str(results['max_drawdown_percent']))
                
                # Save daily results
                for daily_result in results['daily_results']:
                    session.add(daily_result)
                
                session.commit()