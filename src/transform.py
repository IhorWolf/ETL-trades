'''
Transform data according to business logic
1. Convert timestamp to weekly date in Monday format
2. Aggregate by weekly_date, user_id, client_type, symbol and sum total volume and count trades
3. Calculate FIFO PnL for closed positions only
'''
import pandas as pd
import logging
from collections import deque

logger = logging.getLogger(__name__)


class DataTransformer:
    def __init__(self, date_format='%Y-%m-%d %H:%M:%S'):
        self.date_format = date_format

    def clean_data(self, df):
        """Clean and validate data"""
        logger.info("Cleaning data...")
        initial_count = len(df)
        df = df.drop_duplicates()
        logger.info(f"Removed {initial_count - len(df)} duplicate records")
        df = df.dropna(subset=['timestamp', 'user_id', 'symbol', 'side'])
        return df

    def parse_dates(self, df):
        """Parse timestamp column"""
        logger.info("Parsing dates...")
        df['timestamp'] = pd.to_datetime(
            df['timestamp'],
            format=self.date_format,
            errors='coerce'
        )
        invalid_dates = df['timestamp'].isna().sum()
        if invalid_dates > 0:
            logger.warning(f"Found {invalid_dates} invalid timestamps")
        df = df.dropna(subset=['timestamp'])
        return df

    def create_weekly_date(self, df):
        """Create weekly start date (Monday)"""
        logger.info("Creating weekly start dates...")
        df['weekly_start_date'] = (
            df['timestamp']
            .dt.to_period('W-MON')
            .dt.to_timestamp()
            .dt.date
        )
        return df

    def calculate_fifo_pnl(self, df):
        """
        Calculate FIFO PnL for closed positions only.
        Tracks positions across weeks and calculates PnL only when positions are closed.
        
        Returns DataFrame with:
        - closed_pnl: PnL from closed positions in that week
        - closed_qty: Quantity of positions closed in that week
        - opened_qty: Quantity of new positions opened in that week
        - open_position: Net open position at end of week (+ = long, - = short)
        """
        logger.info("Calculating FIFO PnL for closed positions...")
        
        df_sorted = df.sort_values(['user_id', 'client_type', 'symbol', 'timestamp']).copy()
        results = []
        
        for (user_id, client_type, symbol), group in df_sorted.groupby(['user_id', 'client_type', 'symbol']):
            # Queue to track open positions: (qty, price, direction)
            # direction: 1 = long (buy), -1 = short (sell)
            open_positions = deque()
            
            for week, week_data in group.groupby('weekly_start_date'):
                week_closed_pnl = 0.0
                week_closed_qty = 0.0
                week_opened_qty = 0.0
                
                for _, trade in week_data.iterrows():
                    trade_qty = trade['quantity']
                    trade_price = trade['price']
                    trade_side = trade['side']
                    trade_direction = 1 if trade_side == 'buy' else -1
                    remaining_qty = trade_qty
                    
                    # Try to close existing positions (FIFO)
                    while remaining_qty > 0 and open_positions:
                        pos_qty, pos_price, pos_direction = open_positions[0]
                        
                        # Check if this trade closes the position (opposite direction)
                        if pos_direction != trade_direction:
                            close_qty = min(remaining_qty, pos_qty)
                            
                            # Calculate PnL for closed portion
                            if pos_direction == 1:  # Closing long position (sold)
                                pnl = (trade_price - pos_price) * close_qty
                            else:  # Closing short position (bought back)
                                pnl = (pos_price - trade_price) * close_qty
                            
                            week_closed_pnl += pnl
                            week_closed_qty += close_qty
                            remaining_qty -= close_qty
                            
                            # Update or remove the position from queue
                            if close_qty >= pos_qty:
                                open_positions.popleft()
                            else:
                                open_positions[0] = (pos_qty - close_qty, pos_price, pos_direction)
                        else:
                            # Same direction, can't close - break to add new position
                            break
                    
                    # Add remaining quantity as new open position
                    if remaining_qty > 0:
                        open_positions.append((remaining_qty, trade_price, trade_direction))
                        week_opened_qty += remaining_qty
                
                # Calculate total open position (signed: + = long, - = short)
                total_open = sum(q * d for q, p, d in open_positions)
                
                results.append({
                    'weekly_start_date': week,
                    'user_id': user_id,
                    'client_type': client_type,
                    'symbol': symbol,
                    'closed_pnl': round(week_closed_pnl, 2),
                    'closed_qty': round(week_closed_qty, 2),
                    'opened_qty': round(week_opened_qty, 2),
                    'open_position': round(total_open, 2)
                })
        
        logger.info(f"Calculated PnL for {len(results)} week/symbol combinations")
        return pd.DataFrame(results)

    def aggregate_trades(self, df):
        """Aggregate trades by weekly_start_date, user_id, client_type, symbol"""
        logger.info("Aggregating trades...")
        
        groupby_cols = ['weekly_start_date', 'user_id', 'client_type', 'symbol']
        
        agg_df = df.groupby(groupby_cols).agg({
            'quantity': ['sum', 'count']        
            }).reset_index()
        
        agg_df.columns = [
            'weekly_start_date', 'user_id', 'client_type', 'symbol',
            'total_volume', 'trade_count'
        ]
        
        return agg_df

    def transform(self, df):
        """Main transformation pipeline"""
        try:
            logger.info("Starting transformation...")
            
            # Clean and prepare data
            df = self.clean_data(df)
            df = self.parse_dates(df)
            df = self.create_weekly_date(df)
            
            # Get volume aggregations
            agg_df = self.aggregate_trades(df)
            
            # Get FIFO PnL calculations
            pnl_df = self.calculate_fifo_pnl(df)
            
            # Merge results
            result = agg_df.merge(
                pnl_df,
                on=['weekly_start_date', 'user_id', 'client_type', 'symbol'],
                how='left'
            )
            
            # Fill any missing PnL values with 0
            result['closed_pnl'] = result['closed_pnl'].fillna(0)
            result['closed_qty'] = result['closed_qty'].fillna(0)
            result['opened_qty'] = result['opened_qty'].fillna(0)
            result['open_position'] = result['open_position'].fillna(0)
            
            logger.info(f"Transformation complete. Output: {len(result)} records")
            return result
            
        except Exception as e:
            logger.error(f"Error during transformation: {str(e)}")
            raise


# Standalone function for backward compatibility
def transform_data(df):
    """Transform data using DataTransformer class"""
    transformer = DataTransformer()
    return transformer.transform(df)