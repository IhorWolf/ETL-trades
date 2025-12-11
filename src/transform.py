'''
Transform data according to business logic
1. Convert timestamp to weekly date in Monday format
2. Aggregate by weekly_date, user_id, client_type, symbol and sum total volume and count trades, cumulative volume
3. Calculate FIFO PnL for closed positions only
4. Calculate total PnL using weighted average prices
'''
import pandas as pd
from collections import deque


class DataTransformer:
    def __init__(self, date_format='%Y-%m-%d'):
        self.date_format = date_format

    def clean_data(self, df):
        """Clean and validate data"""
        df = df.drop_duplicates()
        df = df.dropna(subset=['timestamp', 'user_id', 'symbol', 'side'])
        return df

    def parse_dates(self, df):
        """Parse timestamp column"""
        df['timestamp'] = pd.to_datetime(
            df['timestamp'],
            format=self.date_format,
            errors='coerce'
        )
        return df

    def create_weekly_date(self, df):
        """Create weekly start date (Monday)"""
        df['weekly_start_date'] = (
            df['timestamp']
            .dt.to_period('W-MON')
            .dt.to_timestamp()
            .dt.date
        )
        return df

    def calculate_fifo_pnl(self, df):
        """
        Calculate FIFO PnL for each week.
        
        Returns DataFrame with:
        - unrealized_pnl: PnL from open positions at week's last price
        - realized_pnl: PnL from closed positions in that week
        - total_pnl: realized_pnl + unrealized_pnl
        """
        df_sorted = df.sort_values(['user_id', 'client_type', 'symbol', 'timestamp']).copy()
        
        df_for_prices = df.sort_values('timestamp')
        last_prices = df_for_prices.groupby(['weekly_start_date', 'symbol'], as_index=False).agg(
            last_price=('price', 'last')
        )
        
        results = []
        
        for (user_id, client_type, symbol), group in df_sorted.groupby(['user_id', 'client_type', 'symbol']):
            open_positions = deque()
            
            for week, week_data in group.groupby('weekly_start_date'):
                week_realized_pnl = 0.0
                
                last_price = last_prices[
                    (last_prices['weekly_start_date'] == week) & 
                    (last_prices['symbol'] == symbol)
                ]['last_price'].iloc[0]
                
                for _, trade in week_data.iterrows():
                    trade_qty = trade['quantity']
                    trade_price = trade['price']
                    trade_side = trade['side']
                    trade_direction = 1 if trade_side == 'buy' else -1
                    remaining_qty = trade_qty
                    
                    while remaining_qty > 0 and open_positions:
                        pos_qty, pos_price, pos_direction = open_positions[0]
                        
                        if pos_direction != trade_direction:
                            close_qty = min(remaining_qty, pos_qty)
                            
                            if pos_direction == 1:
                                pnl = (trade_price - pos_price) * close_qty
                            else:
                                pnl = (pos_price - trade_price) * close_qty
                            
                            week_realized_pnl += pnl
                            remaining_qty -= close_qty
                            
                            if close_qty >= pos_qty:
                                open_positions.popleft()
                            else:
                                open_positions[0] = (pos_qty - close_qty, pos_price, pos_direction)
                        else:
                            break
                    
                    if remaining_qty > 0:
                        open_positions.append((remaining_qty, trade_price, trade_direction))
                
                week_unrealized_pnl = 0.0
                for pos_qty, pos_price, pos_direction in open_positions:
                    if pos_direction == 1:
                        unrealized = (last_price - pos_price) * pos_qty
                    else:
                        unrealized = (pos_price - last_price) * pos_qty
                    week_unrealized_pnl += unrealized
                
                week_total_pnl = week_realized_pnl + week_unrealized_pnl
                
                results.append({
                    'weekly_start_date': week,
                    'user_id': user_id,
                    'client_type': client_type,
                    'symbol': symbol,
                    'unrealized_pnl': round(week_unrealized_pnl, 2),
                    'realized_pnl': round(week_realized_pnl, 2),
                    'total_pnl': round(week_total_pnl, 2)
                })
        
        return pd.DataFrame(results)
    
    def calculate_client_pnl(self, df):
        """
        Calculate realized and unrealized PnL using weighted average prices.
        
        Returns DataFrame with:
        - unrealized_pnl: PnL from open positions at last price
        - realized_pnl: PnL from closed positions
        - total_pnl: unrealized_pnl + realized_pnl
        """
        df = df.copy()
        
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        
        df = df.dropna(subset=['timestamp'])
        
        df_sorted = df.sort_values('timestamp')
        last_trades = df_sorted.groupby('symbol').agg(
            last_price=('price', 'last')
        ).reset_index()
        
        last_prices = last_trades[['symbol', 'last_price']]
        
        df['volume'] = df['quantity'] * df['price']
        
        agg_df = df.groupby(['user_id', 'client_type', 'symbol', 'side']).agg({
            'quantity': 'sum',
            'volume': 'sum'
        }).reset_index()
        
        agg_df['avg_price'] = agg_df['volume'] / agg_df['quantity']
        
        buy_df = agg_df[agg_df['side'] == 'buy'][['user_id', 'client_type', 'symbol', 'quantity', 'avg_price', 'volume']].copy()
        buy_df.columns = ['user_id', 'client_type', 'symbol', 'buy_quantity', 'avg_buy_price', 'buy_volume']
        
        sell_df = agg_df[agg_df['side'] == 'sell'][['user_id', 'client_type', 'symbol', 'quantity', 'avg_price', 'volume']].copy()
        sell_df.columns = ['user_id', 'client_type', 'symbol', 'sell_quantity', 'avg_sell_price', 'sell_volume']
        
        result = pd.merge(
            buy_df, 
            sell_df, 
            on=['user_id', 'client_type', 'symbol'], 
            how='outer'
        )
        result = result.fillna(0)
        
        result['realized_quantity'] = result[['buy_quantity', 'sell_quantity']].min(axis=1)
        mask_has_both = (result['buy_quantity'] > 0) & (result['sell_quantity'] > 0)
        result['realized_pnl'] = 0.0
        result.loc[mask_has_both, 'realized_pnl'] = (
            (result.loc[mask_has_both, 'avg_sell_price'] - result.loc[mask_has_both, 'avg_buy_price']) * 
            result.loc[mask_has_both, 'realized_quantity']
        )
        
        result['net_position'] = result['buy_quantity'] - result['sell_quantity']
        result['net_volume'] = result['buy_volume'] - result['sell_volume']
        result['avg_net_price'] = abs(result['net_volume']) / abs(result['net_position'])
        
        result = result.merge(last_prices, on='symbol', how='left')
        
        result['unrealized_pnl'] = 0.0
        
        long_mask = result['net_position'] > 0
        result.loc[long_mask, 'unrealized_pnl'] = (
            (result.loc[long_mask, 'last_price'] - result.loc[long_mask, 'avg_net_price']) 
            * result.loc[long_mask, 'net_position']
        )
        

        short_mask = result['net_position'] < 0
        result.loc[short_mask, 'unrealized_pnl'] = (
            (result.loc[short_mask, 'avg_net_price'] - result.loc[short_mask, 'last_price']) 
            * abs(result.loc[short_mask, 'net_position']) 
        )
    
        result['total_pnl'] = result['realized_pnl'] + result['unrealized_pnl']
        
        user_pnl = result.groupby(['user_id', 'client_type']).agg({
            'unrealized_pnl': 'sum',
            'realized_pnl': 'sum',
            'total_pnl': 'sum'
        }).reset_index()
        
        user_pnl['unrealized_pnl'] = user_pnl['unrealized_pnl'].round(2)
        user_pnl['realized_pnl'] = user_pnl['realized_pnl'].round(2)
        user_pnl['total_pnl'] = user_pnl['total_pnl'].round(2)
        
        return user_pnl[['user_id', 'client_type', 'unrealized_pnl', 'realized_pnl', 'total_pnl']]

    def aggregate_trades(self, df):
        """Aggregate trades by weekly_start_date, user_id, client_type, symbol"""
        groupby_cols = ['weekly_start_date', 'user_id', 'client_type', 'symbol']

        df_temp = df.copy()
        df_temp['trade_volume'] = df_temp['quantity'] * df_temp['price']
        
        agg_df = df_temp.groupby(groupby_cols).agg(
            trade_volume=('trade_volume', 'sum'),
            trade_count=('trade_volume', 'count')
        ).reset_index()
        
        agg_df['trade_volume'] = agg_df['trade_volume'].round(2)
        
        agg_df['cumulative_trade_volume'] = agg_df.groupby(['user_id', 'client_type'])['trade_volume'].cumsum()
        agg_df['cumulative_trade_volume'] = agg_df['cumulative_trade_volume'].round(2)
        
        return agg_df

    def transform(self, df):
        """Main transformation pipeline"""
        df_cleaned = self.clean_data(df)
        
        client_pnl_df = self.calculate_client_pnl(df_cleaned)
        
        df_weekly = self.parse_dates(df_cleaned.copy())
        df_weekly = self.create_weekly_date(df_weekly)
        
        agg_df = self.aggregate_trades(df_weekly)
        pnl_df = self.calculate_fifo_pnl(df_weekly)
        
        result = agg_df.merge(
            pnl_df,
            on=['weekly_start_date', 'user_id', 'client_type', 'symbol'],
            how='left'
        )
        
        result['realized_pnl'] = result['realized_pnl'].fillna(0)
        result['unrealized_pnl'] = result['unrealized_pnl'].fillna(0)
        result['total_pnl'] = result['total_pnl'].fillna(0)
        
        return result, client_pnl_df


def transform_data(df):
    """Transform data using DataTransformer class"""
    transformer = DataTransformer()
    return transformer.transform(df)