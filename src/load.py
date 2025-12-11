import pandas as pd
from pathlib import Path


class DataLoader:
    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def validate_data(self, df):
        """Validate data before loading"""
        if df is None or df.empty:
            raise ValueError("Cannot load empty or None dataframe")
        return True
    
    def load(self, df, file_name, index=False):
        """Load data to CSV file"""
        self.validate_data(df)
        file_path = self.output_dir / file_name
        df.to_csv(file_path, index=index)
        print(f"Saved {file_name} to {file_path}")
        return file_path
    
    def load_all_analytics(self, agg_df, client_pnl_df, config, client_type='bronze', top_n=3):
        """
        Calculate and load all analytics outputs.
        
        Args:
            agg_df: Aggregated trades DataFrame
            client_pnl_df: Client PnL DataFrame
            config: Config object with file names
            client_type: Type of client to filter (default: 'bronze')
            top_n: Number of top clients to return (default: 3)
        
        Returns:
            dict with output paths
        """
        paths = {}
        
        # Top clients by volume
        filtered = agg_df[agg_df['client_type'] == client_type]
        top_volume = filtered.groupby('user_id').agg({
            'trade_volume': 'sum',
            'trade_count': 'sum'
        }).reset_index()
        top_volume = top_volume.nlargest(top_n, 'trade_volume')
        top_volume['trade_volume'] = top_volume['trade_volume'].round(2)
        paths['volume'] = self.load(top_volume, config.TOP_CLIENTS_VOLUME_FILE)
        
        # Top clients by PnL
        filtered_pnl = client_pnl_df[client_pnl_df['client_type'] == client_type]
        top_pnl = filtered_pnl.nlargest(top_n, 'total_pnl')[
            ['user_id', 'unrealized_pnl', 'realized_pnl', 'total_pnl']
        ].reset_index(drop=True)
        paths['pnl'] = self.load(top_pnl, config.TOP_CLIENTS_PNL_FILE)
        
        return paths