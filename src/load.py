import pandas as pd
from pathlib import Path
from src.analytics import TopClientsAnalytics


class DataLoader:
    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def validate_data(self, df):
        """Validate data before loading"""
        if df.empty:
            raise ValueError("Cannot load empty dataframe")
        return True
    
    def load(self, df, file_name, index=False):
        """Load data to CSV file"""
        self.validate_data(df)
        file_path = self.output_dir / file_name
        df.to_csv(file_path, index=index)
        return file_path
    
    def load_top_clients_analytics(self, df, volume_filename, pnl_filename, top_n=3):
        """
        Generate and load top bronze clients analytics.
        
        Args:
            df: Transformed DataFrame with aggregated trade data
            volume_filename: CSV filename for top by volume
            pnl_filename: CSV filename for top by PnL
            top_n: Number of top clients (default 3)
        
        Returns:
            Tuple of (volume_path, pnl_path)
        """
        # Calculate analytics
        analytics = TopClientsAnalytics(df)
        
        # Get top clients
        top_by_volume = analytics.get_top_clients_by_volume(top_n)
        top_by_pnl = analytics.get_top_clients_by_pnl(top_n)
        
        if top_by_volume is None or top_by_pnl is None:
            return None, None
        
        # Save top by volume
        volume_path = self.load(top_by_volume, volume_filename)
        
        # Save top by PnL
        pnl_path = self.load(top_by_pnl, pnl_filename)
                    
        return volume_path, pnl_path