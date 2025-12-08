import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)
        
        # Create directories if they don't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def validate_data(self, df):
        """Validate data before loading"""
        if df.empty:
            raise ValueError("Cannot load empty dataframe")
        logger.info(f"Validated {len(df)} records for loading")
        return True
    
    def load(self, df, file_name, index=False):
        """Load data to CSV file"""
        try:
            self.validate_data(df)
            
            file_path = self.output_dir / file_name
            
            # Save to CSV (overwrite existing file)
            df.to_csv(file_path, index=index)
            logger.info(f"Data loaded successfully to {file_path}")
            
            return file_path
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
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
        logger.info("="*50)
        logger.info(f"Generating Top {top_n} Clients Report")
        logger.info("="*50)
        
        try:
            from src.analytics import DataAnalytics
            
            # Calculate analytics
            analytics = DataAnalytics()
            analytics_results = analytics.get_top_clients(df, top_n=top_n)
            
            if analytics_results is None:
                logger.error("No analytics results to save")
                return None, None
            
            # Save top by volume
            volume_path = self.load(analytics_results['top_by_volume'], volume_filename)
            
            # Save top by PnL
            pnl_path = self.load(analytics_results['top_by_pnl'], pnl_filename)
            
            logger.info(f"Analytics saved: Volume={volume_path}, PnL={pnl_path}")
            
            return volume_path, pnl_path
            
        except Exception as e:
            logger.error(f"Error loading analytics: {str(e)}")
            raise