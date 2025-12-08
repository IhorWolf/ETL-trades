"""
Analytics module for generating reports and insights from ETL data.
"""
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class DataAnalytics:
    
    def get_top_clients(self, df, top_n=3):
        """
        Calculate top N bronze clients by volume and PnL.
        
        Args:
            df: DataFrame with aggregated trade data
            top_n: Number of top clients to return (default 3)
        
        Returns:
            Dictionary with DataFrames for top by volume and top by PnL
        """
        logger.info(f"Calculating top {top_n} bronze clients...")
        
        # Filter for bronze clients only
        bronze_df = df[df['client_type'] == 'bronze'].copy()
        
        if bronze_df.empty:
            logger.warning("No bronze clients found in data")
            return None
        
        # Aggregate by user_id to get total volume and PnL per client
        client_summary = bronze_df.groupby('user_id').agg({
            'total_volume': 'sum',
            'closed_pnl': 'sum',
        }).reset_index()
        
        # Add client_type back
        client_summary['client_type'] = 'bronze'

        # Top N by volume
        top_by_volume = client_summary.nlargest(top_n, 'total_volume').copy()
        top_by_volume['rank'] = range(1, len(top_by_volume) + 1)
        top_by_volume = top_by_volume[['rank', 'user_id', 'client_type', 'total_volume', 'closed_pnl']]
        
        # Top N by PnL 
        top_by_pnl = client_summary.nlargest(top_n, 'closed_pnl').copy()
        top_by_pnl['rank'] = range(1, len(top_by_pnl) + 1)
        top_by_pnl = top_by_pnl[['rank', 'user_id', 'client_type', 'closed_pnl', 'total_volume']]
        
        return {
            'top_by_volume': top_by_volume,
            'top_by_pnl': top_by_pnl
        }
    
    def save_to_csv(self, results, volume_filename='top_bronze_by_volume.csv', pnl_filename='top_bronze_by_pnl.csv'):
        """
        Save analytics results to separate CSV files.
        
        Args:
            results: Dictionary with DataFrames from get_top_clients
            volume_filename: CSV filename for top by volume
            pnl_filename: CSV filename for top by PnL
        
        Returns:
            Tuple of (volume_path, pnl_path)
        """
        if results is None:
            logger.error("No results to save")
            return None, None
        
        try:
            # Save top by volume
            volume_path = self.output_dir / volume_filename
            results['top_by_volume'].to_csv(volume_path, index=False)
            logger.info(f"Top by volume saved to {volume_path}")
            
            # Save top by PnL
            pnl_path = self.output_dir / pnl_filename
            results['top_by_pnl'].to_csv(pnl_path, index=False)
            logger.info(f"Top by PnL saved to {pnl_path}")
            
            return volume_path, pnl_path
            
        except Exception as e:
            logger.error(f"Error saving CSV files: {str(e)}")
            raise
    
    def generate_top_clients_report(self, df, top_n=3, volume_filename='top_clients_by_volume.csv', pnl_filename='top_clients_by_pnl.csv'):
        """
        Generate complete top bronze clients report and save to CSV files.
        
        Args:
            df: DataFrame with aggregated trade data
            top_n: Number of top clients to include
            volume_filename: CSV filename for top by volume
            pnl_filename: CSV filename for top by PnL
        
        Returns:
            Tuple of (volume_path, pnl_path)
        """
        results = self.get_top_clients(df, top_n)
        return self.save_to_csv(results, volume_filename, pnl_filename)


# Standalone function for easy use
def generate_top_clients_report(df, output_dir, top_n=3, volume_filename='top_clients_by_volume.csv', pnl_filename='top_clients_by_pnl.csv'):
    """
    Generate top bronze clients report with top N by volume and PnL.
    
    Args:
        df: DataFrame with aggregated trade data
        output_dir: Directory to save the CSV files
        top_n: Number of top clients (default 3)
        volume_filename: CSV filename for top by volume
        pnl_filename: CSV filename for top by PnL
    
    Returns:
        Tuple of (volume_path, pnl_path)
    """
    analytics = DataAnalytics(output_dir)
    return analytics.generate_top_clients_report(df, top_n, volume_filename, pnl_filename)