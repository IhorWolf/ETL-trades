from pathlib import Path
from config.config import Config
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader

def main():
    """Main ETL pipeline"""
    try:
        # Extract
        input_path = Path(Config.INPUT_DIR) / Config.INPUT_FILE
        extractor = DataExtractor(input_path)
        df = extractor.extract()
        
        # Transform
        transformer = DataTransformer(date_format=Config.DATE_FORMAT)
        agg_df, client_pnl_df = transformer.transform(df)
        
        # Load
        loader = DataLoader(output_dir=Config.OUTPUT_DIR)
        
        # 1. Load aggregated trades weekly
        loader.load(agg_df, Config.OUTPUT_FILE)
        
        # 2. Load all analytics (top volume + top pnl)
        loader.load_all_analytics(agg_df, client_pnl_df, Config)
        
        print("ETL completed successfully")
        
    except Exception as e:
        print(f"ETL process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()