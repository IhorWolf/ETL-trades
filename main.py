import logging
from pathlib import Path
from datetime import datetime
from config.config import Config
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader

def setup_logging():
    """Setup logging configuration"""
    log_dir = Path(Config.LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / f"etl_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def main():
    """Main ETL pipeline"""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("="*50)
        logger.info("Starting ETL process")
        logger.info("="*50)
        
        # Extract
        input_path = Path(Config.INPUT_DIR) / Config.INPUT_FILE
        extractor = DataExtractor(input_path)
        df = extractor.extract()
        
        # Transform
        transformer = DataTransformer(date_format=Config.DATE_FORMAT)
        transformed_df = transformer.transform(df)
        
        # Load
        loader = DataLoader(output_dir=Config.OUTPUT_DIR)
        
        # Load main ETL output
        output_path = loader.load(transformed_df, Config.OUTPUT_FILE)
        
        # Load analytics output
        volume_path, pnl_path = loader.load_top_clients_analytics(
            transformed_df,
            Config.TOP_CLIENTS_VOLUME_FILE,
            Config.TOP_CLIENTS_PNL_FILE,
            top_n=3
        )
        
        logger.info("="*50)
        logger.info("ETL process completed successfully")
        logger.info(f"Main CSV Output: {output_path}")
        logger.info(f"Top 3 by Volume: {volume_path}")
        logger.info(f"Top 3 by PnL: {pnl_path}")
        logger.info("="*50)
        
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    setup_logging()
    main()