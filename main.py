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
        transformed_df = transformer.transform(df)
        
        # Load
        loader = DataLoader(output_dir=Config.OUTPUT_DIR)
        
        # Load main ETL output
        loader.load(transformed_df, Config.OUTPUT_FILE)
        
        # Load analytics output
        loader.load_top_clients_analytics(
            transformed_df,
            Config.TOP_CLIENTS_VOLUME_FILE,
            Config.TOP_CLIENTS_PNL_FILE,
            top_n=3
        )
        
        print("ETL completed successfully")
        
    except Exception as e:
        print(f"ETL process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()