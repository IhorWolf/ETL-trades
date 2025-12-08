import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class DataExtractor:
    def __init__(self, file_path):
        self.file_path = Path(file_path)
        
    def validate_file(self):
        """Validate file exists and is readable"""
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")
        if not self.file_path.is_file():
            raise ValueError(f"Path is not a file: {self.file_path}")
        return True
    
    def extract(self, chunk_size=None):
        """Extract data from CSV file"""
        try:
            self.validate_file()
            logger.info(f"Extracting data from {self.file_path}")
            
            if chunk_size:
                # For large files, process in chunks
                return pd.read_csv(self.file_path, chunksize=chunk_size)
            else:
                df = pd.read_csv(self.file_path)
                logger.info(f"Extracted {len(df)} records")
                return df
                
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise