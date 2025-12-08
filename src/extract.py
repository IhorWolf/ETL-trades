import pandas as pd
from pathlib import Path

class DataExtractor:
    def __init__(self, input_path):
        self.input_path = Path(input_path)
    
    def extract(self):
        """Extract data from CSV file"""
        if not self.input_path.exists():
            raise FileNotFoundError(f"Input file not found: {self.input_path}")
        
        df = pd.read_csv(self.input_path)
        return df