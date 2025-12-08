import os

class Config:
    # Paths
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    INPUT_DIR = os.path.join(BASE_DIR, 'data', 'input')
    OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'output')
    LOG_DIR = os.path.join(BASE_DIR, 'logs')
    
    # File names
    INPUT_FILE = 'trades.csv'
    OUTPUT_FILE = 'agg_trades_weekly.csv'
    TOP_CLIENTS_VOLUME_FILE = 'top_clients_by_volume.csv'
    TOP_CLIENTS_PNL_FILE = 'top_clients_by_pnl.csv'
    
    # Processing settings
    CHUNK_SIZE = 10000  # For large files
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'