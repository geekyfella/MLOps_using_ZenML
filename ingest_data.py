import logging
from zenml import step
import pandas as pd

class IngestData:
    def __init__(self, file_path: str):
        self.file_path = file_path  
        
    def get_data(self):
        logging.info(f"Reading data from {self.file_path}")
        return pd.read_csv(self.file_path)
    
@step
def data_ingestion_step(data_path:str) -> pd.DataFrame:
    """
    Ingesting data from the data path
    
    Args:
        data_path (str): path to the data file

    Returns:
        pd.DataFrame: the ingested data
    """
    try:
        ingest_data = IngestData(file_path=data_path)
        df = ingest_data.get_data()
        return df
    except Exception as e:
        logging.error(f"Error in data ingestion: {e}")