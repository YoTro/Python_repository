from __future__ import annotations
import csv
import os
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

class CSVHelper:
    """
    Utility class for reading and writing CSV/Excel data.
    """

    @staticmethod
    def save_to_csv(data: List[Dict], file_path: str):
        """
        Save a list of dictionaries to a CSV file.
        """
        if not data:
            logger.warning("No data to save.")
            return

        keys = data[0].keys()
        
        # Ensure directory exists
        dir_path = os.path.dirname(file_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as output_file:
                dict_writer = csv.DictWriter(output_file, fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(data)
            logger.info(f"Successfully saved {len(data)} rows to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save CSV: {e}")

    @staticmethod
    def read_csv(file_path: str) -> List[Dict]:
        """
        Read all rows from a CSV file into a list of dictionaries.
        Supports multiple encodings.
        """
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return []

        encodings = ['utf-8', 'gbk', 'latin1']
        for encoding in encodings:
            try:
                data = []
                with open(file_path, 'r', encoding=encoding) as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        data.append(row)
                logger.info(f"Successfully read CSV with {encoding} encoding.")
                return data
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Failed to read CSV with {encoding}: {e}")
                
        logger.error(f"Failed to read CSV {file_path} after trying all encodings.")
        return []

    @staticmethod
    def read_asins_from_csv(file_path: str, column_name: str = "ASIN") -> List[str]:
        """
        Read a list of ASINs from a CSV file.
        """
        asins = []
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return []

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if column_name in row:
                        asins.append(row[column_name])
            return asins
        except Exception as e:
            logger.error(f"Failed to read ASINs: {e}")
            return []
