import os
import pytest
import tempfile
import csv
import json
from unittest.mock import patch, mock_open

from src.core.utils.csv_helper import CSVHelper
from src.core.utils.parser_helper import parse_price, parse_rating, parse_integer
from src.core.utils.config_helper import ConfigHelper

def test_csv_helper_save_and_read():
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "test.csv")
        data = [{"ASIN": "123", "Title": "A"}, {"ASIN": "456", "Title": "B"}]
        
        CSVHelper.save_to_csv(data, file_path)
        assert os.path.exists(file_path)
        
        read_data = CSVHelper.read_csv(file_path)
        assert len(read_data) == 2
        assert read_data[0]["ASIN"] == "123"

def test_csv_helper_read_asins():
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "test.csv")
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["ASIN", "Other"])
            writer.writeheader()
            writer.writerow({"ASIN": "X1", "Other": "Y1"})
        
        asins = CSVHelper.read_asins_from_csv(file_path)
        assert asins == ["X1"]

def test_parser_helper():
    assert parse_price("$19.99") == 19.99
    assert parse_price("1,234.56") == 1234.56
    assert parse_price("None") is None

    assert parse_rating("4.5 out of 5 stars") == 4.5
    assert parse_rating(None) is None

    assert parse_integer("1,234 ratings") == 1234
    assert parse_integer(None) is None

def test_config_helper():
    ConfigHelper._config = {}
    ConfigHelper._is_loaded = False
    
    mock_config = {"scraper": {"max_retries": 3}}
    with patch("os.path.exists", return_value=True), \
         patch("builtins.open", mock_open(read_data=json.dumps(mock_config))):
        
        assert ConfigHelper.get("scraper.max_retries") == 3
        assert ConfigHelper.get("nonexistent.key", default="fallback") == "fallback"
        
    with patch.dict(os.environ, {"FEISHU_TEST_APP_ID": "123"}):
        creds = ConfigHelper.get_feishu_bot("test")
        assert creds["app_id"] == "123"
