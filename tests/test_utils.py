"""
Tests for utility functions.
"""

import json
import tempfile
from pathlib import Path

import pytest

from src.utils import generate_sample_data, setup_directories, validate_file_size


class TestDataGeneration:
    """Test data generation functions."""
    
    def test_generate_sample_data_creates_files(self):
        """Test that sample data generation creates expected files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            generate_sample_data(temp_dir)
            
            temp_path = Path(temp_dir)
            assert (temp_path / "customers.csv").exists()
            assert (temp_path / "products.csv").exists()
            assert (temp_path / "items.json").exists()
            assert (temp_path / "orders.json").exists()
    
    def test_generated_json_is_valid(self):
        """Test that generated JSON files are valid."""
        with tempfile.TemporaryDirectory() as temp_dir:
            generate_sample_data(temp_dir)
            
            # Test items.json
            items_file = Path(temp_dir) / "items.json"
            with open(items_file) as f:
                items_data = json.load(f)
            
            assert isinstance(items_data, list)
            assert len(items_data) > 0
            assert "item_id" in items_data[0]
            assert "pricing" in items_data[0]
            
            # Test orders.json
            orders_file = Path(temp_dir) / "orders.json" 
            with open(orders_file) as f:
                orders_data = json.load(f)
            
            assert isinstance(orders_data, list)
            assert len(orders_data) > 0
            assert "order_id" in orders_data[0]
            assert "items" in orders_data[0]


class TestFileValidation:
    """Test file validation functions."""
    
    def test_validate_file_size_small_file(self):
        """Test file size validation with small file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("small content")
            f.flush()
            
            assert validate_file_size(f.name, max_size_mb=1.0) is True
    
    def test_validate_file_size_nonexistent_file(self):
        """Test file size validation with nonexistent file."""
        assert validate_file_size("/nonexistent/file.txt") is False
    
    def test_setup_directories_creates_structure(self):
        """Test that setup_directories creates expected structure."""
        # This would need to be tested in isolation or with proper mocking
        # For now, just ensure it doesn't raise exceptions
        try:
            setup_directories()
        except Exception as e:
            pytest.fail(f"setup_directories raised {e}")


if __name__ == "__main__":
    pytest.main([__file__])