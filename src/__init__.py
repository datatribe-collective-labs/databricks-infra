"""
Databricks Infrastructure Package

A comprehensive toolkit for managing Databricks infrastructure with Terraform,
including utilities for data generation, notebook validation, and CLI tools.
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "chanukya.pekala@gmail.com"

# Make key classes and functions available at package level
from .utils import generate_sample_data, validate_file_size

__all__ = [
    "generate_sample_data",
    "validate_file_size",
]