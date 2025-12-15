"""Data ingestion modules for multiple source types."""

from end2end.ingestion.base import BaseIngestion
from end2end.ingestion.file_ingestion import FileIngestion
from end2end.ingestion.api_ingestion import APIIngestion
from end2end.ingestion.database_ingestion import DatabaseIngestion

__all__ = [
    "BaseIngestion",
    "FileIngestion",
    "APIIngestion",
    "DatabaseIngestion",
]