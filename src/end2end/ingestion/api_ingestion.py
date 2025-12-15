"""
API-based data ingestion with retry logic and rate limiting.

Educational Notes:
    - Based on Week 2: 07_api_ingest.py
    - Demonstrates REST API patterns
    - Shows authentication and error handling
"""

from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import requests
import time
import json

from end2end.ingestion.base import BaseIngestion
from end2end.config import PipelineConfig
from end2end.utils import get_logger

logger = get_logger(__name__)


class APIIngestion(BaseIngestion):
    """
    Ingest data from REST API endpoints.

    Educational Pattern from Week 2 (07_api_ingest.py):
        - HTTP request handling
        - Authentication (API keys, tokens)
        - Retry logic with exponential backoff
        - Rate limiting
        - JSON response parsing

    Args:
        config: Pipeline configuration
        api_url: API endpoint URL
        schema: Explicit schema for response data (optional)
        auth_token: Authentication token (optional)
        headers: Additional HTTP headers
        params: URL parameters
        rate_limit_delay: Delay between requests in seconds

    Example:
        >>> config = PipelineConfig(catalog="databricks_course", source_schema="user")
        >>> ingestion = APIIngestion(
        ...     config=config,
        ...     api_url="https://api.example.com/sales",
        ...     auth_token="your-api-key",
        ...     params={"date": "2024-01-01"}
        ... )
        >>> result = ingestion.execute("sales_api_data")
    """

    def __init__(
        self,
        config: PipelineConfig,
        api_url: str,
        schema: Optional[StructType] = None,
        auth_token: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        rate_limit_delay: float = 0.1,
    ):
        super().__init__(config, source_name=api_url)
        self.api_url = api_url
        self.schema = schema
        self.auth_token = auth_token
        self.headers = headers or {}
        self.params = params or {}
        self.rate_limit_delay = rate_limit_delay

        # Set authorization header if token provided
        if auth_token:
            self.headers["Authorization"] = f"Bearer {auth_token}"

        # Update metadata
        self.metadata.update(
            {
                "api_url": api_url,
                "has_auth": auth_token is not None,
                "params": self.params,
            }
        )

    def _make_api_request(
        self,
        url: str,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """
        Make API request with retry logic.

        Args:
            url: Request URL
            max_retries: Maximum retry attempts

        Returns:
            JSON response as dictionary

        Raises:
            requests.exceptions.RequestException: If request fails
        """
        retry_count = 0

        while retry_count <= max_retries:
            try:
                logger.debug(f"Making API request to: {url} (attempt {retry_count + 1})")

                response = requests.get(
                    url,
                    headers=self.headers,
                    params=self.params,
                    timeout=30,
                )

                # Raise exception for bad status codes
                response.raise_for_status()

                # Rate limiting
                if self.rate_limit_delay > 0:
                    time.sleep(self.rate_limit_delay)

                return response.json()

            except requests.exceptions.RequestException as e:
                retry_count += 1
                logger.warning(
                    f"API request failed (attempt {retry_count}): {e}"
                )

                if retry_count > max_retries:
                    logger.error(f"API request failed after {max_retries} retries")
                    raise

                # Exponential backoff
                wait_time = 2**retry_count
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

        return {}

    def read_source(self) -> DataFrame:
        """
        Read data from API endpoint.

        Returns:
            DataFrame with API response data

        Educational Note:
            From Week 2 (07_api_ingest.py):
            - Make HTTP request
            - Parse JSON response
            - Convert to Spark DataFrame
            - Handle pagination if needed
        """
        logger.info(f"Fetching data from API: {self.api_url}")

        try:
            # Make API request
            response_data = self._make_api_request(self.api_url)

            # Handle different response structures
            if isinstance(response_data, dict):
                # Check for common response wrappers
                if "data" in response_data:
                    data = response_data["data"]
                elif "results" in response_data:
                    data = response_data["results"]
                else:
                    # Assume entire response is the data
                    data = [response_data]
            elif isinstance(response_data, list):
                data = response_data
            else:
                raise ValueError(f"Unexpected response type: {type(response_data)}")

            # Convert to DataFrame
            if self.schema:
                df = self.spark.createDataFrame(data, schema=self.schema)
                logger.info("Using explicit schema")
            else:
                # Convert to JSON strings first, then read
                json_strings = [json.dumps(record) for record in data]
                rdd = self.spark.sparkContext.parallelize(json_strings)
                df = self.spark.read.json(rdd)
                logger.warning("Inferring schema from API response (not recommended for production)")

            logger.info(f"Successfully read {df.count()} records from API")

            return df

        except Exception as e:
            logger.error(f"Failed to read from API: {self.api_url}", exc_info=True)
            raise

    def read_paginated(
        self,
        page_param: str = "page",
        page_size_param: str = "page_size",
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> DataFrame:
        """
        Read data from paginated API endpoint.

        Args:
            page_param: URL parameter name for page number
            page_size_param: URL parameter name for page size
            page_size: Number of records per page
            max_pages: Maximum pages to fetch (None for all)

        Returns:
            DataFrame with all paginated data

        Example:
            >>> ingestion = APIIngestion(config, "https://api.example.com/data")
            >>> df = ingestion.read_paginated(page_size=100, max_pages=10)
        """
        logger.info(f"Reading paginated API: {self.api_url}")

        all_data: List[Dict[str, Any]] = []
        page = 1

        while max_pages is None or page <= max_pages:
            # Update params for pagination
            self.params[page_param] = str(page)
            self.params[page_size_param] = str(page_size)

            # Fetch page
            response_data = self._make_api_request(self.api_url)

            # Extract data from response
            if isinstance(response_data, dict):
                data = response_data.get("data", response_data.get("results", []))
            else:
                data = response_data

            if not data:
                logger.info(f"No more data after page {page - 1}")
                break

            all_data.extend(data)
            logger.info(f"Fetched page {page}: {len(data)} records")

            page += 1

        logger.info(f"Total records fetched: {len(all_data)}")

        # Convert to DataFrame
        if self.schema:
            df = self.spark.createDataFrame(all_data, schema=self.schema)
        else:
            json_strings = [json.dumps(record) for record in all_data]
            rdd = self.spark.sparkContext.parallelize(json_strings)
            df = self.spark.read.json(rdd)

        return df

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return f"APIIngestion(url='{self.api_url}')"