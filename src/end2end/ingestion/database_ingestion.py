"""
Database ingestion via JDBC connections.

Educational Notes:
    - Based on Week 2: 08_database_ingest.py
    - Demonstrates JDBC pattern
    - Shows incremental loading strategies
"""

from typing import Optional, Dict
from pyspark.sql import DataFrame

from end2end.ingestion.base import BaseIngestion
from end2end.config import PipelineConfig
from end2end.utils import get_logger

logger = get_logger(__name__)


class DatabaseIngestion(BaseIngestion):
    """
    Ingest data from relational databases via JDBC.

    Educational Pattern from Week 2 (08_database_ingest.py):
        - JDBC connection configuration
        - SQL query-based extraction
        - Incremental loading with watermarks
        - Partition reading for large tables

    Args:
        config: Pipeline configuration
        jdbc_url: JDBC connection URL
        table_or_query: Table name or SQL query
        connection_properties: JDBC connection properties (user, password, etc.)
        partition_column: Column for parallel reading (optional)
        num_partitions: Number of partitions for parallel reading

    Example:
        >>> config = PipelineConfig(catalog="databricks_course", source_schema="user")
        >>> props = {
        ...     "user": "db_user",
        ...     "password": "db_password",
        ...     "driver": "org.postgresql.Driver"
        ... }
        >>> ingestion = DatabaseIngestion(
        ...     config=config,
        ...     jdbc_url="jdbc:postgresql://localhost:5432/mydb",
        ...     table_or_query="sales_transactions",
        ...     connection_properties=props
        ... )
        >>> result = ingestion.execute("sales_from_db")
    """

    def __init__(
        self,
        config: PipelineConfig,
        jdbc_url: str,
        table_or_query: str,
        connection_properties: Dict[str, str],
        partition_column: Optional[str] = None,
        num_partitions: Optional[int] = None,
    ):
        super().__init__(config, source_name=f"jdbc:{table_or_query}")
        self.jdbc_url = jdbc_url
        self.table_or_query = table_or_query
        self.connection_properties = connection_properties
        self.partition_column = partition_column
        self.num_partitions = num_partitions

        # Update metadata (excluding sensitive connection info)
        self.metadata.update(
            {
                "jdbc_url": jdbc_url,
                "source_table": table_or_query,
                "partitioned": partition_column is not None,
            }
        )

    def read_source(self) -> DataFrame:
        """
        Read data from database via JDBC.

        Returns:
            DataFrame with database data

        Educational Note:
            From Week 2 (08_database_ingest.py):
            - Use JDBC for database connectivity
            - Partition large tables for parallel reading
            - Use SQL queries for filtering at source
        """
        logger.info(f"Reading from database: {self.table_or_query}")

        try:
            # Build JDBC reader
            reader = self.spark.read.format("jdbc").option("url", self.jdbc_url)

            # Add connection properties
            for key, value in self.connection_properties.items():
                reader = reader.option(key, value)

            # Handle table vs query
            if self._is_query():
                # Use query as dbtable (wrap in subquery)
                reader = reader.option("dbtable", f"({self.table_or_query}) as tmp")
            else:
                reader = reader.option("dbtable", self.table_or_query)

            # Add partitioning if specified
            if self.partition_column and self.num_partitions:
                logger.info(
                    f"Using partitioned read: column={self.partition_column}, "
                    f"partitions={self.num_partitions}"
                )
                reader = (
                    reader.option("partitionColumn", self.partition_column)
                    .option("numPartitions", str(self.num_partitions))
                    .option("lowerBound", "1")
                    .option("upperBound", "1000000")  # Adjust based on data
                )

            # Read data
            df = reader.load()

            logger.info(f"Successfully read {df.count()} rows from database")

            return df

        except Exception as e:
            logger.error(f"Failed to read from database: {self.table_or_query}", exc_info=True)
            raise

    def _is_query(self) -> bool:
        """Check if table_or_query is a SQL query (contains spaces/keywords)."""
        query_keywords = ["select", "from", "where", "join", "group by"]
        lower_text = self.table_or_query.lower()
        return any(keyword in lower_text for keyword in query_keywords)

    def read_incremental(
        self,
        watermark_column: str,
        last_watermark_value: str,
    ) -> DataFrame:
        """
        Read data incrementally using watermark column.

        Args:
            watermark_column: Column to track progress (timestamp, id)
            last_watermark_value: Last processed value

        Returns:
            DataFrame with new records only

        Example:
            >>> # Read only new records since last run
            >>> df = ingestion.read_incremental(
            ...     watermark_column="updated_at",
            ...     last_watermark_value="2024-01-01 00:00:00"
            ... )

        Educational Note:
            Incremental loading pattern from Week 2 (08_database_ingest.py):
            - Track last processed value
            - Filter at source with WHERE clause
            - Reduces data transfer and processing time
        """
        logger.info(
            f"Reading incremental data: {watermark_column} > {last_watermark_value}"
        )

        # Build incremental query
        if self._is_query():
            # Wrap existing query and add filter
            base_query = self.table_or_query
            incremental_query = (
                f"SELECT * FROM ({base_query}) t "
                f"WHERE t.{watermark_column} > '{last_watermark_value}'"
            )
        else:
            # Simple table with filter
            incremental_query = (
                f"SELECT * FROM {self.table_or_query} "
                f"WHERE {watermark_column} > '{last_watermark_value}'"
            )

        # Temporarily update query
        original_query = self.table_or_query
        self.table_or_query = incremental_query

        try:
            df = self.read_source()

            # Get max watermark value for next run
            from pyspark.sql.functions import max as spark_max

            max_watermark = df.agg(spark_max(watermark_column)).collect()[0][0]

            logger.info(f"New watermark value: {max_watermark}")
            self.metadata["new_watermark"] = str(max_watermark)

            return df

        finally:
            # Restore original query
            self.table_or_query = original_query

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return f"DatabaseIngestion(source='{self.table_or_query}')"