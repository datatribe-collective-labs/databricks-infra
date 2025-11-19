variable "notebook_language" {
  description = "The language of the notebook"
  type        = string
  default     = "PYTHON"
}

variable "create_catalogs" {
  description = "Whether to create catalogs (set to false for Free Edition or CI/CD)"
  type        = bool
  default     = true
}

variable "create_groups" {
  description = "Whether to create groups (workspace-level groups for Unity Catalog grants)"
  type        = bool
  default     = true
}

variable "create_schemas" {
  description = "Whether to create schemas (set to false if schemas already exist)"
  type        = bool
  default     = true
}

variable "create_users" {
  description = "Whether to create users (set to false if users already exist or SCIM API is not available)"
  type        = bool
  default     = true
}

variable "notebook_subdirs" {
  type    = list(string)
  default = ["utils", "01_week", "foundations", "02_week", "03_week", "04_week", "05_week", "advanced"]
}

variable "notebooks" {
  type = map(string)
  default = {
    # Shared utilities
    "utils/user_schema_setup.py" = "PYTHON"

    # Week 1: Databricks Fundamentals (01-05)
    "01_week/01_databricks_fundamentals.py"       = "PYTHON"
    "01_week/02_unity_catalog_deep_dive.py"       = "PYTHON"
    "01_week/03_cluster_management.py"            = "PYTHON"
    "01_week/04_spark_on_databricks.py"           = "PYTHON"
    "01_week/05_delta_lake_concepts_explained.py" = "PYTHON"

    # Foundations: Data Modelling Patterns (01-04)
    "foundations/01_introduction_to_data_modeling.py" = "PYTHON"
    "foundations/02_medallion_architecture.py"        = "PYTHON"
    "foundations/03_dimensional_modeling.py"          = "PYTHON"
    "foundations/04_scd_and_delta_patterns.py"        = "PYTHON"

    # Week 2: Data Ingestion (06-10)
    "02_week/06_file_ingestion.py"               = "PYTHON"
    "02_week/07_api_ingest.py"                   = "PYTHON"
    "02_week/08_database_ingest.py"              = "PYTHON"
    "02_week/09_s3_ingest.py"                    = "PYTHON"
    "02_week/10_ingestion_concepts_explained.py" = "PYTHON"

    # Week 3: Data Transformations (11-14)
    "03_week/11_simple_transformations.py"            = "PYTHON"
    "03_week/12_window_transformations.py"            = "PYTHON"
    "03_week/13_aggregations.py"                      = "PYTHON"
    "03_week/14_transformation_concepts_explained.py" = "PYTHON"

    # Week 4: End-to-End Workflows (15-17)
    "04_week/15_file_to_aggregation.py"         = "PYTHON"
    "04_week/16_api_to_aggregation.py"          = "PYTHON"
    "04_week/17_pipeline_patterns_explained.py" = "PYTHON"

    # Week 5: Production Deployment (18-21)
    "05_week/18_job_orchestration_concepts_explained.py" = "PYTHON"
    "05_week/19_create_multi_task_ingestion_job.py"      = "PYTHON"
    "05_week/20_wheel_creation_with_poetry.py"           = "PYTHON"
    "05_week/21_stock_market_wheel_deployment.py"        = "PYTHON"

    # Advanced: Databricks Apps (01-02)
    "advanced/01_databricks_apps_guide.py"        = "PYTHON"
    "advanced/02_stock_market_analyzer_app.py"    = "PYTHON"
  }
}
