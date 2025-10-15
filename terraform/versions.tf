terraform {
  required_version = ">= 1.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.29"
    }
  }
}

provider "databricks" {
  profile = "datatribe"
  host    = "https://dbc-d8111651-e8b1.cloud.databricks.com"
}