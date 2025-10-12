# Create catalogs (conditional based on variable)
resource "databricks_catalog" "custom_catalogs" {
  for_each       = var.create_catalogs ? toset(local.catalog_config) : toset([])
  name           = each.key
  comment        = "Catalog managed by Terraform"
  properties     = {}
  isolation_mode = "OPEN"
  storage_root   = "s3://dbstorage-prod-nj0pi/uc/841f0d32-d62a-458b-b2f9-34aa65ce130e/76a86886-8a5f-4f4b-b890-a352622aaad1"
}

# Data source to reference existing catalogs when not creating them
data "databricks_catalog" "existing_catalogs" {
  for_each = var.create_catalogs ? toset([]) : toset(local.catalog_config)
  name     = each.key
}

# Create schemas when both catalogs and schemas should be created
resource "databricks_schema" "custom_schemas_with_created_catalogs" {
  for_each      = (var.create_catalogs && var.create_schemas) ? { for schema in local.schemas_config : "${schema.catalog_name}.${schema.schema_name}" => schema } : {}
  catalog_name  = each.value.catalog_name
  name          = each.value.schema_name
  comment       = "Schema managed by Terraform"
  properties    = {}
  force_destroy = true

  depends_on = [databricks_catalog.custom_catalogs]
}

# Create schemas when using existing catalogs but creating new schemas
resource "databricks_schema" "custom_schemas_with_existing_catalogs" {
  for_each      = (!var.create_catalogs && var.create_schemas) ? { for schema in local.schemas_config : "${schema.catalog_name}.${schema.schema_name}" => schema } : {}
  catalog_name  = each.value.catalog_name
  name          = each.value.schema_name
  comment       = "Schema managed by Terraform"
  properties    = {}
  force_destroy = true
}

# Local to unify schema references (only when creating schemas)
locals {
  all_schemas = var.create_schemas ? (
    var.create_catalogs ? 
      databricks_schema.custom_schemas_with_created_catalogs : 
      databricks_schema.custom_schemas_with_existing_catalogs
  ) : {}
}

# Grant catalog permissions (only when catalogs are created)
resource "databricks_grants" "catalog_grants" {
  for_each = var.create_catalogs ? databricks_catalog.custom_catalogs : {}
  catalog  = each.value.name

  grant {
    principal  = "chanukya.pekala@gmail.com"
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }

  depends_on = [databricks_catalog.custom_catalogs, databricks_user.users]
}

# Grant schema permissions (only when schemas are managed by Terraform)
resource "databricks_grants" "schema_grants" {
  for_each = var.create_schemas ? local.all_schemas : {}
  schema   = each.key

  grant {
    principal  = "chanukya.pekala@gmail.com"
    privileges = ["USE_SCHEMA", "CREATE_TABLE"]
  }

  depends_on = [databricks_user.users]
}