# Create catalogs (conditional based on variable)
resource "databricks_catalog" "custom_catalogs" {
  for_each       = var.create_catalogs ? toset(local.catalog_config) : toset([])
  name           = each.key
  comment        = "Catalog managed by Terraform"
  properties     = {}
  isolation_mode = "ISOLATED"
  # storage_root is managed by Databricks and will be imported from existing catalogs

  lifecycle {
    ignore_changes = [
      storage_root,    # Don't change storage location after creation
      storage_location # Don't change storage location after creation
    ]
  }
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

  # Static schema map for grants to avoid dependency issues
  schema_grant_map = var.create_schemas ? {
    for schema in local.schemas_config : "${schema.catalog_name}.${schema.schema_name}" => schema
  } : {}
}

# Grant catalog permissions (only when catalogs are created)
resource "databricks_grants" "catalog_grants" {
  for_each = var.create_catalogs ? databricks_catalog.custom_catalogs : {}
  catalog  = each.value.name

  grant {
    principal  = "chanukya.pekala@gmail.com"
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = "datatribe.collective@gmail.com"
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = "komal.azram@gmail.com"
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }

  grant {
    principal  = "yangtuomailbox@gmail.com"
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }

  grant {
    principal  = "joonas.f.koskinen@gmail.com"
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }

  grant {
    principal  = "rafaela.kschroeder@gmail.com"
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }

  grant {
    principal  = "amydn16@gmail.com"
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }

  grant {
    principal  = "oiivantsov@outlook.com"
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }

  grant {
    principal  = "tishchuk167@gmail.com"
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }

  depends_on = [databricks_catalog.custom_catalogs]
}

# Grant schema permissions (only when schemas are managed by Terraform)
resource "databricks_grants" "schema_grants" {
  for_each = local.schema_grant_map
  schema   = each.key

  grant {
    principal  = "chanukya.pekala@gmail.com"
    privileges = ["USE_SCHEMA", "CREATE_TABLE"]
  }

  grant {
    principal  = "datatribe.collective@gmail.com"
    privileges = ["USE_SCHEMA", "CREATE_TABLE"]
  }

  grant {
    principal  = "komal.azram@gmail.com"
    privileges = ["USE_SCHEMA", "CREATE_TABLE"]
  }

  grant {
    principal  = "yangtuomailbox@gmail.com"
    privileges = ["USE_SCHEMA", "CREATE_TABLE"]
  }

  grant {
    principal  = "joonas.f.koskinen@gmail.com"
    privileges = ["USE_SCHEMA", "CREATE_TABLE"]
  }

  grant {
    principal  = "rafaela.kschroeder@gmail.com"
    privileges = ["USE_SCHEMA", "CREATE_TABLE"]
  }

  grant {
    principal  = "amydn16@gmail.com"
    privileges = ["USE_SCHEMA", "CREATE_TABLE"]
  }

  grant {
    principal  = "oiivantsov@outlook.com"
    privileges = ["USE_SCHEMA", "CREATE_TABLE"]
  }

  grant {
    principal  = "tishchuk167@gmail.com"
    privileges = ["USE_SCHEMA", "CREATE_TABLE"]
  }

  depends_on = [
    databricks_schema.custom_schemas_with_created_catalogs,
    databricks_schema.custom_schemas_with_existing_catalogs
  ]
}