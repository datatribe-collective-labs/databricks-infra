# ===== SHARED REFERENCE CATALOGS =====

# Create shared reference catalogs (conditional based on variable)
resource "databricks_catalog" "shared_catalogs" {
  for_each       = var.create_catalogs ? toset(local.shared_catalog_config) : toset([])
  name           = each.key
  owner          = "chanukya.pekala@gmail.com"
  comment        = "Shared reference catalog managed by Terraform - Read-only for users"
  properties     = {}
  isolation_mode = "ISOLATED"

  lifecycle {
    ignore_changes = [
      storage_root,
      storage_location
    ]
  }
}

# Create course catalog for all user work (single catalog approach)
resource "databricks_catalog" "course_catalog" {
  count          = var.create_catalogs ? 1 : 0
  name           = local.course_catalog_name
  owner          = "chanukya.pekala@gmail.com"
  comment        = "Course catalog with user-specific schemas - Managed by Terraform"
  properties     = {}
  isolation_mode = "ISOLATED"
  storage_root   = "s3://dbstorage-prod-ev7mk/uc/56c074d4-16b0-45f1-b08d-1a86b917dc3c/d2f40d1c-8f17-4d79-82a4-3f02ef706e82"

  lifecycle {
    ignore_changes = [
      storage_location
    ]
  }
}

# Data source to reference existing catalogs when not creating them
data "databricks_catalog" "existing_catalogs" {
  for_each = var.create_catalogs ? toset([]) : toset(local.catalog_config)
  name     = each.key
}

# ===== SCHEMAS =====

# Create schemas when both catalogs and schemas should be created
resource "databricks_schema" "custom_schemas_with_created_catalogs" {
  for_each      = (var.create_catalogs && var.create_schemas) ? { for schema in local.schemas_config : "${schema.catalog_name}.${schema.schema_name}" => schema } : {}
  catalog_name  = each.value.catalog_name
  name          = each.value.schema_name
  owner         = "chanukya.pekala@gmail.com"
  comment       = "Schema managed by Terraform"
  properties    = {}
  force_destroy = true

  depends_on = [
    databricks_catalog.shared_catalogs,
    databricks_catalog.course_catalog
  ]
}

# Create schemas when using existing catalogs but creating new schemas
resource "databricks_schema" "custom_schemas_with_existing_catalogs" {
  for_each      = (!var.create_catalogs && var.create_schemas) ? { for schema in local.schemas_config : "${schema.catalog_name}.${schema.schema_name}" => schema } : {}
  catalog_name  = each.value.catalog_name
  name          = each.value.schema_name
  owner         = "chanukya.pekala@gmail.com"
  comment       = "Schema managed by Terraform"
  properties    = {}
  force_destroy = true
}

# Local to unify schema references
locals {
  all_schemas = var.create_schemas ? (
    var.create_catalogs ?
    databricks_schema.custom_schemas_with_created_catalogs :
    databricks_schema.custom_schemas_with_existing_catalogs
  ) : {}

  # Static schema map for grants
  schema_grant_map = var.create_schemas ? {
    for schema in local.schemas_config : "${schema.catalog_name}.${schema.schema_name}" => schema
  } : {}

  # Separate shared and user schemas for different permissions
  course_shared_schema_map = var.create_schemas ? {
    for key, schema in local.schema_grant_map :
    key => schema if schema.catalog_name == local.course_catalog_name && startswith(schema.schema_name, "shared_")
  } : {}

  course_user_schema_map = var.create_schemas ? {
    for key, schema in local.schema_grant_map :
    key => schema if schema.catalog_name == local.course_catalog_name && !startswith(schema.schema_name, "shared_")
  } : {}

  shared_reference_schema_map = var.create_schemas ? {
    for key, schema in local.schema_grant_map :
    key => schema if contains(local.shared_catalog_config, schema.catalog_name)
  } : {}
}

# ===== SHARED REFERENCE CATALOG GRANTS =====

resource "databricks_grants" "shared_catalog_grants" {
  for_each = var.create_catalogs ? databricks_catalog.shared_catalogs : {}
  catalog  = each.value.name

  # Admins get full control
  grant {
    principal  = "chanukya.pekala@gmail.com"
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = "datatribe.collective@gmail.com"
    privileges = ["ALL_PRIVILEGES"]
  }

  # Admin group gets full control
  grant {
    principal  = "platform_admins"
    privileges = ["ALL_PRIVILEGES"]
  }

  # All users get read-only access
  dynamic "grant" {
    for_each = keys(local.users_config)
    content {
      principal  = grant.value
      privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
    }
  }

  depends_on = [databricks_catalog.shared_catalogs, databricks_user.users]
}

# ===== COURSE CATALOG GRANTS =====

resource "databricks_grants" "course_catalog_grants" {
  count   = var.create_catalogs ? 1 : 0
  catalog = local.course_catalog_name

  # Admins get full control
  grant {
    principal  = "chanukya.pekala@gmail.com"
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = "datatribe.collective@gmail.com"
    privileges = ["ALL_PRIVILEGES"]
  }

  # Admin group gets full control
  grant {
    principal  = "platform_admins"
    privileges = ["ALL_PRIVILEGES"]
  }

  # All users can USE catalog and CREATE schemas
  dynamic "grant" {
    for_each = keys(local.users_config)
    content {
      principal  = grant.value
      privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
    }
  }

  depends_on = [databricks_catalog.course_catalog, databricks_user.users]
}

# ===== SHARED REFERENCE SCHEMA GRANTS =====

resource "databricks_grants" "shared_reference_schema_grants" {
  for_each = local.shared_reference_schema_map
  schema   = each.key

  # Admins get full control
  grant {
    principal  = "chanukya.pekala@gmail.com"
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = "datatribe.collective@gmail.com"
    privileges = ["ALL_PRIVILEGES"]
  }

  # Admin group gets full control
  grant {
    principal  = "platform_admins"
    privileges = ["ALL_PRIVILEGES"]
  }

  # All users get read-only access
  dynamic "grant" {
    for_each = keys(local.users_config)
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }

  depends_on = [
    databricks_schema.custom_schemas_with_created_catalogs,
    databricks_schema.custom_schemas_with_existing_catalogs
  ]
}

# ===== COURSE SHARED SCHEMA GRANTS (shared_bronze, shared_silver, shared_gold) =====

resource "databricks_grants" "course_shared_schema_grants" {
  for_each = local.course_shared_schema_map
  schema   = each.key

  # Admins get full control
  grant {
    principal  = "chanukya.pekala@gmail.com"
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = "datatribe.collective@gmail.com"
    privileges = ["USE_SCHEMA", "CREATE_TABLE"]
  }

  grant {
    principal  = "komal.azram@gmail.com"
    privileges = ["USE_SCHEMA", "CREATE_TABLE"]
  }

  # Admin group gets full control
  grant {
    principal  = "platform_admins"
    privileges = ["ALL_PRIVILEGES"]
  }

  # All users get read-only access
  dynamic "grant" {
    for_each = keys(local.users_config)
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }

  depends_on = [
    databricks_schema.custom_schemas_with_created_catalogs,
    databricks_schema.custom_schemas_with_existing_catalogs
  ]
}

# ===== USER-PERSONAL SCHEMA GRANTS (full control for owner, read-only for peers) =====

resource "databricks_grants" "user_personal_schema_grants" {
  for_each = local.course_user_schema_map
  schema   = each.key

  # Admins get full control
  grant {
    principal  = "chanukya.pekala@gmail.com"
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = "datatribe.collective@gmail.com"
    privileges = ["ALL_PRIVILEGES"]
  }

  # Admin group gets full control
  grant {
    principal  = "platform_admins"
    privileges = ["ALL_PRIVILEGES"]
  }

  # Owner gets full control of their schema
  grant {
    principal  = lookup(each.value, "owner_email", "chanukya.pekala@gmail.com")
    privileges = ["ALL_PRIVILEGES"]
  }

  # Students group gets read-only access (peer learning)
  grant {
    principal  = "platform_students"
    privileges = ["USE_SCHEMA", "SELECT"]
  }

  # All other users get read-only access (peer learning)
  dynamic "grant" {
    for_each = [for email in keys(local.users_config) : email if email != lookup(each.value, "owner_email", "")]
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }

  depends_on = [
    databricks_schema.custom_schemas_with_created_catalogs,
    databricks_schema.custom_schemas_with_existing_catalogs
  ]
}