locals {
  # Users configuration
  # Use users.local.json if it exists (for real deployments), otherwise use users.json (example data)
  users_file = fileexists("${path.module}/users.local.json") ? "${path.module}/users.local.json" : "${path.module}/users.json"
  users_raw = jsondecode(file(local.users_file))
  users_config = {
    for user in local.users_raw.users : user.user_name => {
      user_name      = user.user_name
      display_name   = user.display_name
      groups         = user.groups
      catalog_access = try(user.catalog_access, [])
    }
  }

  # Groups configuration
  group_names = toset(distinct(flatten([for user in local.users_raw.users : user.groups])))

  # User-group mappings
  user_group_mappings = flatten([
    for user_name, user in local.users_config : [
      for group_name in user.groups : {
        user  = user_name
        group = group_name
        key   = "${user_name}-${group_name}"
      }
    ]
  ])

  # Files configuration
  notebook_files = fileset("${path.module}/../course/notebooks", "**/*.py")
  dataset_files  = fileset("${path.module}/../course/datasets", "**/*.*")

  notebooks = {
    for file in local.notebook_files :
    file => replace(replace(file, "/", "_"), ".py", "")
  }

  datasets = {
    for file in local.dataset_files :
    file => file
  }

  # Catalog and schema configuration

  # Shared reference catalogs (read-only for users)
  shared_catalog_config = [
    "sales_dev",
    "sales_prod",
    "marketing_dev",
    "marketing_prod"
  ]

  # Single course catalog for all user work
  course_catalog_name = "databricks_course"

  # Combined catalog list
  catalog_config = concat(local.shared_catalog_config, [local.course_catalog_name])

  # Generate unique user schema names with deduplication
  user_schema_base_names = {
    for user_email in keys(local.users_config) :
    user_email => replace(split("@", user_email)[0], ".", "_")
  }

  # Count occurrences of each base name for deduplication
  schema_name_counts = {
    for base_name in distinct(values(local.user_schema_base_names)) :
    base_name => length([
      for email, name in local.user_schema_base_names : email if name == base_name
    ])
  }

  # User schema map with duplicate handling
  user_schema_map = {
    for user_email in keys(local.users_config) :
    user_email => (
      local.schema_name_counts[local.user_schema_base_names[user_email]] > 1
      ? format("%s_%d",
          local.user_schema_base_names[user_email],
          index(
            sort([for e in keys(local.users_config) : e if local.user_schema_base_names[e] == local.user_schema_base_names[user_email]]),
            user_email
          ) + 1
        )
      : local.user_schema_base_names[user_email]
    )
  }

  # Shared reference schemas (in reference catalogs)
  shared_reference_schemas = {
    "marketing_dev" = ["bronze", "silver", "gold", "experiments"]
  }

  standard_schemas = ["bronze", "silver", "gold"]

  # Schemas for shared reference catalogs
  shared_catalog_schemas = flatten([
    for catalog in local.shared_catalog_config : [
      for schema in lookup(local.shared_reference_schemas, catalog, local.standard_schemas) : {
        catalog_name = catalog
        schema_name  = schema
      }
    ]
  ])

  # Shared reference schemas in course catalog (read-only for all)
  course_shared_schemas = [
    for schema in ["shared_bronze", "shared_silver", "shared_gold"] : {
      catalog_name = local.course_catalog_name
      schema_name  = schema
    }
  ]

  # User-personal schemas in course catalog (one per user)
  user_personal_schemas = [
    for user_email in keys(local.users_config) : {
      catalog_name = local.course_catalog_name
      schema_name  = local.user_schema_map[user_email]
      owner_email  = user_email
    }
  ]

  # Combined schemas
  schemas_config = concat(
    local.shared_catalog_schemas,
    local.course_shared_schemas,
    local.user_personal_schemas
  )
}