locals {
  # Users configuration
  users_raw = jsondecode(file("${path.module}/users.json"))
  users_config = {
    for user in local.users_raw.users : user.user_name => {
      user_name           = user.user_name
      display_name        = user.display_name
      groups             = user.groups
      catalog_permissions = try(user.catalog_permissions, {
        main    = "USE"
        samples = "READ"
      })
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
  catalog_config = [
    "sales_dev",
    "sales_prod",
    "marketing_dev",
    "marketing_prod"
  ]

  standard_schemas = ["bronze", "silver", "gold"]

  custom_schemas = {
    "marketing_dev" = ["bronze", "silver", "gold", "experiments"]
  }

  schemas_config = flatten([
    for catalog in local.catalog_config : [
      for schema in lookup(local.custom_schemas, catalog, local.standard_schemas) : {
        catalog_name = catalog
        schema_name  = schema
      }
    ]
  ])
}