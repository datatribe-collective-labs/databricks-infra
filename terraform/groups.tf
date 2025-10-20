# Create groups (conditional based on variable)
resource "databricks_group" "groups" {
  for_each = var.create_groups ? toset(local.group_names) : toset([])
  display_name = each.key == "admins" ? "platform_admins" : (
    each.key == "users" ? "platform_users" : (
    each.key == "students" ? "platform_students" : each.key
  ))
}

# Data source to reference existing groups when not creating them
data "databricks_group" "existing_groups" {
  for_each = var.create_groups ? toset([]) : toset(local.group_names)
  display_name = each.key == "admins" ? "platform_admins" : (
    each.key == "users" ? "platform_users" : (
    each.key == "students" ? "platform_students" : each.key
  ))
}

# Local to get group references (either created or existing)
locals {
  group_refs = var.create_groups ? databricks_group.groups : data.databricks_group.existing_groups
}

# Add users to their groups (conditional - only if creating users)
resource "databricks_group_member" "user_groups" {
  for_each = var.create_users ? {
    for mapping in local.user_group_mappings : "${mapping.user}-${mapping.group}" => {
      user  = mapping.user
      group = mapping.group
    }
  } : {}

  group_id  = local.group_refs[each.value.group].id
  member_id = databricks_user.users[each.value.user].id

  depends_on = [
    databricks_user.users
  ]
}

# Debug output to verify mappings
output "group_names" {
  value = local.group_names
}

output "user_group_mappings" {
  value = local.user_group_mappings
}