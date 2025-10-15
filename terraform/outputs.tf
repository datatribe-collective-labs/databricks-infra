output "notebook_paths" {
  description = "Paths to all created notebooks"
  value = {
    for key, notebook in databricks_notebook.notebooks : key => notebook.path
  }
}

output "current_user" {
  description = "Current Databricks user"
  value       = data.databricks_current_user.me.user_name
}

output "notebook_base_path" {
  description = "Base path for notebooks in Databricks workspace"
  value       = databricks_directory.notebooks_dir.path
}

output "datasets_base_path" {
  description = "Base path for datasets in Databricks workspace"
  value       = databricks_directory.datasets_dir.path
}

output "course_root_path" {
  description = "Root path for the course in Databricks workspace"
  value       = databricks_directory.course_root.path
}

output "created_users" {
  description = "List of created user emails"
  value       = [for user in databricks_user.users : user.user_name]
  sensitive   = true
}

output "created_groups" {
  description = "Map of created groups and their IDs"
  value = {
    for key, group in databricks_group.groups : key => group.id
  }
}