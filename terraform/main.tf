# Data source for current user
data "databricks_current_user" "me" {}

# Create users from JSON file (conditional)
resource "databricks_user" "users" {
  for_each     = var.create_users ? local.users_config : {}
  user_name    = each.value.user_name
  display_name = each.value.display_name
  force        = true

  lifecycle {
    ignore_changes = [external_id, display_name]
  }
}

# Create base notebook directory
resource "databricks_directory" "course_root" {
  path             = "/Shared/terraform-managed/course"
  delete_recursive = true
}

# Create notebooks directory
resource "databricks_directory" "notebooks_dir" {
  path             = "${databricks_directory.course_root.path}/notebooks"
  depends_on       = [databricks_directory.course_root]
  delete_recursive = true
}

# Create notebook subdirectories (manually listed)
resource "databricks_directory" "notebook_subdirs" {
  for_each         = toset(var.notebook_subdirs)
  path             = "${databricks_directory.notebooks_dir.path}/${each.key}"
  delete_recursive = true
  depends_on       = [databricks_directory.notebooks_dir]
}

# Upload notebooks (manually listed)
resource "databricks_notebook" "notebooks" {
  for_each = var.notebooks

  path       = "${databricks_directory.notebooks_dir.path}/${each.key}"
  source     = "${path.module}/../course/notebooks/${each.key}"
  language   = each.value
  depends_on = [databricks_directory.notebook_subdirs]
}

# Create datasets directory
resource "databricks_directory" "datasets_dir" {
  path             = "${databricks_directory.course_root.path}/datasets"
  depends_on       = [databricks_directory.course_root]
  delete_recursive = true
}

# Upload datasets
resource "databricks_workspace_file" "test_csv" {
  source     = "${path.module}/../course/datasets/test.csv"
  path       = "${databricks_directory.datasets_dir.path}/test.csv"
  depends_on = [databricks_directory.datasets_dir]
}
