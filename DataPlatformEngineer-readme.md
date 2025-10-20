# Data Platform Engineer Guide - Complete Infrastructure Setup

*"I want to deploy and manage Databricks infrastructure for my team of Data Engineers"*

## 🎯 What You'll Accomplish

By following this guide, you'll set up a complete Databricks learning environment where:
- **Students get instant access** to course content via shared workspace
- **New content automatically deploys** when the course is updated
- **User management is centralized** through Infrastructure as Code
- **Everything is reproducible** across different environments

## 📋 Prerequisites

### Required Access & Tools
- **Databricks workspace** with admin access
  - Free tier: Sign up at [databricks.com/try-databricks](https://databricks.com/try-databricks)
  - Premium/Enterprise: Request from your organization's admin
- **GitHub account** for repository access
- **Basic Terraform knowledge** (resources, variables, state management)

### Understanding Your Workspace Type
This infrastructure works with both **Free Edition** and **Premium/Enterprise** workspaces:

| Feature | Free Edition | Premium/Enterprise |
|---------|-------------|-------------------|
| **Catalog Creation** | Manual (via UI) + Import | Terraform manages |
| **Group Creation** | Terraform manages | Terraform manages |
| **Schema Creation** | Terraform manages | Terraform manages |
| **Setup Approach** | Import catalogs first | Create or import all |

**This guide assumes you're using the recommended approach**: Create catalogs manually and import them (works for both editions)

### Install Required Tools
```bash
# Poetry (Python dependency management)
curl -sSL https://install.python-poetry.org | python3 -

# Databricks CLI
brew tap databricks/tap && brew install databricks  # macOS
# OR: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Terraform
brew tap hashicorp/tap && brew install terraform  # macOS
# OR: Download from https://terraform.io/downloads
```

## Initial Setup

### Step 1: Get the Infrastructure Code
```bash
# Clone the repository
git clone https://github.com/chanukyapekala/databricks-infra
cd databricks-infra

# Install Python dependencies
poetry install
```

### Step 2: Configure Authentication

**IMPORTANT**: This project requires **Personal Access Token (PAT)** authentication with **workspace admin privileges** for full Terraform automation.

#### Generate Personal Access Token
1. Log into your Databricks workspace
2. Navigate to **User Settings** → **Developer** → **Access Tokens**
3. Click **Generate New Token**
4. Set token lifetime (90 days recommended for development)
5. Copy and save the token securely (shown only once)

**Prerequisite**: Ensure you have **workspace admin** access:
- Your account must be in the "admins" group
- Required for SCIM API access (user/group management)
- OAuth authentication has limitations - always use PAT for full automation

#### Configure Databricks CLI
```bash
# Create/update CLI profile with PAT
cat > ~/.databrickscfg << EOF
[datatribe]
host  = https://your-workspace.cloud.databricks.com
token = dapi1234567890abcdef...  # Your PAT from above
EOF

# Or use interactive setup
databricks configure --token --profile datatribe
# Enter your workspace URL: https://your-workspace.cloud.databricks.com
# Enter your PAT when prompted

# Verify connection and admin access
databricks workspace list /
databricks users list  # Should work if you have admin privileges
```

#### Update Terraform Provider Configuration
Edit `terraform/versions.tf` to use your profile:
```hcl
provider "databricks" {
  profile = "datatribe"  # Match your profile name in ~/.databrickscfg
}
```

### Step 3: Customize for Your Organization

#### Configure Users and Groups
Edit `terraform/users.json` with your team:
```json
{
  "users": [
    {
      "user_name": "instructor@yourcompany.com",
      "display_name": "Lead Instructor",
      "groups": ["admins"]
    },
    {
      "user_name": "student1@yourcompany.com",
      "display_name": "Student One",
      "groups": ["students"]
    },
    {
      "user_name": "student2@yourcompany.com",
      "display_name": "Student Two",
      "groups": ["students"]
    }
  ]
}
```

**Group Permissions**:
- `admins` → `platform_admins` workspace group → ALL_PRIVILEGES on all resources
- `students` → `platform_students` workspace group → Own schema + read-only on shared resources

#### Customize Catalogs (Optional)
Edit `terraform/locals.tf` to match your naming conventions:
```hcl
catalog_config = [
  "training_dev",        # Replace sales_dev
  "training_prod",       # Replace sales_prod
  "analytics_sandbox",   # Replace marketing_dev
  "team_experiments"     # Replace marketing_prod
]
```

### Step 4: Create Catalogs in Databricks UI
**Important**: Databricks requires catalogs to be created with proper storage configuration.

1. Navigate to **Data** → **Create Catalog** in Databricks UI
2. Create these catalogs (one at a time):
   - `sales_dev`
   - `sales_prod`
   - `marketing_dev`
   - `marketing_prod`
3. Use default storage settings when prompted

### Step 5: Deploy Infrastructure (15 minutes)
```bash
cd terraform

# Initialize Terraform
terraform init

# Import the manually created catalogs
terraform import 'databricks_catalog.custom_catalogs["sales_dev"]' sales_dev
terraform import 'databricks_catalog.custom_catalogs["sales_prod"]' sales_prod
terraform import 'databricks_catalog.custom_catalogs["marketing_dev"]' marketing_dev
terraform import 'databricks_catalog.custom_catalogs["marketing_prod"]' marketing_prod

# Review what will be created (groups, schemas, permissions, notebooks)
terraform plan

# Deploy everything
terraform apply
# Type 'yes' when prompted
```

**What Gets Created:**
- ✅ User accounts for all students
- ✅ Groups with appropriate permissions (platform_admins)
- ✅ Schemas in catalogs (bronze, silver, gold layers + experiments)
- ✅ Course notebooks deployed to `/Shared/terraform-managed/course/notebooks/`
- ✅ Sample datasets in shared location
- ✅ Permission grants for students to access content

**What Gets Imported:**
- ✅ Existing catalogs (sales_dev, sales_prod, marketing_dev, marketing_prod)

## 👥 User/Student Onboarding

### Onboard Your Users
Send each user:

1. **Workspace Access**:
   - Workspace URL: `https://dbc-d8111651-e8b1.cloud.databricks.com`
   - Login instructions (SSO via e-mail account)

2. **Getting Started Guide**:
   - Link to: `DataEngineer-readme.md` in this repository
   - Starting point: `/Shared/terraform-managed/course/notebooks/01_week/`

3. **First Steps**:
   ```bash
   # Students navigate to:
   # Workspace → Shared → terraform-managed → course → notebooks
   # Start with: 01_week/00_databricks_fundamentals.py
   # Clone notebooks to personal folder for editing
   ```

### Verify Student Access
```bash
# Check deployed notebooks
databricks workspace list /Shared/terraform-managed/course/notebooks

# Verify student can access workspace
# (Have student test login and access to shared content)
```

## 🔄 Ongoing Management

### Adding New Users

**For Regular Students**:
```json
// Edit terraform/users.json
{
  "user_name": "newstudent@yourcompany.com",
  "display_name": "New Student",
  "groups": ["students"]
}
```

**For Admin/Instructors**:
```json
{
  "user_name": "newinstructor@yourcompany.com",
  "display_name": "New Instructor",
  "groups": ["admins"]
}
```

**Deploy**:
```bash
cd terraform
terraform apply
```

**What Happens Automatically**:
- User account created in Databricks
- Added to workspace group (platform_students or platform_admins)
- Personal schema created: `databricks_course.newstudent`
- Permissions configured based on group membership

### Removing Users

**Step 1: Backup User Data (if needed)**:
```sql
-- Log into Databricks SQL Editor
-- Transfer schema ownership before removal
ALTER SCHEMA databricks_course.student_name
OWNER TO `admin@yourcompany.com`;
```

**Step 2: Remove from users.json**:
```bash
# Delete the user entry from terraform/users.json
# Then apply:
terraform apply
```

**⚠️ Warning**: User account, personal schema, and ALL data will be permanently deleted unless ownership is transferred first.

### Changing User Roles

**Promote Student to Admin**:
```json
{
  "user_name": "user@yourcompany.com",
  "display_name": "User Name",
  "groups": ["admins"]  // Changed from ["students"]
}
```

**Apply changes**:
```bash
terraform apply
# Permissions update automatically
```

### Getting Course Updates
```bash
# When new course content is released:
# 1. Pull latest changes
git pull origin main

# 2. Review what's new (optional)
git log --oneline --since="1 week ago" course/notebooks/

# 3. Deploy updates to student workspace
cd terraform
terraform plan    # Review new content
terraform apply   # Deploy to students

# Users automatically see new notebooks in shared workspace!
```

### Managing Different User Groups
```bash
# Create advanced group in terraform/users.json:
{
  "user_name": "senior@yourcompany.com",
  "groups": ["data_engineers", "advanced_users"]
}

# Advanced users can get additional permissions or catalogs
# Modify terraform/groups.tf and terraform/catalogs.tf as needed
```

## 🏗️ Infrastructure Architecture

### Directory Structure
```
/Shared/terraform-managed/
├── course/
│   ├── notebooks/          # All course content (auto-deployed)
│   │   ├── 01_week/        # Week 1 fundamentals
│   │   ├── 02_week/        # Week 2 data ingestion
│   │   └── ...
│   └── datasets/           # Sample data files
├── users/                  # Student personal folders (auto-created)
└── shared-resources/       # Team shared resources
```

### Catalog Structure
```
training_dev                # Development environment
├── bronze/                 # Raw data layer
├── silver/                # Cleaned data layer
└── gold/                  # Aggregated data layer

training_prod              # Production environment  
├── bronze/
├── silver/
└── gold/
```

## 🔧 Advanced Configuration

### Multi-Environment Setup (Optional)
#### Not recommended for first-time users
```bash
# For separate dev/staging/prod:
# 1. Create separate terraform workspaces
terraform workspace new dev
terraform workspace new staging  
terraform workspace new prod

# 2. Use environment-specific variables
terraform apply -var="environment=dev"
terraform apply -var="environment=prod"
```

### CI/CD Integration
Your repository includes GitHub Actions that:
- **Automatically deploy** when you push changes
- **Use conditional deployment** for different environments (notebooks only, not infrastructure)
- **Require secrets**: `DATABRICKS_HOST` and `DATABRICKS_TOKEN`

**Important**: CI/CD uses **reference-only mode** (`create_*=false`) since GitHub Actions has no Terraform state file. It only manages notebooks and directories, referencing existing infrastructure.

To enable:
```bash
# Add secrets to your GitHub repository:
# Settings → Secrets → Actions
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token  # Same PAT with admin privileges

# Note: The workflow is configured to:
# - Local dev: Manages ALL resources (catalogs, users, schemas, notebooks)
# - CI/CD: Only manages notebooks (references existing infrastructure)
```

## 🔄 Switching to a New Workspace

If you need to deploy this infrastructure to a different Databricks workspace:

### Quick Migration Steps
```bash
# 1. Configure new workspace authentication
databricks auth login --profile new-workspace
# Enter new workspace URL and token

# 2. Update Terraform provider (if using profiles)
# Edit terraform/versions.tf to use new profile

# OR set environment variables
export DATABRICKS_HOST="https://new-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-new-token"

# 3. Create catalogs in new workspace UI
# Navigate to Data → Create Catalog (see Step 4 above)

# 4. Re-initialize and import
cd terraform
terraform init -reconfigure

# Import catalogs
terraform import 'databricks_catalog.custom_catalogs["sales_dev"]' sales_dev
terraform import 'databricks_catalog.custom_catalogs["sales_prod"]' sales_prod
terraform import 'databricks_catalog.custom_catalogs["marketing_dev"]' marketing_dev
terraform import 'databricks_catalog.custom_catalogs["marketing_prod"]' marketing_prod

# 5. Deploy to new workspace
terraform plan
terraform apply
```

## 🚨 Troubleshooting

### Common Issues

**"Cannot create catalog: Metastore storage root URL does not exist" Error**
```bash
# Root Cause: Databricks requires storage location for catalogs
# Solution: Create catalogs manually in UI first, then import to Terraform

# 1. In Databricks UI: Data → Create Catalog (use default storage)
# 2. Import to Terraform:
terraform import 'databricks_catalog.custom_catalogs["catalog_name"]' catalog_name

# 3. Verify catalog is imported:
terraform state list | grep catalog
```

**"Catalog must be replaced due to storage_root change" Error**
```bash
# Root Cause: Terraform config doesn't match existing catalog storage
# Solution: Already fixed in catalogs.tf with lifecycle ignore_changes

# Verify the fix is in place:
grep -A 5 "lifecycle" terraform/catalogs.tf
# Should show: ignore_changes = [storage_root, storage_location]
```

**"Group already exists" Error** 
```bash
# Solution: Use existing groups
terraform apply -var="create_groups=false"
```

**"Only accessible by admins" or SCIM API errors**
```bash
# Root Cause: PAT lacks workspace admin privileges or was generated before admin access
# Solution: Regenerate PAT after ensuring workspace admin access

# 1. Verify you're in admins group (in Databricks UI or CLI):
databricks users list --filter 'userName eq "your-email@domain.com"'
# Check groups field includes "admins"

# 2. Regenerate PAT after confirming admin access:
# - Databricks UI → User Settings → Developer → Access Tokens
# - Generate new token (delete old one)
# - Update ~/.databrickscfg with new token

# 3. Verify SCIM API access:
databricks users list  # Should work without errors
databricks groups list  # Should return groups

# 4. Retry Terraform:
terraform plan
```

**Users can't access notebooks**
```bash
# Check permissions:
databricks workspace get-status /Shared/terraform-managed/course/notebooks
# Verify user exists:
databricks users list | grep student@company.com
```

**Notebooks not updating**
```bash
# Force refresh:
terraform taint 'databricks_notebook.course_notebooks["notebook_name"]'
terraform apply
```

### Terraform State Management
```bash
# View current state
terraform state list

# Remove problematic resource
terraform state rm 'databricks_user.users["problematic@email.com"]'

# Reimport user
terraform import 'databricks_user.users["user@email.com"]' user@email.com
```

## 📊 Monitoring & Maintenance

### Regular Tasks
- **Weekly**: Check for course updates (`git pull origin main`)
- **Monthly**: Review student access and clean up unused accounts
- **Quarterly**: Update Terraform and provider versions

### Health Checks
```bash
# Check Terraform state
terraform plan  # Should show "No changes"
```

## 🎓 Success Metrics

Your deployment is successful when:
- ✅ Students can access workspace immediately after onboarding
- ✅ Course content appears automatically in shared workspace
- ✅ New students can be added with a single `terraform apply`
- ✅ Course updates deploy seamlessly to student environment
- ✅ Students can focus on learning instead of infrastructure setup

**Ready to deploy infrastructure for your Data Engineering team? Start with Day 1 setup above!** 🚀