# Data Platform Engineer Guide - Complete Infrastructure Setup

*"I want to deploy and manage Databricks infrastructure for my team of Data Engineers"*

## 🎯 What You'll Accomplish

By following this guide, you'll set up a complete Databricks learning environment where:
- **Students get instant access** to course content via shared workspace
- **New content automatically deploys** when the course is updated
- **User management is centralized** through Infrastructure as Code
- **Everything is reproducible** across different environments

## 📋 Prerequisites (Day 0)

### Required Access & Tools
- **Databricks workspace** with admin access (Free tier) and you get it when you sign up at [databricks.com/try-databricks](https://databricks.com/try-databricks)
- **GitHub account** for repository access
- **Basic Terraform knowledge** (resources, variables, state management)

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

## 🚀 Day 1: Initial Setup

### Step 1: Get the Infrastructure Code
```bash
# Clone the repository
git clone https://github.com/chanukyapekala/databricks-infra
cd databricks-infra

# Install Python dependencies
poetry install
```

### Step 2: Configure Authentication
```bash
# Authenticate with your Databricks workspace
databricks auth login
# Enter your workspace URL: https://your-workspace.cloud.databricks.com
# Create and enter your personal access token

# Verify connection
databricks workspace list /
```

### Step 3: Customize for Your Organization

#### Configure Students
Edit `terraform/users.json` with your team:
```json
{
  "users": [
    {
      "user_name": "alice@yourcompany.com",
      "display_name": "Alice Johnson",
      "groups": ["data_engineers"]
    },
    {
      "user_name": "bob@yourcompany.com", 
      "display_name": "Bob Smith",
      "groups": ["data_engineers"]
    },
    {
      "user_name": "charlie@yourcompany.com",
      "display_name": "Charlie Davis", 
      "groups": ["data_engineers", "advanced_users"]
    }
  ]
}
```

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

### Step 4: Deploy Infrastructure (10 minutes)
```bash
cd terraform

# Initialize Terraform
terraform init

# Review what will be created
terraform plan

# Deploy everything
terraform apply
# Type 'yes' when prompted
```

**What Gets Created:**
- ✅ User accounts for all students
- ✅ Groups with appropriate permissions
- ✅ Catalogs and schemas (bronze, silver, gold layers)
- ✅ Course notebooks deployed to `/Shared/terraform-managed/course/notebooks/`
- ✅ Sample datasets in shared location
- ✅ Permission grants for students to access content

## 👥 Day 2: Student Onboarding (10 minutes per student)

### Onboard Your Students
Send each student:

1. **Workspace Access**:
   - Workspace URL: `https://your-workspace.cloud.databricks.com`
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

### Adding New Users (optional)
```bash
# 1. Edit terraform/users.json - add new user
{
  "user_name": "newstudent@yourcompany.com",
  "display_name": "New Student",
  "groups": ["data_engineers"]
}

# 2. Deploy changes
cd terraform
terraform plan
terraform apply

# 3. New user gets automatic access to all course content
```

### Getting Course Updates
```bash
# When new course content is released:
# 1. Pull latest changes
git pull origin main

# 2. Review what's new (optional
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
- **Use conditional deployment** for different environments
- **Require secrets**: `DATABRICKS_HOST` and `DATABRICKS_TOKEN`

To enable:
```bash
# Add secrets to your GitHub repository:
# Settings → Secrets → Actions
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
```

## 🚨 Troubleshooting

### Common Issues

**"Cannot create catalog" Error**
```bash
# Solution: Create catalogs manually first (Free Edition limitation)
# 1. In Databricks UI: Data → Create Catalog
# 2. Import to Terraform:
terraform import 'databricks_catalog.custom_catalogs["catalog_name"]' catalog_name
```

**"Group already exists" Error** 
```bash
# Solution: Use existing groups
terraform apply -var="create_groups=false"
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
# Verify infrastructure health
poetry run python -m src.cli status

# Validate all notebooks
poetry run validate-notebooks

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