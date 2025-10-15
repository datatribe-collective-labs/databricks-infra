# CLAUDE.md - Technical Documentation

This file provides comprehensive technical guidance for Claude Code (claude.ai/code) when working with this Databricks infrastructure project.

## Repository Architecture

This is a **production-deployed** Databricks Infrastructure as Code (IaC) project combining:
- **Terraform**: Unity Catalog, users, permissions management (currently managing 8 users, 4 catalogs, 13 schemas)
- **Python Package**: Professional tooling with Poetry, CLI, validation
- **Databricks Course**: 21 notebooks across 5 weeks (fundamentals to job orchestration)
- **CI/CD Pipeline**: GitHub Actions with automated deployment to Premium Edition workspace

### Current Production State
- **Workspace**: Premium Edition (https://dbc-d8111651-e8b1.cloud.databricks.com)
- **Users**: 8 active users with full catalog access
- **Catalogs**: 4 imported catalogs (sales_dev, sales_prod, marketing_dev, marketing_prod)
- **Schemas**: 13 schemas using medallion architecture
- **Deployment**: Managed via Terraform with imported resources

## Core Technologies Stack

- **Infrastructure**: Terraform + Databricks Provider (~> 1.29)
- **Python**: 3.11+ with Poetry dependency management
- **Data Engineering**: PySpark, Delta Lake, Unity Catalog
- **Quality Tools**: Pre-commit hooks, pytest, ruff
- **Deployment**: GitHub Actions with conditional resource creation

## Project Structure

```
databricks-infra/
├── terraform/                    # Infrastructure as Code
│   ├── versions.tf              # Provider configuration (Databricks)
│   ├── variables.tf             # Input variables and notebook definitions
│   ├── locals.tf                # Dynamic configuration and schema generation
│   ├── main.tf                  # Users, notebooks, directories
│   ├── groups.tf                # Groups and memberships (conditional)
│   ├── catalogs.tf              # Catalogs, schemas, permissions (conditional)
│   ├── outputs.tf               # Resource outputs
│   └── users.json               # User definitions and group assignments
├── src/                         # Python package
│   ├── __init__.py              # Package initialization
│   ├── utils.py                 # Data generation and utilities
├── course/                      # Learning materials
│   ├── notebooks/               # 16 Databricks notebooks (5 weeks)
│   └── datasets/                # Sample data (CSV, JSON, Parquet)
├── tests/                       # Test suite
├── pyproject.toml               # Poetry configuration and tool settings
├── .pre-commit-config.yaml      # Code quality automation
├── .github/workflows/deploy.yml # CI/CD pipeline
└── README.md + CLAUDE.md        # Documentation
```

## Development Workflow

### Poetry Setup and Commands
```bash
# Initial setup
poetry install                              # Install all dependencies
poetry shell                               # Activate virtual environment

# CLI tools
poetry run python -m src.cli status       # Project health check
poetry run generate-datasets              # Generate sample data
poetry run validate-notebooks             # Validate all notebooks
poetry run databricks-setup               # Configure Databricks CLI

# Development tools
poetry run pytest                         # Run test suite
poetry run black src/ tests/              # Format code
poetry run flake8 src/ tests/             # Lint code  
poetry run mypy src/                       # Type checking
poetry run pre-commit run --all-files     # All quality checks
```

### Terraform Deployment Patterns

#### Local Development (Full Control)
```bash
cd terraform
terraform init
terraform plan                            # Creates all resources
terraform apply
```

#### CI/CD Deployment (Databricks Free Edition)
```bash
# Uses existing resources via data sources
terraform plan -var="create_catalogs=false" -var="create_groups=false" -var="create_schemas=false"
terraform apply -var="create_catalogs=false" -var="create_groups=false" -var="create_schemas=false"
```

#### Configuration Variables
- `create_catalogs` (default: true) - Whether to create catalogs via Terraform
- `create_groups` (default: true) - Whether to create groups via Terraform  
- `create_schemas` (default: true) - Whether to create schemas via Terraform

## Key Design Patterns

### Conditional Resource Creation
The infrastructure supports both Databricks Free Edition (limited API) and Premium Edition:

```hcl
# Example from catalogs.tf
resource "databricks_catalog" "custom_catalogs" {
  for_each = var.create_catalogs ? toset(local.catalog_config) : toset([])
  # Resource definition...
}

data "databricks_catalog" "existing_catalogs" {
  for_each = var.create_catalogs ? toset([]) : toset(local.catalog_config)  
  # Data source definition...
}
```

### Unity Catalog Architecture
- **Catalogs**: `sales_dev`, `sales_prod`, `marketing_dev`, `marketing_prod`
- **Schemas**: `bronze` (raw data), `silver` (cleaned), `gold` (aggregated), `experiments` (marketing_dev only)
- **Tables**: Created dynamically by notebooks with proper partitioning

### Data Engineering Patterns
- **Schema-First Approach**: Explicit schemas prevent inference issues
- **Medallion Architecture**: Bronze → Silver → Gold data layers
- **Delta Lake Integration**: ACID transactions, time travel, optimization
- **Performance Optimization**: Partitioning, Z-ordering, caching strategies

## Course Curriculum Technical Details

### Week 1: Platform Mastery (4 notebooks)
Focus: Databricks fundamentals, Unity Catalog, performance optimization
- **00_databricks_fundamentals.py**: Platform architecture, runtime environments
- **01_unity_catalog_deep_dive.py**: Data governance, three-level namespace
- **02_cluster_management.py**: Autoscaling, instance types, cost optimization
- **03_spark_on_databricks.py**: DataFrames, RDD operations, performance tuning

### Week 2: Data Ingestion (4 notebooks)  
Focus: Production-grade ingestion patterns with error handling
- **04_file_ingestion.py**: CSV/JSON/Parquet with explicit schemas, data quality validation
- **05_api_ingest.py**: REST APIs, authentication, retry logic, rate limiting
- **06_database_ingest.py**: JDBC connections, incremental loading, change data capture
- **07_s3_ingest.py**: Cloud storage, partitioning strategies, data lakehouse patterns

### Week 3: Advanced Transformations (3 notebooks)
Focus: Complex Spark operations and analytics
- **08_simple_transformations.py**: Data cleaning, type conversions, business logic
- **09_window_transformations.py**: Ranking, moving averages, lead/lag functions
- **10_aggregations.py**: Complex grouping, CUBE/ROLLUP, statistical functions

### Week 4: Production Workflows (2 notebooks)
Focus: End-to-end pipeline development
- **11_file_to_aggregation.py**: Complete ETL pipeline with monitoring
- **12_api_to_aggregation.py**: Real-time data processing to insights

### Week 5: Automation (3 notebooks)
Focus: Job orchestration and production deployment
- **13_create_job_with_notebook.py**: Databricks Jobs API, scheduling, monitoring
- **14_create_job_with_wheel.py**: Python package deployment, dependency management
- **15_orchestrate_tasks_in_job.py**: Multi-task workflows, task dependencies

## Working with This Repository

### Adding New Users
1. Edit `terraform/users.json`:
```json
{
  "users": [
    {
      "user_name": "new.user@example.com",
      "display_name": "New User", 
      "groups": ["pilot"]
    }
  ]
}
```
2. Run `terraform plan && terraform apply`

### Adding New Catalogs
For Free Edition (manual catalog creation required):
1. Create catalog in Databricks UI
2. Add to `terraform/locals.tf`:
```hcl
catalog_config = [
  "sales_dev",
  "sales_prod", 
  "marketing_dev",
  "marketing_prod",
  "new_catalog"  # Add here
]
```
3. Import: `terraform import 'databricks_catalog.custom_catalogs["new_catalog"]' new_catalog`
4. Apply: `terraform plan && terraform apply`

### Adding New Notebooks
1. Create notebook file in `course/notebooks/XX_week/`
2. Update `terraform/variables.tf`:
```hcl
variable "notebooks" {
  type = map(string)
  default = {
    # ... existing notebooks ...
    "XX_week/YY_new_notebook.py" = "PYTHON"
  }
}
```
3. Deploy: `terraform plan && terraform apply`

### Modifying Course Structure
1. Update physical files in `course/notebooks/`
2. Sync `terraform/variables.tf` notebook definitions
3. Update week directories in `notebook_subdirs` if needed
4. Deploy changes with `terraform plan && terraform apply`

## Python Package Development

### Package Structure
- **src/__init__.py**: Package interface with key exports
- **src/cli.py**: Rich CLI using Click with project management commands
- **src/utils.py**: Data generation utilities and file management
- **src/validation.py**: Notebook validation and project health checks
- **tests/**: PyTest test suite with coverage reporting

### CLI Commands
```bash
poetry run python -m src.cli --help           # Show all commands
poetry run python -m src.cli status           # Project health dashboard
poetry run python -m src.cli init             # Initialize new project
poetry run python -m src.cli generate-data    # Create sample datasets
poetry run python -m src.cli validate         # Comprehensive validation
poetry run python -m src.cli metrics          # Notebook metrics and statistics
```

### Validation Framework
The validation system checks:
- **Project Structure**: Required files and directories
- **Notebook Quality**: Databricks conventions, magic commands, catalog usage
- **Code Quality**: Documentation, error handling, import organization
- **Data Quality**: File sizes, format validation, business rules

## CI/CD Pipeline

### GitHub Actions Workflow
- **Trigger**: Push to `main`, `bugfix/*` branches, manual dispatch
- **Terraform Plan**: Always runs with conditional variables
- **Terraform Apply**: Only on `main` and `bugfix/*` branches
- **Environment**: Uses GitHub Secrets for Databricks authentication

### Deployment Strategy
- **Local Development**: Full resource creation with `create_*=true`
- **CI/CD Environment**: Reference existing resources with `create_*=false`
- **Error Handling**: Comprehensive retry logic and state management

## Pre-commit Hooks Configuration

Quality automation includes:
- **Python**: Black formatting, isort imports, flake8 linting, mypy type checking
- **Terraform**: Format, validate, lint, security scanning
- **Security**: Secret detection, bandit security analysis
- **Databricks**: Custom notebook validation, catalog usage checking
- **Project**: File size limits, documentation reminders

## Troubleshooting Common Issues

### Terraform State Issues
```bash
# Reset corrupted state
terraform state list
terraform state rm <problematic_resource>
terraform import <resource_address> <resource_id>
```

### Databricks Authentication
```bash
# Reconfigure CLI
databricks configure --token --profile dev
# Or use environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
```

### Poetry Environment Issues
```bash
# Reset virtual environment  
poetry env remove python
poetry install
poetry shell
```

### Notebook Deployment Issues
1. Verify notebook paths in `terraform/variables.tf` match physical files
2. Check Databricks workspace permissions
3. Validate notebook syntax with `poetry run validate-notebooks`
4. Review Terraform plan output for resource dependencies

## Performance and Optimization

### Terraform Performance
- Use conditional resources to minimize API calls in CI/CD
- Implement resource dependencies to avoid race conditions
- Use data sources for read-only resource references

### Python Package Performance
- Poetry lock files ensure consistent dependency resolution
- Pre-commit hooks catch issues early in development cycle
- Rich CLI provides fast feedback on project status

### Notebook Performance
- Explicit schemas prevent expensive data type inference
- Delta Lake optimizations (partitioning, Z-ordering) improve query performance
- Caching strategies for frequently accessed data

## Claude Code MCP (Model Context Protocol) Integration

This project includes configuration for enhanced Claude Code functionality through MCP servers that provide additional capabilities for repository management and analysis.

### MCP Server Configuration

The project supports MCP servers for:
- **GitHub Integration**: Repository management, issues, PRs, and workflow monitoring
- **Enhanced Filesystem Access**: Advanced file analysis and search across the project

### Setup Instructions

#### 1. MCP Configuration Template
```bash
# Copy the MCP template (if available)
cp .vscode/mcp.json.template .vscode/mcp.json
```

#### 2. GitHub Token Configuration
1. **Create GitHub Personal Access Token**:
   - Visit: https://github.com/settings/tokens
   - Generate new token with scopes: `repo`, `workflow`, `actions:read`
2. **Configure token in mcp.json**:
   - Replace `your-github-token-here` with your actual token
   - Ensure `.vscode/mcp.json` is gitignored (it is by default)

#### 3. VS Code Setup
1. **Restart VS Code** to load MCP servers
2. **Auto-installation**: MCP servers install automatically when Claude Code needs them
3. **Manual installation** (if needed):
   ```bash
   npx -y @github/mcp-server-github
   npx -y @modelcontextprotocol/server-filesystem
   ```

### Available MCP Capabilities

#### GitHub Server Features
- **Repository Management**: Access to issues, pull requests, and repository metadata
- **Workflow Monitoring**: GitHub Actions status and deployment tracking
- **Code Review**: Enhanced pull request analysis and review capabilities
- **Project Management**: Issue creation and tracking for course development

**Usage Examples**:
- "Check the status of our latest GitHub Actions deployment"
- "Create an issue for the notebook validation bug in Week 2"
- "Analyze recent pull requests for terraform changes"

#### Filesystem Server Features  
- **Advanced Search**: Pattern-based file discovery across the project
- **Code Analysis**: Structure analysis of Terraform configurations and notebooks
- **Content Search**: Find specific Spark functions or Databricks patterns
- **Project Insights**: Comprehensive project structure understanding

**Usage Examples**:
- "Find all notebooks that use Unity Catalog three-level namespace"
- "Analyze the structure of our Terraform conditional resource patterns"
- "Search for Delta Lake optimization patterns across course materials"

### Security Considerations

#### Local Development
- **mcp.json is gitignored**: Prevents exposure of local file paths and tokens
- **Relative paths only**: Template uses `.` for security when sharing
- **Token isolation**: GitHub tokens remain local to development environment

#### Public Repository Sharing
- **Use template only**: Always use mcp.json.template for public contributions
- **No absolute paths**: Avoid exposing local filesystem structure
- **Secret management**: Never commit actual tokens or workspace URLs

### Extending MCP for Project-Specific Needs

#### Custom Server Ideas
1. **Terraform State Inspector**: MCP server for terraform state analysis
2. **Course Content Manager**: Specialized server for notebook management
3. **Databricks Workspace Integration**: Direct workspace API access
4. **Documentation Search**: Enhanced search across all project documentation

#### Implementation Pattern
```typescript
// Example custom MCP server structure
const server = new Server({
  name: "databricks-course-manager",
  version: "1.0.0"
});

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: "validate_notebook",
      description: "Validate Databricks notebook structure",
      inputSchema: { /* ... */ }
    }
  ]
}));
```

### Troubleshooting MCP Issues

#### Server Installation Problems
```bash
# Clear npm cache and reinstall
npm cache clean --force
npx -y @github/mcp-server-github
npx -y @modelcontextprotocol/server-filesystem
```

#### Permission Issues
- Ensure Claude Code has filesystem access permissions
- Verify npm/npx execution permissions
- Check VS Code MCP server connection status

#### Configuration Validation
```bash
# Validate JSON syntax
cat .vscode/mcp.json | python -m json.tool

# Check server connectivity (if available)
claude-code mcp status
```

The MCP integration enhances Claude Code's ability to understand and work with this Databricks infrastructure project, providing deeper insights into code structure, deployment status, and development workflows.

---

This technical documentation provides the foundation for maintaining and extending the Databricks infrastructure project while ensuring code quality, security, and performance best practices.