# Terraform Configuration

## User Management

### Understanding User Files

This project uses a **smart file loading pattern** for user configuration:

- **`users.json`** - Example data (committed to repo) ✅
  - Contains dummy emails like `admin@example.com`
  - Shows structure for learning purposes
  - Safe to share publicly

- **`users.local.json`** - Real data (gitignored) 🔒
  - Contains your actual user emails
  - Not committed to repository
  - Used automatically if present

### How Terraform Loads Users

Terraform automatically checks:
1. **Does `users.local.json` exist?** → Use it (real deployment)
2. **No local file?** → Fall back to `users.json` (example mode)

### Setup for Your Environment

**Option 1: Quick Start (for real deployment)**
```bash
# Copy example to local file
cp users.json users.local.json

# Edit with your real user emails
vim users.local.json
```

**Option 2: Manual Creation**
Create `terraform/users.local.json`:
```json
{
  "users": [
    {
      "user_name": "your.email@company.com",
      "display_name": "Your Name",
      "groups": ["admins"]
    },
    {
      "user_name": "student@company.com",
      "display_name": "Student Name",
      "groups": ["students"]
    }
  ]
}
```

### User Groups

- **admins**: Full access to all catalogs, schemas, and admin privileges
- **students**: Limited access, can only write to their own schema

### Important Notes

- ✅ `users.json` - Example data, safe to commit
- 🔒 `users.local.json` - Real data, automatically gitignored
- 🎯 Terraform uses local file if exists, otherwise example file
- 👤 Each user automatically gets: `databricks_course.{username}` schema

### Deployment

```bash
cd terraform

# Review what will be deployed (uses example data if no local file)
terraform plan

# Deploy with your real data (create users.local.json first)
terraform apply
```

### Adding New Users

Edit `users.local.json`:
```json
{
  "user_name": "newuser@company.com",
  "display_name": "New User",
  "groups": ["students"]
}
```

Then apply:
```bash
terraform apply
```

Terraform will automatically:
- Create user account
- Add to appropriate group
- Create personal schema
- Configure permissions

### Removing Users

Remove user entry from `users.local.json` and run:
```bash
terraform apply
```

⚠️ **Warning**: User schema and all data will be permanently deleted.

## More Information

See the [main README](../README.md) for complete setup instructions.