# DataTribe - Databricks Learning Platform

> **Production-Ready Infrastructure as Code + Comprehensive Data Engineering Course**

A complete Databricks learning platform combining:
- 🎓 **27 hands-on notebooks** (fundamentals to production apps + data modelling)
- 🏗️ **Terraform automation** for Unity Catalog, users, permissions, and RBAC
- 🚀 **Zero-setup learning** for students + 15-minute deployment for admins

---

## 🌐 Get Started

**👉 Visit the Web UI:**
- **🌍 Live Site**: [https://datatribe-collective-labs.github.io/databricks-infra](https://datatribe-collective-labs.github.io/databricks-infra) (GitHub Pages)
- **📁 Local**: [Open index.html](./web/index.html)

The web UI provides:
- **Student Guide** - Get workspace access and start learning in 3 steps
- **Admin Guide** - Deploy complete infrastructure in 15 minutes
- **Course Curriculum** - Browse all 27 notebooks organized by module
- **🌙 Day/Night Mode** - Toggle theme for comfortable viewing
- **📱 Mobile-Friendly** - Responsive design with hamburger menu

---

## 📁 Repository Structure

```
databricks-infra/
├── web/                            # Web UI (START HERE)
│   ├── index.html                  # Main landing page
│   ├── data-engineer.html          # Student guide
│   ├── platform-engineer.html      # Admin guide
│   ├── curriculum.html             # Course curriculum
│   └── styles.css                  # Shared styles
├── README.md                       # This file
├── CLAUDE.md                       # Technical docs for AI assistance
├── docs/                           # Reference documentation
│   ├── DataEngineer-readme.md      # Detailed student guide
│   ├── DataPlatformEngineer-readme.md  # Detailed admin guide
│   ├── USER_SCHEMA_GUIDE.md        # User isolation technical guide
│   └── assets/                     # Logo and images
├── course/                         # Course content
│   ├── notebooks/                  # 27 Databricks notebooks
│   └── datasets/                   # Sample data files
├── terraform/                      # Infrastructure as Code
│   ├── main.tf, groups.tf, catalogs.tf
│   ├── users.json                  # User configuration
│   └── versions.tf                 # Provider config
├── src/                            # Python package
│   ├── cli.py                      # CLI tools
│   └── utils.py                    # Utilities
└── tests/                          # Test suite
```

---

## 🎯 Quick Links

### For Students
- 🌐 **Web Guide**: [web/data-engineer.html](./web/data-engineer.html)
- 📖 **Detailed Docs**: [docs/DataEngineer-readme.md](./docs/DataEngineer-readme.md)
- 🔗 **Workspace**: https://dbc-d8111651-e8b1.cloud.databricks.com

### For Admins
- 🌐 **Web Guide**: [web/platform-engineer.html](./web/platform-engineer.html)
- 📖 **Detailed Docs**: [docs/DataPlatformEngineer-readme.md](./docs/DataPlatformEngineer-readme.md)
- 🔧 **Technical Reference**: [CLAUDE.md](./CLAUDE.md)

### Course Content
- 🌐 **Curriculum**: [web/curriculum.html](./web/curriculum.html)
- 📁 **Notebooks**: [course/notebooks/](./course/notebooks/)

---

## 📊 What's Included

### Course Structure

**Foundational Knowledge:**
- **Week 1**: Databricks Fundamentals (5 notebooks)
- **Foundations**: Data Modelling Patterns (4 notebooks)

**Applied Learning:**
- **Week 2**: Data Ingestion (5 notebooks)
- **Week 3**: Advanced Transformations (4 notebooks)
- **Week 4**: End-to-End Workflows (3 notebooks)
- **Week 5**: Production Deployment (4 notebooks)

**Advanced Topics:**
- **Advanced**: Databricks Apps with Streamlit (2 notebooks)

### Infrastructure
- **8 users** with role-based access control
- **5 Unity Catalogs** (sales, marketing, course)
- **24 schemas** (medallion architecture: bronze, silver, gold)
- **User isolation** - each student gets personal workspace
- **CI/CD pipeline** - automated deployment via GitHub Actions

---

## 🚀 Quick Start Commands

### Data Engineers
```bash
# Open the web UI to get workspace access
open web/index.html
# Then navigate to: /Shared/terraform-managed/course/notebooks/ in Databricks
```

### Data Platform Engineers
```bash
# Clone and setup
git clone https://github.com/chanukyapekala/databricks-infra
cd databricks-infra
poetry install

# Configure authentication (requires workspace admin)
databricks configure --token --profile datatribe

# Deploy infrastructure
cd terraform
terraform init
terraform apply
```

---

## 🛠️ User Management CLI

**For Admins**: Manage users efficiently with the built-in CLI tool.

### Quick Reference

```bash
# View all users
poetry run user-list

# Get detailed user info
poetry run user-get
# Then type email when prompted

# Add new user (interactive)
poetry run user-add

# Add multiple users
poetry run user-add-batch

# Remove user
poetry run user-remove

# Infrastructure status
poetry run user-status
```

### Examples

**Add a new admin user:**
```bash
$ poetry run user-add
Email address: new.user@example.com
Display name: New User
Group (admins/students): admins

📝 User will be created with:
   Email: new.user@example.com
   Name: New User
   Group: platform_admins
   Schema: databricks_course.new_user

Add this user? [Y/n]: y
✅ Added user: new.user@example.com

📌 Next steps:
   cd terraform
   terraform apply ...
```

**View specific user details:**
```bash
$ echo "user@example.com" | poetry run user-get
👤 User Details
============================================================
📧 Email: user@example.com
👤 Display Name: User Name
👥 Group: platform_admins
📁 Personal Schema: databricks_course.user

🔐 Permissions:
   ✅ ALL_PRIVILEGES on all catalogs and schemas
   ...
```

**Filter user list:**
```bash
# Find specific user
poetry run user-list | grep user@example.com
```

### Features
- ✅ **Email validation** - Prevents invalid emails
- ✅ **Duplicate detection** - Won't add existing users
- ✅ **Preview before commit** - See what will be created
- ✅ **Safe removal** - Confirmation prompts for deletions
- ✅ **Auto-deployment** - Optional `--apply` flag to deploy immediately
- ✅ **Batch operations** - Add multiple users at once

### What Gets Created
When you add a user, Terraform automatically creates:
1. Databricks user account
2. Group membership (platform_admins or platform_students)
3. Personal schema in `databricks_course` catalog
4. Appropriate permissions based on group
5. Access to all 27 course notebooks

---

## 🔐 Request Workspace Access

### For Students
Visit the web UI and click "Request Workspace Access" - you'll be directed to a Google Form to submit your request. We'll respond within 24 hours.

### For Admins: Google Form Setup
The web UI uses a Google Form for access requests (replacing the old broken mailto: link). To set up:

1. **Create Google Form** at https://forms.google.com with fields:
   - Full Name (short answer, required)
   - Email Address (short answer with email validation, required)
   - Background (multiple choice: Student/Professional/Academic/Other)
   - Why do you want to learn Databricks? (paragraph, required)
   - GDPR Consent checkbox (required)

2. **Configure Settings**:
   - Enable "Collect email addresses"
   - Enable "Limit to 1 response" (optional)
   - Set confirmation message: "Thank you! We'll contact you within 24 hours."

3. **Get Form URL**: Click Send → Link icon → Copy URL

4. **Update Web UI**: Edit `web/data-engineer.html` line 220 and replace `YOUR_FORM_ID_HERE` with your actual form URL

5. **Enable Notifications**: In form settings, enable email notifications for new responses

**Benefits**: No email client required, mobile-friendly, structured data collection, GDPR compliant

---

## 📞 Support

- **🐛 Issues**: Use [GitHub Issues](https://github.com/datatribe-collective-labs/databricks-infra/issues)
- **📖 Technical Docs**: See [CLAUDE.md](./CLAUDE.md) for AI-assisted development
- **🔍 Troubleshooting**: Check guides in [docs/](./docs/)
- **💬 Contact**: Reach out via Discord data-engg channel here: [DataTribe Discord](https://discord.gg/rmzqksHy)

---

## 🏷️ Project Status

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/datatribe-collective-labs/databricks-infra/deploy.yml?branch=main)
![Poetry](https://img.shields.io/badge/dependency%20manager-poetry-blue)
![Terraform](https://img.shields.io/badge/infrastructure-terraform-purple)

---

**🎓 Ready to learn? 🏗️ Ready to deploy? Start your Databricks journey with DataTribe today! 🚀**