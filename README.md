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