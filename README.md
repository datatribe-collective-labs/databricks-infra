# Databricks Infrastructure & Learning Platform

A complete Databricks learning platform with Infrastructure as Code (Terraform) and a comprehensive 5-week data engineering course.

## 🎯 What This Project Offers

- **📚 Complete Databricks Course**: 16 hands-on notebooks covering fundamentals to advanced job orchestration
- **🏗️ Infrastructure as Code**: Terraform configuration for Unity Catalog, users, and permissions
- **🐍 Professional Python Setup**: Poetry dependency management with CLI tools and testing
- **🔧 Development Tools**: Pre-commit hooks, automated testing, and code quality checks
- **🚀 CI/CD Ready**: GitHub Actions for automated deployment and validation

## 🚀 Choose Your Path

### 👨‍💻 I'm a Data Engineer
*"I want to learn Databricks through hands-on notebooks and work with data"*

**➡️ [Follow the Data Engineer Guide](./DataEngineer-readme.md)**

- Focus on learning through 16 course notebooks
- Work directly in Databricks workspace
- No infrastructure complexity
- Start learning immediately

---

### 🏗️ I'm a Data Platform Engineer  
*"I want to manage Databricks infrastructure with Terraform and control the full stack"*

**➡️ [Follow the Data Platform Engineer Guide](./DataPlatformEngineer-readme.md)**

- Manage infrastructure with Terraform
- Control users, groups, and permissions
- Set up CI/CD pipelines
- Full development environment

---

## 📚 Course Overview (5 Weeks, 16 Notebooks)

| Week | Focus | Notebooks | Key Skills |
|------|-------|-----------|------------|
| **1** | Databricks Fundamentals | 4 | Platform mastery, Unity Catalog, cluster management |
| **2** | Data Ingestion | 4 | Files, APIs, databases, cloud storage patterns |  
| **3** | Data Transformations | 3 | Advanced Spark operations, window functions |
| **4** | End-to-End Workflows | 2 | Complete pipeline development |
| **5** | Job Orchestration | 3 | Production automation and monitoring |

## 🏗️ Current Infrastructure

This repository is configured for production deployment:

- **Workspace**: Premium Edition with full Terraform management
- **CI/CD**: Automated deployments via GitHub Actions
- **Users**: 8 users with full catalog access
- **Catalogs**: 4 catalogs (sales_dev, sales_prod, marketing_dev, marketing_prod)
- **Schemas**: 13 schemas with medallion architecture (bronze, silver, gold + experiments)
- **Course Content**: 21 notebooks automatically deployed

## 📞 Support

- **🐛 Issues**: Use GitHub Issues for bugs and feature requests
- **📖 Technical Docs**: See [CLAUDE.md](./CLAUDE.md) for detailed technical guidance
- **💬 Questions**: Check notebook troubleshooting sections

## 📈 Project Status

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/chanukyapekala/databricks-infra/deploy.yml?branch=main)
![Poetry](https://img.shields.io/badge/dependency%20manager-poetry-blue)
![Terraform](https://img.shields.io/badge/infrastructure-terraform-purple)

**Choose your path above and start your Databricks journey!** 🚀