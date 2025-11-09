# Troubleshooting

Common issues and solutions for the Databricks course and infrastructure.

## Course Issues

### Notebooks Not Loading

**Problem:** Notebooks don't appear in `/Shared/terraform-managed/course/notebooks/`

**Solution:**
```bash
# Verify Terraform deployment
cd terraform
terraform plan

# Re-deploy if needed
terraform apply
```

### Schema Permission Errors

**Problem:** "Permission denied" when writing to schemas

**Solution:**
- Check you're using `%run ../utils/user_schema_setup`
- Verify your user schema exists: `SHOW SCHEMAS IN databricks_course`
- Contact admin if schema not provisioned

### Serverless Compute Issues

**Problem:** Cluster config options not available (Free Edition)

**Solution:**
- Free Edition only supports serverless compute
- No cluster configuration needed
- All notebooks work on serverless
- See [Week 1 - Cluster Management](../course/week1/03-cluster-management.md) for details

## Infrastructure Issues

See [Platform Engineer Guide](../getting-started/platform-engineer.md#troubleshooting) for Terraform and deployment issues.
