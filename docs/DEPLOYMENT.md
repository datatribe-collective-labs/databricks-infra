# MkDocs Deployment Guide

Complete guide for deploying the Databricks course documentation website.

## Local Development

### Preview the Site Locally

```bash
# Start development server with live reload
poetry run mkdocs serve

# Site will be available at: http://127.0.0.1:8000
```

The development server automatically rebuilds the site when you edit documentation files.

### Build the Site

```bash
# Build static site to site/ directory
poetry run mkdocs build

# Build with warnings treated as errors
poetry run mkdocs build --strict
```

---

## Deploy to GitHub Pages

### One-Command Deployment

```bash
# Deploy directly to GitHub Pages (gh-pages branch)
poetry run mkdocs gh-deploy

# With custom commit message
poetry run mkdocs gh-deploy -m "Update documentation for Week 5"
```

This command:
1. Builds the documentation
2. Commits to `gh-pages` branch
3. Pushes to GitHub
4. GitHub Pages automatically serves the site

### Manual Deployment

If you prefer manual control:

```bash
# 1. Build the site
poetry run mkdocs build

# 2. Deploy site/ directory to gh-pages branch
git checkout gh-pages
cp -r site/* .
git add .
git commit -m "Update documentation"
git push origin gh-pages
git checkout main
```

---

## GitHub Pages Configuration

### Enable GitHub Pages

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Pages**
3. Under **Source**, select:
   - Branch: `gh-pages`
   - Folder: `/ (root)`
4. Click **Save**

### Custom Domain (Optional)

If you want a custom domain like `docs.yourdomain.com`:

1. Add `docs.yourdomain.com` CNAME record pointing to `yourusername.github.io`
2. In repository settings, add custom domain
3. Enable "Enforce HTTPS"

---

## Automated Deployment with GitHub Actions

### Option 1: Deploy on Push to Main

Create `.github/workflows/docs.yml`:

```yaml
name: Deploy Documentation

on:
  push:
    branches:
      - main
    paths:
      - 'docs/**'
      - 'mkdocs.yml'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install Poetry
        run: |
          curl -sSL https://install.python-poetry.org | python3 -
          echo "$HOME/.local/bin" >> $GITHUB_PATH

      - name: Install dependencies
        run: poetry install --only dev

      - name: Deploy to GitHub Pages
        run: |
          git config user.name github-actions
          git config user.email github-actions@github.com
          poetry run mkdocs gh-deploy --force
```

### Option 2: Deploy on Release

Trigger deployment only when you create a release:

```yaml
on:
  release:
    types: [published]
```

---

## Troubleshooting

### Build Warnings

If you see warnings about missing files:

```bash
# These are expected for placeholder pages
# To see warnings:
poetry run mkdocs build

# To fail on warnings (strict mode):
poetry run mkdocs build --strict
```

### Port Already in Use

If `mkdocs serve` fails with "Address already in use":

```bash
# Use different port
poetry run mkdocs serve -a localhost:8001
```

### GitHub Pages Not Updating

1. Check **Settings** → **Pages** shows correct branch (`gh-pages`)
2. Verify `gh-pages` branch exists and has recent commits
3. Check **Actions** tab for deployment status
4. Clear browser cache (Ctrl+Shift+R)
5. Wait 1-2 minutes for GitHub's CDN to update

### Permission Denied on gh-deploy

```bash
# Ensure you have push access to repository
git remote -v

# Re-authenticate if needed
gh auth login
```

---

## Site URLs

After deployment, your documentation will be available at:

**GitHub Pages:** `https://yourusername.github.io/databricks-infra/`

Update `site_url` in `mkdocs.yml` to match your actual URL:

```yaml
site_url: https://datatribe-collective-labs.github.io/databricks-infra/
```

---

## Content Updates

### Adding New Pages

1. Create markdown file in appropriate `docs/` subdirectory
2. Add to navigation in `mkdocs.yml`
3. Build and preview locally
4. Deploy when ready

### Updating Existing Pages

1. Edit markdown files in `docs/`
2. Preview changes with `mkdocs serve`
3. Commit and push (auto-deploys if GitHub Actions configured)

### Adding New Week Content

When course content is updated:

```bash
# 1. Update notebook documentation in docs/course/weekX/
# 2. Add links in mkdocs.yml navigation
# 3. Preview locally
poetry run mkdocs serve

# 4. Deploy
poetry run mkdocs gh-deploy
```

---

## Performance Tips

### Optimize Build Time

For faster builds during development:

```bash
# Skip search index generation
poetry run mkdocs serve --no-livereload
```

### Optimize Images

If adding images to `docs/assets/`:

- Use WebP format when possible
- Compress images before adding
- Keep images under 500KB

---

## Next Steps

1. **Preview locally**: `poetry run mkdocs serve`
2. **Check everything looks good**: Navigate through all pages
3. **Deploy**: `poetry run mkdocs gh-deploy`
4. **Verify**: Visit your GitHub Pages URL
5. **Share**: Give the URL to students!

---

## Support

- **MkDocs Documentation**: https://www.mkdocs.org/
- **Material Theme Docs**: https://squidfunk.github.io/mkdocs-material/
- **GitHub Pages Docs**: https://docs.github.com/en/pages
