# MkDocs Website Setup - Quick Start

Your Databricks course now has a beautiful documentation website! 🎉

## ✅ What's Been Set Up

- ✅ MkDocs Material theme installed
- ✅ Complete site configuration (`mkdocs.yml`)
- ✅ Documentation structure created (`docs/`)
- ✅ Navigation with 6 main sections
- ✅ Responsive design (mobile + desktop)
- ✅ Dark/light theme toggle
- ✅ Search functionality
- ✅ Code syntax highlighting
- ✅ Custom Databricks branding colors

## 🚀 Quick Start (3 commands)

### 1. Preview Locally

```bash
poetry run mkdocs serve
```

Open http://127.0.0.1:8000 in your browser

### 2. Check the Site

Navigate through:
- Home page
- Getting Started (Data Engineer / Platform Engineer guides)
- Course Content (Week 1-5 + Advanced)
- Infrastructure docs
- Technical Reference

### 3. Deploy to GitHub Pages

```bash
poetry run mkdocs gh-deploy
```

Your site will be live at: `https://datatribe-collective-labs.github.io/databricks-infra/`

---

## 📁 Directory Structure

```
databricks-infra/
├── mkdocs.yml                    # Main configuration
├── docs/                         # All documentation content
│   ├── index.md                  # Home page (from README.md)
│   ├── getting-started/          # User guides
│   │   ├── index.md              # Choose your path
│   │   ├── data-engineer.md      # For students
│   │   └── platform-engineer.md  # For admins
│   ├── course/                   # Course content
│   │   ├── index.md              # Course overview
│   │   ├── week1/                # Week 1 content
│   │   ├── week2/                # Week 2 content
│   │   ├── week3/                # Week 3 content
│   │   ├── week4/                # Week 4 content
│   │   ├── week5/                # Week 5 content
│   │   └── advanced/             # Advanced content
│   ├── infrastructure/           # Infrastructure docs
│   │   ├── architecture.md       # From CLAUDE.md
│   │   └── unity-catalog.md      # Catalog structure
│   ├── reference/                # Technical reference
│   │   └── troubleshooting.md    # Common issues
│   ├── assets/                   # Images, logos
│   └── stylesheets/              # Custom CSS
│       └── extra.css             # Databricks colors
└── site/                         # Generated site (gitignored)
```

---

## 🎨 Features

### Material Theme

- **Modern UI** - Clean, professional design
- **Responsive** - Works on all devices
- **Dark Mode** - Toggle between light/dark themes
- **Search** - Instant search across all content
- **Navigation** - Tabbed navigation + sidebar
- **Code Blocks** - Syntax highlighting for Python, SQL, Terraform

### Custom Styling

- **Databricks Colors** - Orange primary (#FF3621) and accent colors
- **Code Highlighting** - Border on code blocks for emphasis
- **Hover Effects** - Interactive cards with hover states

### Navigation Structure

1. **Home** - Repository overview
2. **Getting Started** - Choose your learning path
3. **Course Content** - 19 notebooks organized by week
4. **Infrastructure** - Terraform and Unity Catalog docs
5. **Technical Reference** - Developer guides and troubleshooting

---

## 📝 Next Steps

### Complete the Documentation

Currently, the site has:
- ✅ Main structure and navigation
- ✅ Index pages for all sections
- ⚠️ Placeholder pages for individual notebooks

To finish:

1. **Create Notebook Documentation Pages**

   For each notebook (e.g., `01_databricks_fundamentals.py`), create a markdown page:

   ```markdown
   # Databricks Fundamentals

   Introduction to the Databricks platform...

   ## Learning Objectives
   - Understand workspace navigation
   - Master notebook features
   - ...

   ## Key Concepts
   ...

   ## Hands-On Exercises
   ...
   ```

2. **Add Screenshots**

   Place images in `docs/assets/` and reference them:

   ```markdown
   ![Databricks Workspace](../assets/workspace-screenshot.png)
   ```

3. **Update Links**

   The current site has some placeholder links that point to non-existent pages. These will show warnings during build but won't break the site.

### Customize Further

**Change Colors:**

Edit `docs/stylesheets/extra.css`:

```css
:root {
  --md-primary-fg-color: #YOUR_COLOR;
  --md-accent-fg-color: #YOUR_ACCENT;
}
```

**Add Logo:**

1. Place logo in `docs/assets/databricks-logo.png`
2. Logo will appear in top-left corner

**Add Analytics:**

Edit `mkdocs.yml` to add Google Analytics:

```yaml
extra:
  analytics:
    provider: google
    property: G-XXXXXXXXXX
```

---

## 🔧 Common Commands

```bash
# Preview site locally with live reload
poetry run mkdocs serve

# Build static site
poetry run mkdocs build

# Deploy to GitHub Pages
poetry run mkdocs gh-deploy

# Build with strict mode (fail on warnings)
poetry run mkdocs build --strict

# Serve on different port
poetry run mkdocs serve -a localhost:8001
```

---

## 🌐 Enable GitHub Pages

After your first deployment:

1. Go to https://github.com/datatribe-collective-labs/databricks-infra/settings/pages
2. Under **Source**, select:
   - Branch: `gh-pages`
   - Folder: `/ (root)`
3. Click **Save**
4. Wait 1-2 minutes for deployment
5. Visit: https://datatribe-collective-labs.github.io/databricks-infra/

---

## 📚 Documentation

- **Full Deployment Guide**: `docs/DEPLOYMENT.md`
- **MkDocs Official Docs**: https://www.mkdocs.org/
- **Material Theme Docs**: https://squidfunk.github.io/mkdocs-material/
- **GitHub Pages Setup**: https://docs.github.com/en/pages

---

## ✨ What Students Will See

Once deployed, students can:

1. Browse course content organized by week
2. Search for specific topics instantly
3. Toggle between light/dark themes
4. View on mobile devices
5. Navigate easily with sidebar + breadcrumbs
6. Copy code snippets with one click
7. See clear learning paths based on their role

---

## 🎉 You're All Set!

Try it out:

```bash
poetry run mkdocs serve
```

Then visit: http://127.0.0.1:8000

When you're happy with the site:

```bash
poetry run mkdocs gh-deploy
```

Your beautiful documentation site is ready to share! 🚀
