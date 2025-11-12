# Assets Directory

This directory contains images and assets for the MkDocs documentation site.

## Logo Setup

### Upload Your Logo Here

**Required files:**

1. **Logo (for header):**
   - Filename: `databricks-logo.png`
   - Recommended size: 120x40px to 200x60px
   - Format: PNG with transparent background
   - Shows in top-left corner of navigation

2. **Favicon (browser tab icon):**
   - Filename: `favicon.png`
   - Recommended size: 32x32px or 64x64px
   - Format: PNG or ICO
   - Shows in browser tab

### How to Add Your Logo

**Option 1: Drag and Drop (GitHub)**
1. Navigate to this folder in GitHub: `docs/assets/`
2. Drag your `databricks-logo.png` file into the browser
3. Commit the file

**Option 2: Command Line**
```bash
# From project root
cp /path/to/your/logo.png docs/assets/databricks-logo.png
cp /path/to/your/favicon.png docs/assets/favicon.png

# Commit
git add docs/assets/*.png
git commit -m "Add logo and favicon"
```

**Option 3: Direct File Upload**
1. Click "Add file" → "Upload files" in GitHub
2. Upload `databricks-logo.png` and `favicon.png`
3. Commit changes

### Logo Specifications

**Recommended dimensions:**
- **Logo**: 120-200px wide, 40-60px tall
- **Favicon**: 32x32px or 64x64px square

**File formats:**
- PNG (transparent background recommended)
- SVG (scalable, best quality)

**File size:**
- Keep under 100KB for fast loading
- Optimize with tools like TinyPNG or ImageOptim

### Current Configuration

Your logo is already configured in `mkdocs.yml`:

```yaml
theme:
  name: material
  logo: assets/databricks-logo.png    # ← Your logo here
  favicon: assets/favicon.png          # ← Your favicon here
```

Once you upload the files, they'll automatically appear!

### Preview Your Logo

After uploading:

```bash
# Preview locally
poetry run mkdocs serve

# Visit http://127.0.0.1:8000
# Logo should appear in top-left corner
```

### Need Help?

If you need to:
- **Change logo size**: Edit CSS in `docs/stylesheets/extra.css`
- **Use different filename**: Update `mkdocs.yml` theme section
- **Remove logo**: Delete the logo lines from `mkdocs.yml`

---

## Other Assets

You can also add other images here for use in documentation:

```markdown
<!-- Reference in any markdown file -->
![Description](../assets/your-image.png)
```

### Recommended Image Sizes

- **Screenshots**: 1200-1600px wide
- **Diagrams**: 800-1200px wide
- **Icons**: 64-128px square
- **Banners**: 1200x400px

### Image Optimization

Before uploading images:
- Compress with [TinyPNG](https://tinypng.com/)
- Convert to WebP for better performance
- Keep under 500KB per image
