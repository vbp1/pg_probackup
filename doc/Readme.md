# pg_probackup Documentation

This directory contains the source files for pg_probackup documentation in DocBook XML format.

## Online Documentation

The documentation is automatically published to GitHub Pages:
https://vbp1.github.io/pg_probackup

## Generating Documentation Locally

```bash
# Validate XML (optional)
xmllint --noout --valid probackup.xml

# Generate HTML
xsltproc stylesheet.xsl probackup.xml > index.html
```

> [!NOTE]
> Install `docbook-xsl` if you get:
> ```
> "xsl:import : unable to load http://docbook.sourceforge.net/release/xsl/current/xhtml/docbook.xsl"
> ```
>
> On Debian/Ubuntu: `sudo apt-get install docbook-xsl`
> On RHEL/CentOS: `sudo yum install docbook-style-xsl`

## Files

- `probackup.xml` - Main DocBook document
- `pgprobackup.xml` - Detailed documentation content
- `stylesheet.xsl` - XSLT stylesheet for HTML generation
- `stylesheet.css` - CSS styles for HTML output
- `404.html` - Custom 404 error page for GitHub Pages

## Automatic Deployment

Documentation is automatically deployed to GitHub Pages when changes are pushed to the `master` branch. The deployment is handled by the `.github/workflows/docs.yml` workflow.

The workflow:
1. Generates HTML from DocBook XML using `xsltproc`
2. Deploys to GitHub Pages using the modern `actions/deploy-pages` action
