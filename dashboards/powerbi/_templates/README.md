# Power BI Templates

Shared starting points for SunCulture Power BI dashboards.

| Path | What it is | Status |
|---|---|---|
| `BRAND.md` | How the brand manual applies to dashboards: layout, logo, colour, type, writing | Ready (draft) |
| `STANDARDS.md` | Build and review checklist | Ready |
| `themes/sunculture.json` | Report theme built from the brand manual (colour-blind-tested chart palette, Arial) | Ready |
| `assets/sunculture-logo-stacked.png` | Primary logo, taken from the SunCulture presentation template | Ready |
| `assets/header-sidebar.png` | Header bar (white with the brand accent line) for pages with the navigation sidebar, 1072 px wide | Ready |
| `assets/nav-icons/` | Line icons for sidebar buttons (`currentColor`; the tool colours them) | Ready |
| `tools/sidebar_nav.py` | Adds or rebuilds the navigation sidebar on every page of a `.pbip` | Ready |
| `starter/` | Blank .pbip project: Date table, `_Measures` table, theme applied, 1280×720 overview/analysis/detail pages with header background | To do |
| `tools/` (review) | Review scripts (model size, unused columns, broken references) | To do |

## Applying the theme to an existing report

In Power BI Desktop: **View → Themes → Browse for themes** → select `themes/sunculture.json`.

## Adding the navigation sidebar

For any dashboard with more than one page (spec: `BRAND.md`, *Navigation sidebar*):

1. Write a `nav.json` next to the `.pbip` listing the sections in order. Every page must appear once; `icon` is a file name from `assets/nav-icons/` without `.svg`. Example: `../ug/ops/nav.json`.

   ```json
   {"sections": [
     {"label": null, "pages": [{"page": "Overview", "icon": "grid"}]},
     {"label": "ENGINEERING", "pages": [{"page": "Installations", "icon": "wrench"}]}
   ]}
   ```

2. Close the report in Power BI Desktop (otherwise Desktop saves its copy over the change), then run from `dashboards/powerbi/`:

   ```
   python -I _templates/tools/sidebar_nav.py "<country>/<domain>/<Name>.Report" "<country>/<domain>/nav.json"
   ```

3. Reopen the `.pbip` and check each page.

The first run on a page moves its content into the area right of the sidebar (x = 224 to 1256, widths scaled to fit) and points a full-width header image at `header-sidebar.png`. Later runs only redraw the sidebar, so rerun the tool whenever pages are added, removed or renamed. The tool stops if `nav.json` and the report's pages don't match, or if an icon doesn't exist. Icons available: `assets/nav-icons/`.

## Starting a new dashboard (once `starter/` exists)

1. Copy `starter/` to `dashboards/powerbi/<country>/<domain>/` and rename the `.pbip`, `.Report` and `.SemanticModel` names.
2. Open the `.pbip` in Power BI Desktop and point the queries at the right dataflow entities.
3. Build against `STANDARDS.md`. Ask Claude to review against it before raising the PR.

## Working with Claude on .pbip files

A `.pbip` saves the report as JSON (`*.Report/definition/`) and the model as TMDL text (`*.SemanticModel/definition/`), so Claude can read, edit and review them like code. See `../CLAUDE.md` for the rules Claude follows in this folder.
