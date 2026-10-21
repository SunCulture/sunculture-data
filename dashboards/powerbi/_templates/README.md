# Power BI Templates

Shared starting points for SunCulture Power BI dashboards.

| Path | What it is | Status |
|---|---|---|
| `BRAND.md` | How the brand manual applies to dashboards: layout, logo, colour, type, writing | Ready (draft) |
| `STANDARDS.md` | Build and review checklist | Ready |
| `themes/sunculture.json` | Report theme built from the brand manual (colour-blind-tested chart palette, Arial) | Ready |
| `assets/sunculture-logo-stacked.png` | Primary logo, taken from the SunCulture presentation template | Ready |
| `starter/` | Blank .pbip project: Date table, `_Measures` table, theme applied, 1280×720 overview/analysis/detail pages with header background | To do |
| `tools/` | Review scripts (model size, unused columns, broken references) | To do |

## Applying the theme to an existing report

In Power BI Desktop: **View → Themes → Browse for themes** → select `themes/sunculture.json`.

## Starting a new dashboard (once `starter/` exists)

1. Copy `starter/` to `dashboards/powerbi/<country>/<domain>/` and rename the `.pbip`, `.Report` and `.SemanticModel` names.
2. Open the `.pbip` in Power BI Desktop and point the queries at the right dataflow entities.
3. Build against `STANDARDS.md`. Ask Claude to review against it before raising the PR.

## Working with Claude on .pbip files

A `.pbip` saves the report as JSON (`*.Report/definition/`) and the model as TMDL text (`*.SemanticModel/definition/`), so Claude can read, edit and review them like code. See `../CLAUDE.md` for the rules Claude follows in this folder.
