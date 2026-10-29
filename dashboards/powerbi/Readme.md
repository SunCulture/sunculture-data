# Power BI Dashboards

SunCulture Power BI dashboards, saved as **Power BI projects (.pbip)** so the report and model are text files that can be reviewed, diffed and edited (by people or Claude) like code.

## Table of Contents

- [Folder layout](#folder-layout)
- [Inside a .pbip project](#inside-a-pbip-project)
- [Templates](#templates)
- [Building a dashboard](#building-a-dashboard)
- [What goes in git](#what-goes-in-git)

## Folder layout

```
powerbi/
├── _templates/                 shared theme, brand rules, checklist, assets, tools
├── CLAUDE.md                   rules Claude follows in this folder
└── <country>/<domain>/         ke, ug, global, esg … / payg, sales, ops, cx …
    ├── <dashboard>_v1/         original .pbix (not in git) + review/
    └── <dashboard>_v2/         rebuilt .pbip project + README.md + CHANGELOG.md
```

Example: `ke/payg/credit_history_v1/` holds the original `Credit History -New (Ke).pbix`; `ke/payg/credit_history_v2/` holds `Credit History - Ke.pbip`, rebuilt from it.

## Inside a .pbip project

```
Credit History - Ke.pbip                 open this in Power BI Desktop (File → Open)
Credit History - Ke.Report/              the report: pages and visuals
├── definition.pbir                      points the report at its semantic model
├── definition/
│   ├── report.json                      report settings, theme, registered images
│   ├── pages/<page id>/page.json        one folder per page
│   ├── pages/<page id>/visuals/<id>/visual.json   one file per visual
│   └── bookmarks/
└── StaticResources/                     theme JSON and images (logo, header)
Credit History - Ke.SemanticModel/       the model: data, measures, relationships
├── definition.pbism
└── definition/
    ├── model.tmdl, database.tmdl, relationships.tmdl
    └── tables/<table>.tmdl              columns, measures and Power Query (M) per table
```

- **Report** (PBIR, JSON): layout and formatting. A visual is a small file, so a change shows up as a readable diff.
- **Semantic model** (TMDL, text): tables, DAX measures, calculated columns, relationships and source queries. A measure changed in Desktop appears as a one-line diff.
- `.pbi/` folders hold local state: `cache.abf` is the loaded data, `localSettings.json` is per machine. Neither is committed.

## Templates

`_templates/` (details in [`_templates/README.md`](_templates/README.md)):

| Path | Use |
|---|---|
| `BRAND.md` | Layout (1280 canvas, header, sidebar), colours, type, writing |
| `STANDARDS.md` | Build and review checklist; cite its items in reviews |
| `themes/sunculture.json` | Report theme (brand palette, Arial) |
| `assets/` | Logo, header image, sidebar icons |
| `tools/sidebar_nav.py` | Adds or rebuilds the navigation sidebar from a `nav.json` |

## Building a dashboard

**From an existing .pbix** (the Credit History approach):

1. Put the original in `<country>/<domain>/<dashboard>_v1/` and leave it unchanged.
2. Review it against `STANDARDS.md` (broken fields, unused tables and columns, duplicate measures); save the write-up in `<dashboard>_v1/review/`.
3. Open it in Desktop and use **File → Save as → Power BI project** into `<dashboard>_v2/`.
4. Make the changes in the project: model edits in `*.SemanticModel/definition/`, report edits in `*.Report/definition/`. Keep `lineageTag`s on existing objects, and check every `visual.json` for references before removing a field.
5. Apply `themes/sunculture.json` and the `BRAND.md` page layout; for more than one page, add a `nav.json` and run `sidebar_nav.py`.
6. Close Desktop before editing files outside it (it saves over them), then reopen the `.pbip`, refresh and check every changed page.
7. Write `README.md` (purpose, pages, data model; architecture only) and `CHANGELOG.md` (dated, one line per change).
8. Commit and raise a PR. Publish only after the review.

**New dashboard:** start a blank report in Desktop, save it as a project in `<country>/<domain>/<dashboard>_v2/`, then follow steps 4–8. A `starter/` project with the theme, Date table and page layout is planned in `_templates/`.

## What goes in git

| Commit | Ignored (root `.gitignore`) |
|---|---|
| `*.pbip`, `*.Report/`, `*.SemanticModel/` (definitions, `StaticResources/`, `.platform`, `.pbi/editorSettings.json`) | `*.pbix`: data included, often over GitHub's 100 MB limit, binary diffs |
| `README.md`, `CHANGELOG.md`, `review/`, `nav.json` | `.pbi/cache.abf` (loaded data, contains customer PII), `.pbi/localSettings.json` |

Keep master copies of the `.pbix` originals in the team's shared storage. Credentials are never stored in a project, but tables made with *Enter data* keep their rows in the TMDL, so check them for personal data before committing.
