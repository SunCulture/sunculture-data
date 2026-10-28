# Analysis 3 - New vs. Refurb Units

## Table of Contents

- [What this analysis covers](#what-this-analysis-covers)
- [Folder layout](#folder-layout)
- [How to run the analysis](#how-to-run-the-analysis)
  - [Setup (once)](#setup-once)
  - [Run on the saved (static) data](#run-on-the-saved-static-data)
  - [Run on fresh data](#run-on-fresh-data)
  - [Run on a specific earlier pull](#run-on-a-specific-earlier-pull)
  - [Options](#options)
- [Updating the product mapping](#updating-the-product-mapping)

## What this analysis covers

Refurbished vs. new units, on gross unit sales (sold and refunded accounts, `category = 'Unit'`), one report per country. Units are the sum of `productQty`. A unit is Refurb when its product has `isRefurb = 1` in `amt.products` (joined on `productId`); every other unit is New.

There are two reports:

| Report | Script | Output file |
|---|---|---|
| Product view: refurb vs. new overall and by product | `analysis/builder.py` | `refurbs_vs_new_review_<country>_<stamp>.md` |
| Battery vs. Direct: New Battery, Refurb Battery, New Direct, Refurb Direct | `analysis_v2/builder.py` | `refurbs_vs_new_battery_vs_direct_<country>_<stamp>.md` (+ `.docx`) |

## Folder layout

```
03_refurbs_vs_new/
├── queries/                  SQL run against ClickHouse
│   ├── sales_and_refunds_accounts.sql   gross-sales unit accounts (marts.mart_accounts)
│   └── products_refurb.sql              amt.products id / product / isRefurb
├── data/
│   └── product_groups_v2.csv            hand-maintained product → Battery / Direct mapping (tracked in git)
├── analysis/                 product view: builder.py (tables) + generator.py (report)
├── analysis_v2/              Battery vs. Direct: builder.py + generator.py
└── output/
    ├── *.md / *.docx         reports, one new file per run
    └── data/                 data pulls (git-ignored, contain customer PII)
```

## How to run the analysis

Run every command from the repo root with the project virtual environment. The examples use the Battery vs. Direct report (`analysis_v2`). The product view (`analysis`) takes the same options, except `--docx`.

### Setup (once)

1. Install the packages: `.\sd-venv\Scripts\python.exe -m pip install -r requirements.txt`.
2. Fresh pulls need the ClickHouse credentials in the repo-root `.env`: `SC_CH_DB_HOST`, `SC_CH_DB_PORT`, `SC_CH_DB_USER` and `SC_CH_DB_PASSWORD`. Runs on saved data don't need them.

### Run on the saved (static) data

Use this when you're changing the report (wording, tables, layout) and want the figures to stay the same between runs. With no options, the script uses the latest pull in `output/data/`:

```powershell
.\sd-venv\Scripts\python.exe analysis/03_refurbs_vs_new/analysis_v2/builder.py
.\sd-venv\Scripts\python.exe analysis/03_refurbs_vs_new/analysis_v2/builder.py --docx              # also write the Word version
.\sd-venv\Scripts\python.exe analysis/03_refurbs_vs_new/analysis_v2/builder.py --country uganda
```

If `output/data/` is empty (for example on a new machine), the script pulls fresh data first, so the first run always works.

### Run on fresh data

Use this for a new reporting period (e.g. the monthly update), or when the source data has changed. `--fresh` (or `--refresh`) runs both queries, saves the results to `output/data/` with the time of the pull, then builds the report on them:

```powershell
.\sd-venv\Scripts\python.exe analysis/03_refurbs_vs_new/analysis_v2/builder.py --fresh --docx
```

Each pull is saved as a pair of files with the same stamp (`YYYYMMDDHHMM`), and later runs without `--fresh` reuse it:

```
output/data/sales_and_refunds_accounts_202610271841.csv
output/data/products_refurb_202610271841.csv
```

### Run on a specific earlier pull

To re-run a report on the exact data behind an earlier version, list the saved pulls and pass one with `--snapshot`. It accepts either the stamp or one of the pull's file names:

```powershell
.\sd-venv\Scripts\python.exe analysis/03_refurbs_vs_new/analysis_v2/builder.py --list-snapshots
.\sd-venv\Scripts\python.exe analysis/03_refurbs_vs_new/analysis_v2/builder.py --snapshot 202610271841
.\sd-venv\Scripts\python.exe analysis/03_refurbs_vs_new/analysis_v2/builder.py --snapshot sales_and_refunds_accounts_202610271841.csv
```

A pull is usable only when both of its files are in `output/data/`.

### Options

| Option | What it does |
|---|---|
| *(none)* | Latest saved pull, Kenya, Markdown report |
| `--fresh` / `--refresh` | Pull new data from ClickHouse first and save it to `output/data/` |
| `--snapshot <stamp or file name>` | Use a specific saved pull |
| `--list-snapshots` | List the saved pulls and exit |
| `--country kenya\|uganda` | Country to report on (default: `kenya`) |
| `--docx` | Also write a Word version next to the Markdown (`analysis_v2` only) |

Every run writes a new report named with the time of the run, so earlier drafts are never overwritten. Delete the outdated ones from `output/` by hand.

## Updating the product mapping

`data/product_groups_v2.csv` (`product_id, product, product_group`) decides whether a product is Battery or Direct. Products missing from it, or with a blank `product_group`, count as Other and fall outside the Battery vs. Direct comparison. When a new product is launched, add a row for it and re-run the report; no fresh pull is needed. The file covers Kenya products only, so most Uganda products would count as Other until they're added.
