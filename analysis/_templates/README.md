# Analysis report templates

Shared pieces for the reports under `analysis/<nn_topic>/`.

## `report_sections.py`

Reports open with the title and go straight into **Key Highlights**. These standard sections close every report, in this order:

| Function                                                | Renders as                                                                                                                             |
| ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `methodology(items)`                                    | `## Methodology`, then one **Label:** detail line per item (scope, units, definitions, data source)                                    |
| `report_details(date, period, prepared_by="Data Team")` | **Prepared by:** Data Team · **Date:** Oct. 27, 2026 · **Period Covered:** February 23 – March 8, 2026 as the last lines of the report |

Both return blocks in the format the generators already use (`title`, `h2`, `table`, …), so add them to the end of the block list. See the docstring in `report_sections.py` for an example, and `analysis/03_refurbs_vs_new/analysis*/generator.py` for the reports that use it.

## `docx_styles.py`

Default fonts for the Word (`.docx`) version of a report. Call `docx_styles.apply_defaults(doc)` right after `Document()`:

| Text                           | Font                               |
| ------------------------------ | ---------------------------------- |
| Report title (Heading 1)       | Calibri 16 pt, bold                |
| Section titles (Heading 2)     | Calibri 13 pt, bold                |
| Sub-section titles (Heading 3) | Calibri 11 pt, bold                |
| Content (Normal)               | Calibri 10 pt                      |
| Table cells                    | Calibri 8.5 pt (`TABLE_PT`)        |
| Table notes                    | Calibri 7.5 pt, italic (`NOTE_PT`) |

Change a size here and every report picks it up on its next run. Used by `analysis/03_refurbs_vs_new/analysis_v2/generator.py`.
