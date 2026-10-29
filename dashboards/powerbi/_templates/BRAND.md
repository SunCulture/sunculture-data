# SunCulture Power BI Brand Guidelines

How the SunCulture Brand Guidelines (v1.0, April 2019; source in `/_templates/`) apply to Power BI dashboards. The visual reference is the template mockup on claude.ai (overview, analysis, detail and style-guide pages).

## Page layout

| Element | Spec |
|---|---|
| Canvas | **1280 × 720** (Power BI default; text sizes are tuned to it). Allow vertical scroll for long pages rather than changing width. |
| Sidebar | **Dashboards with more than one page:** a 208 px navigation sidebar on the left of every page. See *Navigation sidebar* below. |
| Header | 72 px, white. With the sidebar it starts at x = 208: page title (+ subtitle `Country · Domain · Page`) on the left, "Data refreshed" time in EAT on the right; the logo is in the sidebar. **Single-page dashboards** (no sidebar) keep the full-width header with the logo, divider and title. |
| Accent line | 4 px gradient under the header: `#F3D500 → #62C628 → #00CCC8` (the logo ring). The only gradient on the page. |
| Grid | 12 columns, 16 px gutters, 12 px between rows. Content area: x = 224 to 1256 (1032 px) with the sidebar; x = 24 to 1256 (1232 px) without. |
| Order | Filter row → KPI row (4–5 cards) → main chart (8 cols) + side chart (4 cols) → table. |
| Cards | White, 6 px corners, no border, no shadow, padding 14/16 px. |
| Page background | `#F5F6F4` |

Build the header from an image visual (`assets/header-sidebar.png` with the sidebar, 1072 px wide; it carries the accent line) plus a text box for the title and a card for the refresh time, so every page is identical.

## Navigation sidebar

Used on every page of a multi-page dashboard, in place of Power BI's page tabs. Readers should never need the Pages pane.

| Element | Spec |
|---|---|
| Panel | 208 px wide, full page height, white `#FFFFFF`, 1 px divider `#DCDFDA` on its right edge |
| Logo | `assets/sunculture-logo-stacked.png`, 60 px tall, centred at the top (y = 14). Its box keeps the logo's proportions (415 : 402) because Power BI's *Fit* stretches an image to its box |
| Section labels | Upper case, Arial 8 pt bold, `#636569` (e.g. ENGINEERING, SUPPLY CHAIN). The overview page sits above the first label |
| Page buttons | One per page, in page order: 184 × 30 px, 6 px corners, 34 px apart. Line icon (`assets/nav-icons/`) on the left in `#636569`; page name Arial 10 pt `#1F2326` |
| Hover | Button fill `#F5F6F4` |
| Current page | Fill `#EAF5E1`, name bold and icon in `#2F7D0A`, plus a 3 px brand-green `#49A500` bar on the left edge |

Icons are 24 × 24 line drawings (2 px stroke, round ends) in one style; add new ones to `assets/nav-icons/` in the same style rather than mixing icon sets.

Build it with `tools/sidebar_nav.py` from a `nav.json` next to the `.pbip` (sections, pages and icons); see `README.md`. If you build it by hand, give every sidebar visual **no background, no border and 0 padding**: the theme's default 14/16 px padding leaves a 30 px button about 2 px to draw in, and it shows as a blank box.

## Logo

- Use `assets/sunculture-logo-stacked.png` as supplied. Do not stretch, recolour or rotate it, and do not place it on a gradient or photo.
- Minimum **57 px** tall on screen (manual: digital minimum). It sits at 60 px, in the sidebar or in the 72 px header.
- Clear space: at least half the ring's height on all sides.
- To do: ask the brand owner for the **horizontal** logo as SVG/PNG. The manual allows it "where the centered size doesn't work", which suits a dashboard header.

## Colour

The manual's colours are bright gradient tones. Most of them fall below 3:1 contrast on white, so they are used in two ways.

**Brand colours** (logo, accent line, cover pages only):
Yellow `#F3D500` · Orange `#FF8202` · Green `#62C628` · Red `#E23D26` · Blue `#00CCC8` · Purple `#9D2AAD` · Warm grey `#A0918A` · Grey `#636569`

**Chart colours.** These are the same hues, darkened. Use them in this fixed order:

| Slot | Colour | Hex |
|---|---|---|
| 1 | Green | `#49A500` |
| 2 | Blue | `#018EB1` |
| 3 | Orange | `#E67502` |
| 4 | Pink | `#C0015C` |
| 5 | Yellow | `#C3AB05` (2.3:1 on white; **data labels must be on**) |
| 6 | Purple | `#9119A0` |
| 7 | Teal | `#05A19D` |
| Other | Warm grey | `#A0918A` |

This order was tested for colour-blind separation (worst adjacent pair ΔE 18.5 under deuteranopia/protanopia simulation; 8 is the target) and normal-vision separation (22.1, floor 15).

- A chart with one series uses slot 1. Never colour bars by their value.
- More than 7 series: fold the rest into "Other", or split into small multiples.
- A colour follows the entity, not its rank, so filtering must not repaint the remaining series.

**Status** (reserved; always shown with an icon and a label, never colour alone):
On track `#2F7D0A` · Watch `#F3D500` icon with dark text · Behind `#C42B17`

**Ordered steps** (funnels, bands, heatmaps): `#7CBF4E → #49A500 → #2F7D0A → #1E5506`

**Neutrals:** Text `#1F2326` · Secondary text `#636569` (brand grey) · Gridlines `#ECEEEA` · Axis `#C9CDC6` · Borders `#DCDFDA`

## Typography

The brand font, **Circular Std**, cannot be used: the Power BI Service renders only built-in fonts. Use **Arial**, which the manual names as the system fallback.

| Use | Size |
|---|---|
| Page title | 20 bold |
| KPI value | 28 bold |
| Visual title | 14 bold |
| Labels, table text | 12 |
| Axis, legend | 11 |

## Writing (from "What we say")

The manual asks for succinct, grounded, UK-English copy. On a dashboard that means:

- Titles say what is shown, in sentence case, with the period: "Units sold by region · MTD". No "Chart 7:" prefixes.
- Short words: "use", not "utilise"; "Refunds", not "Refund Transactions Summary".
- UK spelling: "colour", "programme", "analyse".
- No exclamation marks.

## Charts

- No dual-axis charts; use two charts.
- Lines 2 px; bars with a small gap; gridlines light and horizontal only.
- Pie/donut charts only for 2–3 parts of a whole. Otherwise use a bar chart.
- Label values directly when there are few; always keep a legend for two or more series.

## Data on the page

- Show account references, not customer names, phone numbers or ID numbers.
- No images or icons loaded from external websites.
