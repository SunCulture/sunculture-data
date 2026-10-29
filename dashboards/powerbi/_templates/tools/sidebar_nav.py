"""Add or rebuild the SunCulture navigation sidebar on every page of a .pbip report (spec: ../BRAND.md, "Navigation sidebar").

usage:
    python -I sidebar_nav.py "<path>/<Name>.Report" "<path>/nav.json"

nav.json lists the sidebar sections in order; every page in the report must appear exactly once:
    {"sections": [
        {"label": null,          "pages": [{"page": "Overview", "icon": "grid"}]},
        {"label": "ENGINEERING", "pages": [{"page": "Installations", "icon": "wrench"}, ...]}
    ]}
Icons are the file names in ../assets/nav-icons/ (without .svg).

Safe to run again: it deletes and redraws the nav* visuals. On a page that has no sidebar yet it also moves the page's
content into the area right of the sidebar, and points full-width header images at ../assets/header-sidebar.png.
Close the report in Desktop first, or Desktop will save over the changes.
"""
import json, os, re, shutil, sys

TEMPLATES = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ICON_DIR = os.path.join(TEMPLATES, "assets", "nav-icons")
VC = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.7.0/schema.json"

# layout (px on the 1280-wide canvas)
CANVAS_W, NAV_W, CONTENT_X = 1280, 208, 224
CONTENT_W = CANVAS_W - CONTENT_X - 24
OLD_X, OLD_W = 24, 1232  # content area of a page without a sidebar
BTN_X, BTN_W, BTN_H, STEP, FIRST_Y = 12, NAV_W - 24, 30, 34, 96
LOGO_H, LOGO_RATIO = 60, 415 / 402  # assets/sunculture-logo-stacked.png is 415 x 402

# colours (BRAND.md)
PANEL, DIVIDER, TEXT, MUTED, HOVER = "#FFFFFF", "#DCDFDA", "#1F2326", "#636569", "#F5F6F4"
ACTIVE_FILL, ACTIVE_TEXT, ACCENT = "#EAF5E1", "#2F7D0A", "#49A500"

lit = lambda v: {"expr": {"Literal": {"Value": v}}}
col = lambda h: {"solid": {"color": lit(f"'{h}'")}}
off = lambda: [{"properties": {"show": lit("false")}}]
res = lambda item: {"expr": {"ResourcePackageItem": {"PackageName": "RegisteredResources", "PackageType": 1, "ItemName": item}}}


def bare():
    """Container settings for every nav visual. The theme's default white background and 14/16 px padding
    would otherwise leave a 30 px button about 2 px to draw in."""
    return {"background": off(), "border": off(), "dropShadow": off(), "title": off(), "subTitle": off(), "visualHeader": off(),
            "padding": [{"properties": {k: lit("0D") for k in ("top", "bottom", "left", "right")}}]}


def J(p):
    with open(p, encoding="utf-8-sig") as f:
        return json.load(f)


def W(p, d):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8", newline="\n") as f:
        f.write(d if isinstance(d, str) else json.dumps(d, indent=2, ensure_ascii=False))


def shape(name, x, y, w, h, z, fill, radius=0):
    sh = {"tileShape": lit("'rectangleRounded'" if radius else "'rectangle'")}
    if radius:
        sh["rectangleRoundedCurve"] = lit(f"{radius}L")
    return {"$schema": VC, "name": name, "position": {"x": x, "y": y, "z": z, "width": w, "height": h, "tabOrder": z},
            "visual": {"visualType": "shape",
                       "objects": {"shape": [{"properties": sh}],
                                   "fill": [{"properties": {"fillColor": col(fill), "transparency": lit("0D")}, "selector": {"id": "default"}}],
                                   "outline": off()},
                       "visualContainerObjects": bare(), "drillFilterOtherVisuals": True}}


def logo():
    w = round(LOGO_H * LOGO_RATIO)  # box keeps the logo's proportions; 'Fit' stretches to the box
    return {"$schema": VC, "name": "navLogo", "position": {"x": round((NAV_W - w) / 2), "y": 14, "z": 60, "width": w, "height": LOGO_H, "tabOrder": 60},
            "visual": {"visualType": "image",
                       "objects": {"general": [{"properties": {"imageUrl": res("sunculture-logo-stacked.png")}}],
                                   "imageScaling": [{"properties": {"imageScalingType": lit("'Fit'")}}]},
                       "visualContainerObjects": bare()}}


def label(name, text, y):
    run = {"value": text, "textStyle": {"fontFamily": "Arial", "fontSize": "8pt", "color": MUTED, "fontWeight": "bold"}}
    return {"$schema": VC, "name": name, "position": {"x": BTN_X + 10, "y": y, "z": 70, "width": BTN_W - 10, "height": 18, "tabOrder": 70},
            "visual": {"visualType": "textbox", "objects": {"general": [{"properties": {"paragraphs": [{"textRuns": [run]}]}}]},
                       "visualContainerObjects": bare()}}


def button(idx, page_name, icon, target, y, active):
    img = {"image": {"name": lit(f"'{icon}'"), "url": res(f"nav-{icon}-{'on' if active else 'dim'}.svg"), "scaling": lit("'Normal'")}}
    vco = bare()
    vco["visualLink"] = [{"properties": {"show": lit("true"), "type": lit("'PageNavigation'"), "navigationSection": lit(f"'{target}'")}}]
    return {"$schema": VC, "name": f"navButton{idx:02d}", "position": {"x": BTN_X, "y": y, "z": 80 + idx, "width": BTN_W, "height": BTN_H, "tabOrder": 80 + idx},
            "visual": {"visualType": "actionButton",
                       "objects": {
                           "icon": [{"properties": {"show": lit("true")}},
                                    {"properties": {"shapeType": lit("'custom'"), "image": img, "horizontalAlignment": lit("'left'"),
                                                    "leftMargin": lit("12L"), "rightMargin": lit("4L")}, "selector": {"id": "default"}}],
                           "shape": [{"properties": {"tileShape": lit("'rectangleRounded'"), "rectangleRoundedCurve": lit("6L")}, "selector": {"id": "default"}}],
                           "fill": [{"properties": {"show": lit("true")}},
                                    {"properties": {"fillColor": col(ACTIVE_FILL if active else PANEL), "transparency": lit("0D")}, "selector": {"id": "default"}},
                                    {"properties": {"fillColor": col(ACTIVE_FILL if active else HOVER)}, "selector": {"id": "hover"}},
                                    {"properties": {"fillColor": col(ACTIVE_FILL)}, "selector": {"id": "press"}}],
                           "text": [{"properties": {"show": lit("true")}},
                                    {"properties": {"text": lit("'" + page_name.replace("'", "''") + "'"), "fontColor": col(ACTIVE_TEXT if active else TEXT),
                                                    "fontSize": lit("10D"), "fontFamily": lit("'Arial'"), "bold": lit("true" if active else "false"),
                                                    "horizontalAlignment": lit("'left'"), "verticalAlignment": lit("'middle'"),
                                                    "leftMargin": lit("38L"), "rightMargin": lit("4L")}, "selector": {"id": "default"}}],
                           "outline": off()},
                       "visualContainerObjects": vco, "drillFilterOtherVisuals": True}}


def move_content_right(vdir):
    """First run on a page: squeeze everything into the area right of the sidebar."""
    k = CONTENT_W / OLD_W
    for n in os.listdir(vdir):
        f = os.path.join(vdir, n, "visual.json")
        v = J(f)
        p = v["position"]
        if p.get("x", 0) <= 0 and p["width"] >= CANVAS_W - 1:  # full-width header
            p.update(x=NAV_W, width=CANVAS_W - NAV_W)
            gen = v.get("visual", {}).get("objects", {}).get("general", [{}])[0].get("properties", {})
            if v.get("visual", {}).get("visualType") == "image" and "imageUrl" in gen:
                gen["imageUrl"] = res("header-sidebar.png")
        else:
            p["x"] = round(CONTENT_X + max(0, p["x"] - OLD_X) * k, 1)
            p["width"] = round(min(p["width"] * k, CANVAS_W - 24 - p["x"]), 1)
            for cw in v.get("visual", {}).get("objects", {}).get("columnWidth", []):
                val = cw["properties"]["value"]["expr"]["Literal"]
                val["Value"] = f"{round(float(val['Value'].rstrip('D')) * k)}D"
        W(f, v)


def main(report_dir, config_path):
    pages_dir = os.path.join(report_dir, "definition", "pages")
    res_dir = os.path.join(report_dir, "StaticResources", "RegisteredResources")
    sections = J(config_path)["sections"]
    order = J(os.path.join(pages_dir, "pages.json"))["pageOrder"]
    pid = {J(os.path.join(pages_dir, p, "page.json"))["displayName"]: p for p in order}
    listed = [e["page"] for s in sections for e in s["pages"]]
    missing, unknown = set(pid) - set(listed), set(listed) - set(pid)
    dupes = {n for n in listed if listed.count(n) > 1}
    if missing or unknown or dupes:
        sys.exit(f"nav.json does not match the report. Not in nav.json: {sorted(missing)}; not in report: {sorted(unknown)}; listed twice: {sorted(dupes)}")
    icons = {e["icon"] for s in sections for e in s["pages"]}
    bad = sorted(i for i in icons if not os.path.isfile(os.path.join(ICON_DIR, f"{i}.svg")))
    if bad:
        sys.exit(f"unknown icons {bad}; available: {sorted(f[:-4] for f in os.listdir(ICON_DIR))}")

    # resources: icons in two tones, logo, header bar
    os.makedirs(res_dir, exist_ok=True)
    for fn in os.listdir(res_dir):
        if re.match(r"nav-.+-(on|dim)\.svg$", fn):
            os.remove(os.path.join(res_dir, fn))
    new_items = []
    for icon in sorted(icons):
        svg = open(os.path.join(ICON_DIR, f"{icon}.svg"), encoding="utf-8").read()
        for tone, colour in (("dim", MUTED), ("on", ACTIVE_TEXT)):
            W(os.path.join(res_dir, f"nav-{icon}-{tone}.svg"), svg.replace("currentColor", colour))
            new_items.append(f"nav-{icon}-{tone}.svg")
    for asset in ("sunculture-logo-stacked.png", "header-sidebar.png"):
        shutil.copyfile(os.path.join(TEMPLATES, "assets", asset), os.path.join(res_dir, asset))
        new_items.append(asset)
    rep_path = os.path.join(report_dir, "definition", "report.json")
    rep = J(rep_path)
    pkgs = rep.setdefault("resourcePackages", [])
    pkg = next((p for p in pkgs if p["name"] == "RegisteredResources"), None)
    if pkg is None:
        pkg = {"name": "RegisteredResources", "type": "RegisteredResources", "items": []}
        pkgs.append(pkg)
    pkg["items"] = [i for i in pkg.get("items", []) if not re.match(r"nav-.+-(on|dim)\.svg$", i["name"]) and i["name"] not in new_items]
    pkg["items"] += [{"name": n, "path": n, "type": "Image"} for n in new_items]
    W(rep_path, rep)

    for page_name, page_id in pid.items():
        page_path = os.path.join(pages_dir, page_id, "page.json")
        page = J(page_path)
        vdir = os.path.join(pages_dir, page_id, "visuals")
        os.makedirs(vdir, exist_ok=True)
        if not os.path.isdir(os.path.join(vdir, "navBackground")):
            move_content_right(vdir)
            print(f"{page_name}: content moved right of the sidebar")
        for n in os.listdir(vdir):
            if n.startswith("nav"):
                shutil.rmtree(os.path.join(vdir, n))
        height = page.get("height", 720)
        nav = [shape("navBackground", 0, 0, NAV_W, height, 50, PANEL), shape("navDivider", NAV_W - 1, 0, 1, height, 55, DIVIDER), logo()]
        y, idx = FIRST_Y, 0
        for s in sections:
            if s.get("label"):
                y += 12
                nav.append(label(f"navLabel{len(nav):02d}", s["label"], y))
                y += 22
            for e in s["pages"]:
                active = e["page"] == page_name
                nav.append(button(idx, e["page"], e["icon"], pid[e["page"]], y, active))
                if active:
                    nav.append(shape("navActiveBar", BTN_X, y + 6, 3, BTN_H - 12, 200, ACCENT, radius=2))
                y += STEP
                idx += 1
        for v in nav:
            W(os.path.join(vdir, v["name"], "visual.json"), v)
        if y > height:
            print(f"warning: {page_name}: sidebar ends at y={y}, below the page height {height}")
    print(f"sidebar drawn on {len(pid)} pages ({idx} buttons, ends at y={y})")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
