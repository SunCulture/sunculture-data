"""
Default fonts for the Word (.docx) versions of the analysis reports (analysis/<nn_topic>/analysis*/generator.py).

    Report title       Calibri 16 pt, bold   (Heading 1)
    Section titles     Calibri 13 pt, bold   (Heading 2, '## Section …')
    Sub-section titles Calibri 11 pt, bold   (Heading 3)
    Content            Calibri 10 pt         (Normal: highlights, takeaways, text, methodology)
    Table cells        Calibri 8.5 pt
    Table notes        Calibri 7.5 pt, italic

A generator calls apply_defaults(doc) right after Document() and uses TABLE_PT / NOTE_PT for its tables:

    import docx_styles
    doc = Document()
    docx_styles.apply_defaults(doc)
"""
from docx.oxml.ns import qn
from docx.shared import Pt

FONT = "Calibri"
TITLE_PT = 16
SECTION_PT = 13
SUBSECTION_PT = 11
CONTENT_PT = 10
TABLE_PT = 8.5
NOTE_PT = 7.5


def _set_font(style, size):
    style.font.name = FONT
    style.font.size = Pt(size)
    # Word's built-in styles take their font from the document theme (Calibri Light for headings),
    # which overrides font.name; pin every script to FONT instead.
    rfonts = style.element.get_or_add_rPr().get_or_add_rFonts()
    for attr in ("w:asciiTheme", "w:hAnsiTheme", "w:eastAsiaTheme", "w:cstheme"):
        rfonts.attrib.pop(qn(attr), None)
    for attr in ("w:ascii", "w:hAnsi", "w:eastAsia", "w:cs"):
        rfonts.set(qn(attr), FONT)


def apply_defaults(doc):
    """Set the content and heading fonts on a python-docx Document."""
    _set_font(doc.styles["Normal"], CONTENT_PT)
    for name, size in (("Heading 1", TITLE_PT), ("Heading 2", SECTION_PT), ("Heading 3", SUBSECTION_PT)):
        _set_font(doc.styles[name], size)
        doc.styles[name].font.bold = True
    return doc
