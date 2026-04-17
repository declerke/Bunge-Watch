"""
All CSS/HTML selectors for scraped sites in one place.
When a site changes, only this file needs editing.
"""

# ─── Kenya Law (kenyalaw.org) ─────────────────────────────────────────────────
# Static HTML table at ?id=12043&yr=YYYY
KENYALAW = {
    "bills_table": "table",                  # Main data table
    "bill_rows": "table tr",                 # All rows including header
    "col_title": 1,                          # Column index: bill title (0-based)
    "col_sponsor": 2,
    "col_bill_number": 3,
    "col_date": 4,
    "col_gazette": 6,
    "col_first_read": 7,
    "col_second_read": 8,
    "col_third_read": 9,
    "col_remarks": 10,
    "col_assent": 11,
    "pdf_link": "a[href]",                   # Anchor inside title cell
    "pdf_href_pattern": "/kl/fileadmin/pdfdownloads/bills/",
}

# ─── Parliament of Kenya (parliament.go.ke) ───────────────────────────────────
# JS-rendered Drupal Views — scraper parses rendered DOM
# DOM structure (verified 2026-04-16):
#   table > tr > td.views-field-nothing > .post-block > .post-content
#     .post-title > a          ← title text AND direct PDF href
#     .post-digest a           ← bill digest page link (optional)
#     .post-billtracker a      ← bill tracker page link (optional)
PARLIAMENT = {
    "bills_path": "/the-national-assembly/house-business/bills",
    "tracker_path": "/the-national-assembly/house-business/bill-tracker",
    "senate_bills_path": "/the-senate/senate-bills",
    "views_ajax_path": "/views/ajax",
    "bill_items": "td.views-field-nothing",  # Each bill row cell
    "bill_title_link": ".post-title a",      # Title text + PDF href in same element
    "bill_digest_link": ".post-digest a",
    "bill_tracker_link": ".post-billtracker a",
}
