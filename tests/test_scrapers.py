"""
Tests for the kenyalaw_scraper HTML parser.
Uses bundled HTML fixtures — no live network calls.
"""
import pytest
from bs4 import BeautifulSoup
from scrapers.kenyalaw_scraper import _parse_bill_row, _parse_date, _make_bill_id
from scrapers.selectors import KENYALAW


def _get_rows(html: str):
    soup = BeautifulSoup(html, "lxml")
    return soup.select(KENYALAW["bill_rows"])


class TestParseBillRow:
    def test_parses_valid_row(self, kenyalaw_html):
        rows = _get_rows(kenyalaw_html)
        data_rows = [r for r in rows if r.find("td")]
        assert len(data_rows) >= 2

        cells = data_rows[0].find_all("td")
        result = _parse_bill_row(cells, 2025)

        assert result is not None
        assert "Data Protection" in result["title"]
        assert result["sponsor"] == "Jane Mwangi"
        assert result["source"] == "kenyalaw"
        assert result["chamber"] == "NA"
        assert result["is_passed"] is False

    def test_first_reading_detected(self, kenyalaw_html):
        rows = _get_rows(kenyalaw_html)
        data_rows = [r for r in rows if r.find("td")]
        cells = data_rows[0].find_all("td")
        result = _parse_bill_row(cells, 2025)
        assert result["current_stage"] == "1st Reading"

    def test_second_reading_detected(self, kenyalaw_html):
        rows = _get_rows(kenyalaw_html)
        data_rows = [r for r in rows if r.find("td")]
        cells = data_rows[1].find_all("td")
        result = _parse_bill_row(cells, 2025)
        assert result["current_stage"] == "2nd Reading"

    def test_pdf_url_extracted(self, kenyalaw_html):
        rows = _get_rows(kenyalaw_html)
        data_rows = [r for r in rows if r.find("td")]
        cells = data_rows[0].find_all("td")
        result = _parse_bill_row(cells, 2025)
        assert result["pdf_url"] is not None
        assert ".pdf" in result["pdf_url"].lower()

    def test_header_row_returns_none(self, kenyalaw_html):
        rows = _get_rows(kenyalaw_html)
        header = next((r for r in rows if r.find("th")), None)
        if header:
            cells = header.find_all("td")
            assert _parse_bill_row(cells, 2025) is None

    def test_empty_cells_returns_none(self):
        assert _parse_bill_row([], 2025) is None
        assert _parse_bill_row(["x"] * 3, 2025) is None


class TestParseDate:
    def test_dd_mm_yy(self):
        result = _parse_date("15/03/25")
        assert result is not None
        assert result.year == 2025
        assert result.month == 3
        assert result.day == 15

    def test_dd_mm_yyyy(self):
        result = _parse_date("02/04/2025")
        assert result is not None
        assert result.year == 2025

    def test_invalid_returns_none(self):
        assert _parse_date("") is None
        assert _parse_date("not-a-date") is None
        assert _parse_date("—") is None


class TestMakeBillId:
    def test_stable_id(self):
        id1 = _make_bill_id("NA Bill No. 7 of 2025", 2025)
        id2 = _make_bill_id("NA Bill No. 7 of 2025", 2025)
        assert id1 == id2

    def test_different_years_differ(self):
        id1 = _make_bill_id("NA Bill No. 7 of 2025", 2025)
        id2 = _make_bill_id("NA Bill No. 7 of 2025", 2024)
        assert id1 != id2

    def test_uppercase(self):
        bill_id = _make_bill_id("bill7", 2025)
        assert bill_id == bill_id.upper()
