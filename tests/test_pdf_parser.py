"""
Tests for the PDF parser — pdfplumber and PyMuPDF extractors.
Uses the sample PDF fixture from conftest.py.
"""
import pytest
from pipeline.pdf_parser import extract_with_pdfplumber, extract_with_pymupdf


class TestPdfPlumber:
    def test_extracts_text_from_valid_pdf(self, sample_bill_pdf):
        if sample_bill_pdf is None:
            pytest.skip("reportlab not installed — skipping PDF fixture test")
        result = extract_with_pdfplumber(sample_bill_pdf)
        assert result is not None
        assert len(result) > 0
        assert "Data Protection" in result or "Bill" in result

    def test_returns_none_for_nonexistent_file(self):
        result = extract_with_pdfplumber("/nonexistent/path/bill.pdf")
        assert result is None


class TestPyMuPDF:
    def test_extracts_text_from_valid_pdf(self, sample_bill_pdf):
        if sample_bill_pdf is None:
            pytest.skip("reportlab not installed — skipping PDF fixture test")
        result = extract_with_pymupdf(sample_bill_pdf)
        assert result is not None
        assert len(result) > 0

    def test_returns_none_for_nonexistent_file(self):
        result = extract_with_pymupdf("/nonexistent/path/bill.pdf")
        assert result is None


class TestKeywordExtractor:
    def test_extracts_keywords_from_text(self, sample_bill_text):
        from pipeline.keyword_extractor import extract_keywords
        results = extract_keywords(sample_bill_text)
        assert isinstance(results, list)
        assert len(results) > 0
        assert all(isinstance(kw, str) for kw, _ in results)
        assert all(0 <= score <= 1 for _, score in results)

    def test_empty_text_returns_empty(self):
        from pipeline.keyword_extractor import extract_keywords
        assert extract_keywords("") == []
        assert extract_keywords("short") == []

    def test_scores_in_descending_order(self, sample_bill_text):
        from pipeline.keyword_extractor import extract_keywords
        results = extract_keywords(sample_bill_text)
        scores = [s for _, s in results]
        assert scores == sorted(scores, reverse=True)
