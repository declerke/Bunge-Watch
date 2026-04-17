"""
Tests for the change detector — hash detection, stage ordering logic.
Pure unit tests: no DB required.
"""
import pytest
from pipeline.change_detector import STAGE_ORDER
from pipeline.pdf_parser import _sha256_text


class TestStageOrdering:
    def test_all_stages_defined(self):
        expected = [
            "Published", "1st Reading", "Committee Stage",
            "2nd Reading", "3rd Reading", "Assented",
        ]
        assert STAGE_ORDER == expected

    def test_assented_is_last(self):
        assert STAGE_ORDER[-1] == "Assented"

    def test_published_is_first(self):
        assert STAGE_ORDER[0] == "Published"

    def test_correct_count(self):
        assert len(STAGE_ORDER) == 6


class TestHashDetection:
    def test_same_text_same_hash(self, sample_bill_text):
        h1 = _sha256_text(sample_bill_text)
        h2 = _sha256_text(sample_bill_text)
        assert h1 == h2

    def test_different_text_different_hash(self, sample_bill_text):
        h1 = _sha256_text(sample_bill_text)
        h2 = _sha256_text(sample_bill_text + " amended clause")
        assert h1 != h2

    def test_hash_is_64_chars(self, sample_bill_text):
        h = _sha256_text(sample_bill_text)
        assert len(h) == 64

    def test_hash_is_hex(self, sample_bill_text):
        h = _sha256_text(sample_bill_text)
        int(h, 16)  # raises ValueError if not valid hex


class TestSummaryParsing:
    def test_parses_valid_claude_response(self):
        from pipeline.claude_summarizer import _parse_response
        response = (
            "SHORT: This bill amends the Data Protection Act to include AI systems.\n"
            "DETAILED: The bill introduces a new definition for artificial intelligence "
            "and requires data controllers deploying AI to notify the Data Commissioner "
            "within 30 days, with fines up to five million shillings for non-compliance."
        )
        short, detailed = _parse_response(response)
        assert "Data Protection" in short
        assert "five million" in detailed

    def test_handles_missing_sections(self):
        from pipeline.claude_summarizer import _parse_response
        short, detailed = _parse_response("Some random text without markers")
        assert short == ""
        assert detailed == ""
