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


class TestExtractedSummary:
    def test_extracts_summary_from_bill_text(self, sample_bill_text):
        from pipeline.claude_summarizer import extractive_summary
        short, detailed = extractive_summary(sample_bill_text)
        assert isinstance(short, str)
        assert isinstance(detailed, str)
        assert len(short) > 0
        assert len(detailed) >= len(short)

    def test_short_summary_under_word_limit(self, sample_bill_text):
        from pipeline.claude_summarizer import extractive_summary
        short, _ = extractive_summary(sample_bill_text, max_short_words=60)
        assert len(short.split()) <= 70  # small buffer for edge cases

    def test_empty_text_returns_fallback(self):
        from pipeline.claude_summarizer import extractive_summary
        short, detailed = extractive_summary(" ")
        assert isinstance(short, str)
        assert isinstance(detailed, str)
