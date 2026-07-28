"""Tests for BQL argument handling."""

import pytest

from bifract_mcp import bql


def test_time_window_omits_blanks():
    assert bql.time_window("", "") == {}
    assert bql.time_window("  ", "2026-01-01T00:00:00Z") == {"end": "2026-01-01T00:00:00Z"}
    assert bql.time_window(" 2026-01-01T00:00:00Z ", "") == {"start": "2026-01-01T00:00:00Z"}


def test_literal_quotes_ordinary_values():
    assert bql.literal("powershell.exe", field="image") == '"powershell.exe"'
    assert bql.literal("{a-b-c}", field="guid") == '"{a-b-c}"'
    assert bql.literal("net group /domain", field="cmd") == '"net group /domain"'


@pytest.mark.parametrize(
    "value",
    ['bad"quote', "bad'quote", "back\\slash", "pipe|split", "null\x00byte", "new\nline"],
)
def test_literal_rejects_values_that_would_break_out(value):
    with pytest.raises(bql.BQLValueError):
        bql.literal(value, field="image")


def test_operators_build_expected_expressions():
    assert bql.equals("bifract_category", "process_creation") == 'bifract_category="process_creation"'
    assert bql.contains("image", "powershell") == 'image=~"powershell"'
