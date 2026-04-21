import pytest

from delphix_core.utils.cron import is_valid_cron


# ---- valid expressions ---------------------------------------------------

@pytest.mark.unit
@pytest.mark.parametrize("expression", [
    # wildcards
    "* * * * *",
    # fixed values
    "0 0 1 1 0",
    "59 23 31 12 7",
    # steps
    "*/15 * * * *",
    "*/1 */2 */3 */4 */5",
    # ranges
    "0-30 * * * *",
    "0 9-17 * * *",
    "* * 1-15 * *",
    "* * * 6-12 *",
    "* * * * 1-5",
    # ranges with steps
    "0-30/5 * * * *",
    "* 8-18/2 * * *",
    "* * 1-15/3 * *",
    # lists
    "0,15,30,45 * * * *",
    "0 0,12 * * *",
    "* * 1,15 * *",
    "* * * 1,4,7,10 *",
    "* * * * 0,6",
    # mixed lists (value, range, step)
    "0,15,30-45/5 * * * *",
    "0 8,12,18-22/2 * * *",
    # boundary values
    "0 0 1 1 0",
    "59 23 31 12 7",
    # day of week 7 (Sunday alternative)
    "0 0 * * 7",
    # real-world schedules
    "0 2 * * 0",
    "30 8 * * 1-5",
    "0 0 1 * *",
    "0 */6 * * *",
    "15 14 1 * *",
])
def test_valid_cron_expressions(expression):
    assert is_valid_cron(expression) is True


# ---- invalid expressions -------------------------------------------------

@pytest.mark.unit
@pytest.mark.parametrize("expression,reason", [
    # wrong number of fields
    ("* * *", "only 3 fields"),
    ("* * * *", "only 4 fields"),
    ("* * * * * *", "6 fields"),
    ("*", "single field"),
    # out of range values
    ("60 * * * *", "minute 60 out of range"),
    ("* 24 * * *", "hour 24 out of range"),
    ("* * 0 * *", "day of month 0 out of range"),
    ("* * 32 * *", "day of month 32 out of range"),
    ("* * * 0 *", "month 0 out of range"),
    ("* * * 13 *", "month 13 out of range"),
    ("* * * * 8", "day of week 8 out of range"),
    # inverted ranges
    ("30-10 * * * *", "inverted minute range"),
    ("* 17-9 * * *", "inverted hour range"),
    ("* * 20-5 * *", "inverted day range"),
    ("* * * 12-1 *", "inverted month range"),
    ("* * * * 5-1", "inverted dow range"),
    # step of zero
    ("*/0 * * * *", "step of 0"),
    ("0-30/0 * * * *", "range step of 0"),
    # non-numeric
    ("abc * * * *", "letters in minute"),
    ("* def * * *", "letters in hour"),
    ("* * * JAN *", "named month"),
    ("* * * * MON", "named day of week"),
    ("@daily", "special string"),
    ("@weekly", "special string"),
    # empty / whitespace
    ("", "empty string"),
    ("   ", "whitespace only"),
    # malformed parts
    ("1- * * * *", "dangling dash"),
    ("-1 * * * *", "leading dash"),
    ("1/2/3 * * * *", "double slash"),
    ("* * * * ,1", "leading comma"),
    ("* * * * 1,", "trailing comma"),
])
def test_invalid_cron_expressions(expression, reason):
    assert is_valid_cron(expression) is False, reason


# ---- edge cases ----------------------------------------------------------

@pytest.mark.unit
def test_cron_with_extra_whitespace_is_valid():
    """Leading/trailing spaces are trimmed."""
    assert is_valid_cron("  0 2 * * 0  ") is True


@pytest.mark.unit
def test_cron_with_tabs_between_fields_is_invalid():
    """Only single spaces separate fields."""
    assert is_valid_cron("0\t2\t*\t*\t0") is False


@pytest.mark.unit
def test_cron_with_double_spaces_between_fields_is_invalid():
    """Double spaces produce empty fields after split."""
    assert is_valid_cron("0  2  *  *  0") is False
