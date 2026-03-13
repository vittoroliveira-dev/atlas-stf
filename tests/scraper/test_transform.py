"""Tests for _transform: strip_html and clean_record."""

from atlas_stf.scraper._transform import clean_record, strip_html


class TestStripHtml:
    def test_removes_tags(self) -> None:
        assert strip_html("<p>Hello <b>world</b></p>") == "Hello world"

    def test_decodes_entities(self) -> None:
        assert strip_html("foo&amp;bar&nbsp;baz") == "foo&bar baz"

    def test_collapses_whitespace(self) -> None:
        assert strip_html("  a   b\n\tc  ") == "a b c"

    def test_empty_string(self) -> None:
        assert strip_html("") == ""

    def test_no_html(self) -> None:
        assert strip_html("plain text") == "plain text"

    def test_nested_tags(self) -> None:
        assert strip_html("<div><p>a</p><p>b</p></div>") == "a b"

    def test_br_tags(self) -> None:
        assert strip_html("line1<br/>line2<br>line3") == "line1 line2 line3"


class TestCleanRecord:
    def test_cleans_text_fields(self) -> None:
        record = {
            "_id": "abc",
            "decisao_texto": "<p>Hello</p>",
            "processo_numero": "12345",
        }
        result = clean_record(record, ("decisao_texto",))
        assert result["decisao_texto"] == "Hello"
        assert result["processo_numero"] == "12345"

    def test_skips_missing_fields(self) -> None:
        record = {"_id": "abc"}
        result = clean_record(record, ("decisao_texto",))
        assert "decisao_texto" not in result

    def test_skips_non_string(self) -> None:
        record = {"decisao_texto": None}
        result = clean_record(record, ("decisao_texto",))
        assert result["decisao_texto"] is None
