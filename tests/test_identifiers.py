"""Tests for SQL identifier quoting (mixed case + whitespace)."""

from datasight.identifiers import (
    build_identifier_case_map,
    build_special_identifier_list,
    configure_runner_identifier_quoting,
    quote_mixed_case_identifiers,
    quote_special_identifiers,
)


class TestBuildIdentifierCaseMap:
    def test_mixed_case_table_picked(self):
        info = [{"name": "Battery", "columns": [{"name": "id"}]}]
        cm = build_identifier_case_map(info)
        assert cm["battery"] == "Battery"

    def test_mixed_case_column_picked(self):
        info = [{"name": "battery", "columns": [{"name": "SerialNumber"}]}]
        cm = build_identifier_case_map(info)
        assert cm["serialnumber"] == "SerialNumber"

    def test_lowercase_names_included(self):
        """All identifiers are included so the quoter can fix LLM
        over-quoting of lowercase names."""
        info = [{"name": "orders", "columns": [{"name": "id"}, {"name": "total"}]}]
        cm = build_identifier_case_map(info)
        assert cm == {"orders": "orders", "id": "id", "total": "total"}

    def test_combined_tables_and_columns(self):
        info = [
            {"name": "Battery", "columns": [{"name": "id"}, {"name": "SerialNumber"}]},
            {"name": "cable", "columns": [{"name": "id"}]},
        ]
        cm = build_identifier_case_map(info)
        assert cm["battery"] == "Battery"
        assert cm["serialnumber"] == "SerialNumber"
        assert cm["cable"] == "cable"
        assert cm["id"] == "id"

    def test_missing_columns_key_tolerated(self):
        info = [{"name": "Foo"}]
        cm = build_identifier_case_map(info)
        assert cm["foo"] == "Foo"


CASE_MAP = {
    "battery": "Battery",
    "cable": "Cable",
    "serialnumber": "SerialNumber",
    "id": "id",
    "cost": "cost",
    "equipment_id": "equipment_id",
    "lowercase_table": "lowercase_table",
}


class TestQuoteMixedCaseIdentifiers:
    def test_empty_case_map_returns_unchanged(self):
        sql = "SELECT * FROM Battery"
        assert quote_mixed_case_identifiers(sql, {}) == sql

    def test_empty_sql_returns_unchanged(self):
        assert quote_mixed_case_identifiers("", CASE_MAP) == ""

    def test_bare_mixed_case_reference_gets_quoted(self):
        out = quote_mixed_case_identifiers("SELECT * FROM Battery", CASE_MAP)
        assert '"Battery"' in out

    def test_lowercase_reference_gets_corrected_and_quoted(self):
        # Postgres would fold `battery` → lowercase and fail to match `Battery`
        out = quote_mixed_case_identifiers("SELECT * FROM battery", CASE_MAP)
        assert '"Battery"' in out

    def test_qualified_column_ref_gets_quoted(self):
        out = quote_mixed_case_identifiers("SELECT b.SerialNumber FROM Battery b", CASE_MAP)
        assert '"SerialNumber"' in out
        assert '"Battery"' in out

    def test_correctly_quoted_mixed_case_untouched(self):
        sql = 'SELECT "Battery".* FROM "Battery"'
        assert quote_mixed_case_identifiers(sql, CASE_MAP) == sql

    def test_wrongly_quoted_lowercase_column_unquoted(self):
        """LLM over-quotes a lowercase column (`"Cost"`) when the real
        column is `cost`. The quoter should unquote it so Postgres
        resolves the name correctly."""
        out = quote_mixed_case_identifiers(
            'SELECT equipment_id, "Cost" FROM "Cable" WHERE "Cost" > 100', CASE_MAP
        )
        assert '"Cost"' not in out
        assert "cost" in out
        assert '"Cable"' in out  # mixed-case table stays quoted

    def test_correctly_quoted_lowercase_normalized(self):
        """`"cost"` → `cost` (quoting a lowercase name is unnecessary)."""
        out = quote_mixed_case_identifiers('SELECT "cost" FROM "Cable"', CASE_MAP)
        assert '"cost"' not in out
        assert "cost" in out

    def test_wrongly_quoted_mixed_case_recased(self):
        """`"BATTERY"` should become `"Battery"` (original casing)."""
        out = quote_mixed_case_identifiers('SELECT * FROM "BATTERY"', CASE_MAP)
        assert '"Battery"' in out
        assert '"BATTERY"' not in out

    def test_string_literal_with_matching_text_untouched(self):
        out = quote_mixed_case_identifiers("SELECT 'Battery' AS label FROM Battery", CASE_MAP)
        assert "'Battery'" in out  # literal preserved
        assert 'FROM "Battery"' in out

    def test_lowercase_only_identifiers_not_quoted(self):
        out = quote_mixed_case_identifiers("SELECT * FROM lowercase_table", CASE_MAP)
        assert '"lowercase_table"' not in out

    def test_parse_error_falls_through(self):
        bad = "SELECT FROM WHERE"
        # Should not raise — returns input unchanged on parse failure
        assert quote_mixed_case_identifiers(bad, CASE_MAP) == bad


class _FakePostgresRunner:
    def __init__(self):
        self.mixed_case_identifiers: dict[str, str] | None = None
        self.special_identifiers: list[str] | None = None


class _FakeDuckDBRunner:
    def __init__(self):
        self.special_identifiers: list[str] | None = None


class _FakeCachingRunner:
    def __init__(self, inner):
        self._inner = inner


class TestConfigureRunnerIdentifierQuoting:
    def test_sets_on_postgres_runner(self):
        r = _FakePostgresRunner()
        configure_runner_identifier_quoting(r, [{"name": "Battery", "columns": [{"name": "id"}]}])
        assert r.mixed_case_identifiers == {"battery": "Battery", "id": "id"}

    def test_unwraps_caching_wrapper(self):
        inner = _FakePostgresRunner()
        outer = _FakeCachingRunner(inner)
        configure_runner_identifier_quoting(
            outer, [{"name": "Battery", "columns": [{"name": "id"}]}]
        )
        assert inner.mixed_case_identifiers == {"battery": "Battery", "id": "id"}

    def test_all_lowercase_schema_still_set(self):
        """Lowercase-only schemas still get a map so the quoter can fix
        LLM over-quoting of lowercase identifiers."""
        r = _FakePostgresRunner()
        configure_runner_identifier_quoting(r, [{"name": "orders", "columns": [{"name": "id"}]}])
        assert r.mixed_case_identifiers == {"orders": "orders", "id": "id"}

    def test_non_postgres_runner_is_noop(self):
        class Dummy:
            pass

        d = Dummy()
        configure_runner_identifier_quoting(d, [{"name": "Battery", "columns": [{"name": "id"}]}])
        assert not hasattr(d, "mixed_case_identifiers")

    def test_sets_special_identifiers_on_duckdb_runner(self):
        r = _FakeDuckDBRunner()
        configure_runner_identifier_quoting(
            r,
            [
                {
                    "name": "hosts",
                    "columns": [{"name": "Host Name"}, {"name": "id"}],
                }
            ],
        )
        assert r.special_identifiers == ["Host Name"]

    def test_special_identifiers_unset_when_no_spaces(self):
        r = _FakeDuckDBRunner()
        configure_runner_identifier_quoting(r, [{"name": "orders", "columns": [{"name": "id"}]}])
        assert r.special_identifiers is None

    def test_postgres_runner_gets_both_lists(self):
        r = _FakePostgresRunner()
        configure_runner_identifier_quoting(
            r,
            [
                {
                    "name": "Battery",
                    "columns": [{"name": "Host Name"}],
                }
            ],
        )
        assert r.mixed_case_identifiers == {"battery": "Battery", "host name": "Host Name"}
        assert r.special_identifiers == ["Host Name"]


class TestBuildSpecialIdentifierList:
    def test_picks_columns_with_spaces(self):
        info = [{"name": "t", "columns": [{"name": "Host Name"}, {"name": "id"}]}]
        assert build_special_identifier_list(info) == ["Host Name"]

    def test_picks_table_with_space(self):
        info = [{"name": "Power Plant", "columns": [{"name": "id"}]}]
        assert build_special_identifier_list(info) == ["Power Plant"]

    def test_orders_longest_words_first(self):
        info = [
            {
                "name": "t",
                "columns": [
                    {"name": "Net Generation MWh"},
                    {"name": "Net Generation"},
                ],
            }
        ]
        assert build_special_identifier_list(info) == [
            "Net Generation MWh",
            "Net Generation",
        ]

    def test_no_spaces_returns_empty(self):
        info = [{"name": "orders", "columns": [{"name": "id"}, {"name": "total"}]}]
        assert build_special_identifier_list(info) == []

    def test_dedupes_repeats_across_tables(self):
        info = [
            {"name": "a", "columns": [{"name": "Host Name"}]},
            {"name": "b", "columns": [{"name": "Host Name"}]},
        ]
        assert build_special_identifier_list(info) == ["Host Name"]


class TestQuoteSpecialIdentifiers:
    def test_empty_list_returns_unchanged(self):
        sql = "SELECT Host Name FROM hosts"
        assert quote_special_identifiers(sql, []) == sql

    def test_empty_sql_returns_unchanged(self):
        assert quote_special_identifiers("", ["Host Name"]) == ""

    def test_bare_two_word_name_quoted(self):
        out = quote_special_identifiers("SELECT Host Name FROM hosts", ["Host Name"])
        assert out == 'SELECT "Host Name" FROM hosts'

    def test_multiple_occurrences_quoted(self):
        out = quote_special_identifiers(
            "SELECT Host Name FROM hosts WHERE Host Name = 'foo'", ["Host Name"]
        )
        assert out == 'SELECT "Host Name" FROM hosts WHERE "Host Name" = \'foo\''

    def test_case_insensitive_match_uses_canonical_casing(self):
        out = quote_special_identifiers("SELECT host name FROM hosts", ["Host Name"])
        assert out == 'SELECT "Host Name" FROM hosts'

    def test_string_literal_left_alone(self):
        out = quote_special_identifiers("SELECT 'Host Name' AS label FROM hosts", ["Host Name"])
        assert out == "SELECT 'Host Name' AS label FROM hosts"

    def test_already_quoted_left_alone(self):
        sql = 'SELECT "Host Name" FROM hosts'
        assert quote_special_identifiers(sql, ["Host Name"]) == sql

    def test_line_comment_left_alone(self):
        out = quote_special_identifiers(
            "SELECT Host Name -- pick Host Name\nFROM hosts", ["Host Name"]
        )
        assert "-- pick Host Name" in out
        assert 'SELECT "Host Name"' in out

    def test_block_comment_left_alone(self):
        out = quote_special_identifiers(
            "SELECT Host Name /* skip Host Name here */ FROM hosts", ["Host Name"]
        )
        assert "/* skip Host Name here */" in out
        assert 'SELECT "Host Name"' in out

    def test_three_word_name_quoted(self):
        out = quote_special_identifiers(
            "SELECT Net Generation MWh FROM gen", ["Net Generation MWh"]
        )
        assert out == 'SELECT "Net Generation MWh" FROM gen'

    def test_longer_name_wins_over_prefix(self):
        out = quote_special_identifiers(
            "SELECT Net Generation MWh FROM gen",
            ["Net Generation MWh", "Net Generation"],
        )
        assert '"Net Generation MWh"' in out
        assert '"Net Generation" MWh' not in out

    def test_qualified_column_quoted(self):
        out = quote_special_identifiers("SELECT t.Host Name FROM hosts t", ["Host Name"])
        # Qualified-form survives: t."Host Name" is legal.
        assert 't."Host Name"' in out

    def test_substring_inside_other_identifier_not_matched(self):
        # `MyHost Name` should not match `Host Name` because the H is
        # preceded by an identifier character.
        out = quote_special_identifiers("SELECT MyHost Name FROM t", ["Host Name"])
        assert out == "SELECT MyHost Name FROM t"

    def test_no_match_returns_unchanged(self):
        sql = "SELECT id FROM orders"
        assert quote_special_identifiers(sql, ["Host Name"]) == sql

    def test_embedded_double_quote_escaped_per_sql_standard(self):
        """An identifier name containing a literal `"` (rare but legal in
        a CSV header) must be emitted as `"a "" b"`, not the malformed
        `"a " b"`. Use a single unpaired `"` so the literal-splitter
        leaves it in code chunks where the rewriter actually sees it."""
        out = quote_special_identifiers('SELECT a " b FROM t', ['a " b'])
        assert '"a "" b"' in out
        # And the broken form must not appear.
        assert 'SELECT a " b FROM t' not in out
