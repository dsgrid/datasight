"""Tests for Postgres mixed-case identifier quoting."""

from datasight.identifiers import (
    build_identifier_case_map,
    configure_runner_identifier_quoting,
    quote_mixed_case_identifiers,
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
