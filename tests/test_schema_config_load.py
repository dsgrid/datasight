"""Tests for ``load_schema_config`` — schema.yaml parsing."""

from datasight.config import load_schema_config


def test_missing_file_returns_none(tmp_path):
    assert load_schema_config(None, str(tmp_path)) is None


def test_explicit_missing_path_returns_none(tmp_path):
    missing = tmp_path / "nope.yaml"
    assert load_schema_config(str(missing), str(tmp_path)) is None


def test_basic_tables_list(tmp_path):
    (tmp_path / "schema.yaml").write_text(
        "tables:\n  - name: orders\n  - name: customers\n", encoding="utf-8"
    )
    cfg = load_schema_config(None, str(tmp_path))
    assert cfg == {"tables": [{"name": "orders"}, {"name": "customers"}]}


def test_columns_preserved(tmp_path):
    (tmp_path / "schema.yaml").write_text(
        "tables:\n  - name: orders\n    columns: [id, total]\n",
        encoding="utf-8",
    )
    cfg = load_schema_config(None, str(tmp_path))
    assert cfg == {"tables": [{"name": "orders", "columns": ["id", "total"]}]}


def test_excluded_columns_preserved(tmp_path):
    (tmp_path / "schema.yaml").write_text(
        "tables:\n  - name: orders\n    excluded_columns: [debug_*, _internal]\n",
        encoding="utf-8",
    )
    cfg = load_schema_config(None, str(tmp_path))
    assert cfg == {"tables": [{"name": "orders", "excluded_columns": ["debug_*", "_internal"]}]}


def test_null_columns_becomes_empty_list(tmp_path):
    (tmp_path / "schema.yaml").write_text(
        "tables:\n  - name: orders\n    columns:\n", encoding="utf-8"
    )
    cfg = load_schema_config(None, str(tmp_path))
    assert cfg == {"tables": [{"name": "orders", "columns": []}]}


def test_null_excluded_columns_becomes_empty_list(tmp_path):
    (tmp_path / "schema.yaml").write_text(
        "tables:\n  - name: orders\n    excluded_columns:\n", encoding="utf-8"
    )
    cfg = load_schema_config(None, str(tmp_path))
    assert cfg == {"tables": [{"name": "orders", "excluded_columns": []}]}


def test_non_list_columns_dropped_with_warning(tmp_path):
    (tmp_path / "schema.yaml").write_text(
        "tables:\n  - name: orders\n    columns: not-a-list\n",
        encoding="utf-8",
    )
    cfg = load_schema_config(None, str(tmp_path))
    # field dropped, but entry kept
    assert cfg == {"tables": [{"name": "orders"}]}


def test_whitespace_stripped_empty_names_dropped(tmp_path):
    (tmp_path / "schema.yaml").write_text(
        "tables:\n  - name: orders\n    columns: ['  id  ', '', '  ']\n",
        encoding="utf-8",
    )
    cfg = load_schema_config(None, str(tmp_path))
    assert cfg == {"tables": [{"name": "orders", "columns": ["id"]}]}


def test_entries_without_name_skipped(tmp_path):
    (tmp_path / "schema.yaml").write_text(
        "tables:\n  - name: orders\n  - columns: [x]\n", encoding="utf-8"
    )
    cfg = load_schema_config(None, str(tmp_path))
    assert cfg == {"tables": [{"name": "orders"}]}


def test_malformed_yaml_returns_none(tmp_path):
    (tmp_path / "schema.yaml").write_text("tables: [\n  - bad\n", encoding="utf-8")
    assert load_schema_config(None, str(tmp_path)) is None


def test_top_level_not_mapping_returns_none(tmp_path):
    (tmp_path / "schema.yaml").write_text("- not-a-mapping\n", encoding="utf-8")
    assert load_schema_config(None, str(tmp_path)) is None


def test_missing_tables_key_returns_empty_list(tmp_path):
    (tmp_path / "schema.yaml").write_text("notes: hi\n", encoding="utf-8")
    cfg = load_schema_config(None, str(tmp_path))
    assert cfg == {"tables": []}


def test_explicit_path_overrides_default(tmp_path):
    default = tmp_path / "schema.yaml"
    default.write_text("tables:\n  - name: from_default\n", encoding="utf-8")
    other = tmp_path / "other.yaml"
    other.write_text("tables:\n  - name: from_other\n", encoding="utf-8")
    cfg = load_schema_config(str(other), str(tmp_path))
    assert cfg == {"tables": [{"name": "from_other"}]}


class TestGenerateOutputRoundtrip:
    """The YAML shape emitted by ``datasight generate`` must parse back
    into the exact structure ``filter_tables`` expects."""

    def test_default_format_parses(self, tmp_path):
        (tmp_path / "schema.yaml").write_text(
            "tables:\n"
            "  - name: Orders\n"
            "    excluded_columns: []\n"
            "  - name: Customers\n"
            "    excluded_columns: []\n",
            encoding="utf-8",
        )
        cfg = load_schema_config(None, str(tmp_path))
        assert cfg == {
            "tables": [
                {"name": "Orders", "excluded_columns": []},
                {"name": "Customers", "excluded_columns": []},
            ]
        }

    def test_compact_format_parses(self, tmp_path):
        (tmp_path / "schema.yaml").write_text(
            "tables:\n  - name: Orders\n  - name: Customers\n", encoding="utf-8"
        )
        cfg = load_schema_config(None, str(tmp_path))
        assert cfg == {"tables": [{"name": "Orders"}, {"name": "Customers"}]}

    def test_default_format_exposes_all_columns(self, tmp_path):
        """Empty excluded_columns behaves as if no filter was set."""
        from datasight.schema import ColumnInfo, TableInfo, filter_tables

        (tmp_path / "schema.yaml").write_text(
            "tables:\n  - name: Orders\n    excluded_columns: []\n",
            encoding="utf-8",
        )
        cfg = load_schema_config(None, str(tmp_path))
        tables = [
            TableInfo(
                name="Orders",
                columns=[ColumnInfo("id", "int"), ColumnInfo("total", "int")],
            )
        ]
        out = filter_tables(tables, cfg)
        assert [c.name for c in out[0].columns] == ["id", "total"]
