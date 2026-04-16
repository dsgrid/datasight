"""Tests for ``filter_tables`` — schema.yaml table/column filtering."""

from datasight.schema import ColumnInfo, TableInfo, filter_tables


def _table(name: str, cols: list[str]) -> TableInfo:
    return TableInfo(name=name, columns=[ColumnInfo(c, "int") for c in cols])


def _names(tables: list[TableInfo], i: int = 0) -> list[str]:
    return [c.name for c in tables[i].columns]


class TestTableAllowlist:
    def test_listed_tables_survive(self):
        t = [_table("a", ["x"]), _table("b", ["x"]), _table("c", ["x"])]
        out = filter_tables(t, {"tables": [{"name": "a"}, {"name": "c"}]})
        assert [x.name for x in out] == ["a", "c"]

    def test_unlisted_tables_dropped(self):
        t = [_table("a", ["x"]), _table("b", ["x"])]
        out = filter_tables(t, {"tables": [{"name": "a"}]})
        assert [x.name for x in out] == ["a"]

    def test_table_name_case_insensitive(self):
        t = [_table("Orders", ["id"])]
        out = filter_tables(t, {"tables": [{"name": "orders"}]})
        assert out and out[0].name == "Orders"

    def test_unknown_table_logged_not_error(self, caplog):
        t = [_table("a", ["x"])]
        out = filter_tables(t, {"tables": [{"name": "a"}, {"name": "nope"}]})
        assert [x.name for x in out] == ["a"]

    def test_none_config_passes_through(self):
        t = [_table("a", ["x"])]
        assert filter_tables(t, None) is t

    def test_empty_tables_key_passes_through(self):
        t = [_table("a", ["x"])]
        assert filter_tables(t, {"tables": []}) is t


class TestColumnAllowlist:
    def test_exact_allowlist(self):
        t = [_table("a", ["id", "name", "secret"])]
        out = filter_tables(t, {"tables": [{"name": "a", "columns": ["id", "name"]}]})
        assert _names(out) == ["id", "name"]

    def test_order_preserved_from_config(self):
        t = [_table("a", ["id", "name", "date"])]
        out = filter_tables(t, {"tables": [{"name": "a", "columns": ["date", "id"]}]})
        assert _names(out) == ["date", "id"]

    def test_unknown_column_dropped(self):
        t = [_table("a", ["id", "name"])]
        out = filter_tables(t, {"tables": [{"name": "a", "columns": ["id", "ghost"]}]})
        assert _names(out) == ["id"]

    def test_all_invalid_columns_skips_table(self):
        t = [_table("a", ["id"])]
        out = filter_tables(t, {"tables": [{"name": "a", "columns": ["ghost"]}]})
        assert out == []

    def test_empty_list_treated_as_omitted(self):
        t = [_table("a", ["id", "name"])]
        out = filter_tables(t, {"tables": [{"name": "a", "columns": []}]})
        assert _names(out) == ["id", "name"]

    def test_column_match_case_insensitive(self):
        t = [_table("a", ["Id", "Name"])]
        out = filter_tables(t, {"tables": [{"name": "a", "columns": ["id"]}]})
        assert _names(out) == ["Id"]


class TestColumnDenylist:
    def test_exact_exclusion(self):
        t = [_table("a", ["id", "debug_flag", "name"])]
        out = filter_tables(t, {"tables": [{"name": "a", "excluded_columns": ["debug_flag"]}]})
        assert _names(out) == ["id", "name"]

    def test_glob_star(self):
        t = [_table("a", ["id", "debug_x", "debug_y", "name"])]
        out = filter_tables(t, {"tables": [{"name": "a", "excluded_columns": ["debug_*"]}]})
        assert _names(out) == ["id", "name"]

    def test_glob_charset(self):
        t = [_table("a", [f"sensor_{i}" for i in range(12)])]
        out = filter_tables(
            t,
            {"tables": [{"name": "a", "excluded_columns": ["sensor_[1-9]*"]}]},
        )
        # sensors 1..9 and 10, 11 all match; only sensor_0 remains
        assert _names(out) == ["sensor_0"]

    def test_question_mark(self):
        t = [_table("a", ["c1", "c2", "c10"])]
        out = filter_tables(t, {"tables": [{"name": "a", "excluded_columns": ["c?"]}]})
        assert _names(out) == ["c10"]

    def test_empty_list_treated_as_omitted(self):
        t = [_table("a", ["id", "name"])]
        out = filter_tables(t, {"tables": [{"name": "a", "excluded_columns": []}]})
        assert _names(out) == ["id", "name"]

    def test_excludes_all_skips_table(self):
        t = [_table("a", ["id", "name"])]
        out = filter_tables(t, {"tables": [{"name": "a", "excluded_columns": ["*"]}]})
        assert out == []

    def test_case_insensitive(self):
        t = [_table("a", ["Debug", "DEBUG_2", "keep"])]
        out = filter_tables(t, {"tables": [{"name": "a", "excluded_columns": ["debug*"]}]})
        assert _names(out) == ["keep"]


class TestBothFieldsSet:
    def test_columns_wins_when_both_set(self, caplog):
        t = [_table("a", ["id", "debug_flag", "name"])]
        out = filter_tables(
            t,
            {
                "tables": [
                    {
                        "name": "a",
                        "columns": ["id", "debug_flag"],
                        "excluded_columns": ["debug_*"],
                    }
                ]
            },
        )
        # columns wins: debug_flag is included even though it matches the exclude glob
        assert _names(out) == ["id", "debug_flag"]

    def test_empty_columns_with_exclusions_applies_exclusions(self):
        t = [_table("a", ["id", "debug_flag", "name"])]
        out = filter_tables(
            t,
            {
                "tables": [
                    {
                        "name": "a",
                        "columns": [],
                        "excluded_columns": ["debug_*"],
                    }
                ]
            },
        )
        assert _names(out) == ["id", "name"]


class TestNeitherFieldSet:
    def test_all_columns_exposed(self):
        t = [_table("a", ["id", "name", "anything"])]
        out = filter_tables(t, {"tables": [{"name": "a"}]})
        assert _names(out) == ["id", "name", "anything"]


class TestDriftLogging:
    def test_hidden_count_logged(self, caplog):
        import logging

        caplog.set_level(logging.INFO)
        # Bridge loguru → stdlib so caplog captures it
        from loguru import logger as loguru_logger

        sink_id = loguru_logger.add(
            lambda m: logging.getLogger("loguru").info(m.rstrip()), level="INFO"
        )
        try:
            t = [_table("Orders", ["id", "name", "new_col"])]
            filter_tables(t, {"tables": [{"name": "Orders", "columns": ["id", "name"]}]})
        finally:
            loguru_logger.remove(sink_id)

        messages = " ".join(r.message for r in caplog.records)
        assert "Orders exposing 2 of 3" in messages
        assert "new_col" in messages

    def test_no_hidden_no_per_table_log(self, caplog):
        import logging

        caplog.set_level(logging.INFO)
        from loguru import logger as loguru_logger

        sink_id = loguru_logger.add(
            lambda m: logging.getLogger("loguru").info(m.rstrip()), level="INFO"
        )
        try:
            t = [_table("a", ["id", "name"])]
            filter_tables(t, {"tables": [{"name": "a", "columns": ["id", "name"]}]})
        finally:
            loguru_logger.remove(sink_id)

        messages = " ".join(r.message for r in caplog.records)
        assert "exposing" not in messages
