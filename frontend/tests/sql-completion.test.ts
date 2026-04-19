import { describe, it, expect } from "vitest";
import { extractFromTables } from "$lib/utils/sql-completion";

describe("extractFromTables", () => {
  it("pulls a single FROM table with no alias", () => {
    expect(extractFromTables("SELECT * FROM generation_fuel WHERE")).toEqual([
      { table: "generation_fuel", alias: null },
    ]);
  });

  it("picks up an AS alias", () => {
    expect(
      extractFromTables("SELECT * FROM generation_fuel AS g WHERE"),
    ).toEqual([{ table: "generation_fuel", alias: "g" }]);
  });

  it("picks up an implicit alias", () => {
    expect(extractFromTables("SELECT * FROM generation_fuel g WHERE")).toEqual(
      [{ table: "generation_fuel", alias: "g" }],
    );
  });

  it("does not treat reserved words as aliases", () => {
    expect(extractFromTables("SELECT * FROM generation_fuel WHERE x = 1"))
      .toEqual([{ table: "generation_fuel", alias: null }]);
    expect(
      extractFromTables(
        "SELECT * FROM plants p JOIN generation_fuel g ON p.id = g.id",
      ),
    ).toEqual([
      { table: "plants", alias: "p" },
      { table: "generation_fuel", alias: "g" },
    ]);
  });

  it("handles comma-separated FROM lists", () => {
    expect(extractFromTables("SELECT * FROM plants p, generation_fuel g"))
      .toEqual([
        { table: "plants", alias: "p" },
        { table: "generation_fuel", alias: "g" },
      ]);
  });

  it("handles multiple JOINs", () => {
    expect(
      extractFromTables(
        "SELECT * FROM plants p LEFT JOIN generation_fuel g ON p.id = g.id INNER JOIN fuel_types ft ON g.fuel = ft.code",
      ),
    ).toEqual([
      { table: "plants", alias: "p" },
      { table: "generation_fuel", alias: "g" },
      { table: "fuel_types", alias: "ft" },
    ]);
  });

  it("strips quoted identifiers", () => {
    expect(extractFromTables('SELECT * FROM "My Table" mt')).toEqual([
      { table: "My Table", alias: "mt" },
    ]);
  });

  it("is case-insensitive on keywords", () => {
    expect(extractFromTables("select * from plants")).toEqual([
      { table: "plants", alias: null },
    ]);
  });

  it("returns empty when there's no FROM clause yet", () => {
    expect(extractFromTables("SELECT 1")).toEqual([]);
    expect(extractFromTables("")).toEqual([]);
  });

  it("ignores FROM inside a string literal", () => {
    expect(extractFromTables("SELECT 'from plants' FROM generation_fuel"))
      .toEqual([{ table: "generation_fuel", alias: null }]);
  });

  it("handles schema-qualified table names", () => {
    expect(extractFromTables("SELECT * FROM public.plants p")).toEqual([
      { table: "public.plants", alias: "p" },
    ]);
  });
});
