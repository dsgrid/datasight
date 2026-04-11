import { describe, it, expect } from "vitest";
import {
  escapeHtml,
  scoreFuzzySubsequence,
  scorePaletteResult,
  highlightMatch,
  getVisibleSchemaEntries,
} from "$lib/utils/search";

describe("escapeHtml", () => {
  it("escapes HTML special characters", () => {
    expect(escapeHtml('<script>"xss" & \'test\'')).toBe(
      "&lt;script&gt;&quot;xss&quot; &amp; &#39;test&#39;",
    );
  });

  it("handles empty/null input", () => {
    expect(escapeHtml("")).toBe("");
  });
});

describe("scoreFuzzySubsequence", () => {
  it("returns -1 for no match", () => {
    expect(scoreFuzzySubsequence("xyz", "abc")).toBe(-1);
  });

  it("returns positive score for exact match", () => {
    expect(scoreFuzzySubsequence("abc", "abc")).toBeGreaterThan(0);
  });

  it("returns positive score for subsequence match", () => {
    expect(scoreFuzzySubsequence("ac", "abc")).toBeGreaterThan(0);
  });

  it("gives higher score for contiguous matches", () => {
    const contiguous = scoreFuzzySubsequence("ab", "abc");
    const spread = scoreFuzzySubsequence("ac", "abcde");
    expect(contiguous).toBeGreaterThan(spread);
  });

  it("gives higher score for earlier matches", () => {
    const early = scoreFuzzySubsequence("a", "abc");
    const late = scoreFuzzySubsequence("c", "abc");
    expect(early).toBeGreaterThanOrEqual(late);
  });

  it("handles empty query", () => {
    expect(scoreFuzzySubsequence("", "abc")).toBe(-1);
  });
});

describe("scorePaletteResult", () => {
  it("returns base score when no query", () => {
    expect(scorePaletteResult("", ["test"], 800)).toBe(800);
  });

  it("scores exact match highest", () => {
    const exact = scorePaletteResult("test", ["test"], 100);
    const prefix = scorePaletteResult("test", ["testing"], 100);
    expect(exact).toBeGreaterThan(prefix);
  });

  it("scores prefix match higher than substring", () => {
    const prefix = scorePaletteResult("test", ["testing"], 100);
    const substring = scorePaletteResult("test", ["a_testing"], 100);
    expect(prefix).toBeGreaterThan(substring);
  });

  it("returns -1 when no haystack matches", () => {
    expect(scorePaletteResult("xyz", ["abc", "def"], 100)).toBe(-1);
  });

  it("uses best match across multiple haystacks", () => {
    const score = scorePaletteResult("test", ["no_match", "test"], 100);
    expect(score).toBe(100 + 120); // exact match bonus
  });
});

describe("highlightMatch", () => {
  it("wraps matching text in mark tags", () => {
    expect(highlightMatch("hello world", "world")).toBe(
      'hello <mark class="schema-match">world</mark>',
    );
  });

  it("returns escaped text when no match", () => {
    expect(highlightMatch("hello", "xyz")).toBe("hello");
  });

  it("returns escaped text when no query", () => {
    expect(highlightMatch("hello <b>", "")).toBe("hello &lt;b&gt;");
  });

  it("is case-insensitive", () => {
    expect(highlightMatch("Hello", "hello")).toBe(
      '<mark class="schema-match">Hello</mark>',
    );
  });
});

describe("getVisibleSchemaEntries", () => {
  const tables = [
    { name: "users", columns: [{ name: "id" }, { name: "email" }] },
    { name: "orders", columns: [{ name: "id" }, { name: "user_id" }] },
    { name: "products", columns: [{ name: "name" }, { name: "price" }] },
  ];

  it("returns all tables when no query", () => {
    const result = getVisibleSchemaEntries(tables, "");
    expect(result).toHaveLength(3);
    expect(result[0].tableMatches).toBe(false);
  });

  it("filters by table name", () => {
    const result = getVisibleSchemaEntries(tables, "user");
    // "user" matches table "users" and column "user_id" in orders
    expect(result).toHaveLength(2);
    expect(result[0].table.name).toBe("users");
    expect(result[0].tableMatches).toBe(true);
    expect(result[1].table.name).toBe("orders");
    expect(result[1].matchingColumns[0].name).toBe("user_id");
  });

  it("filters by column name", () => {
    const result = getVisibleSchemaEntries(tables, "price");
    expect(result).toHaveLength(1);
    expect(result[0].table.name).toBe("products");
    expect(result[0].matchingColumns).toHaveLength(1);
    expect(result[0].matchingColumns[0].name).toBe("price");
  });

  it("matches across tables and columns", () => {
    const result = getVisibleSchemaEntries(tables, "id");
    // "id" appears as column in users and orders
    expect(result).toHaveLength(2);
  });
});
