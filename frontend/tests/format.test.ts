import { describe, it, expect } from "vitest";
import { formatCost, formatTimestamp, formatDuration, pluralize } from "$lib/utils/format";

describe("formatCost", () => {
  it("uses 4 decimal places for small amounts", () => {
    expect(formatCost(0.005)).toBe("$0.0050");
    expect(formatCost(0.0001)).toBe("$0.0001");
  });

  it("uses 2 decimal places for larger amounts", () => {
    expect(formatCost(0.5)).toBe("$0.50");
    expect(formatCost(1.234)).toBe("$1.23");
  });

  it("handles zero", () => {
    expect(formatCost(0)).toBe("$0.0000");
  });
});

describe("formatDuration", () => {
  it("formats milliseconds for short durations", () => {
    expect(formatDuration(42)).toBe("42ms");
    expect(formatDuration(999)).toBe("999ms");
  });

  it("formats seconds for longer durations", () => {
    expect(formatDuration(1000)).toBe("1.0s");
    expect(formatDuration(2500)).toBe("2.5s");
  });
});

describe("formatTimestamp", () => {
  it("returns a time string", () => {
    const result = formatTimestamp("2024-01-15T10:30:45Z");
    // Just check it returns something reasonable — locale varies
    expect(result).toBeTruthy();
    expect(result.length).toBeGreaterThan(0);
  });
});

describe("pluralize", () => {
  it("uses singular for count of 1", () => {
    expect(pluralize(1, "message")).toBe("1 message");
  });

  it("uses plural for other counts", () => {
    expect(pluralize(0, "message")).toBe("0 messages");
    expect(pluralize(5, "message")).toBe("5 messages");
  });
});
