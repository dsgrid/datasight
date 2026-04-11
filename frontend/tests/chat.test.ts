import { describe, it, expect } from "vitest";
import { extractClarifyOptions } from "$lib/api/chat";

describe("extractClarifyOptions", () => {
  it("returns empty array when no question mark", () => {
    expect(extractClarifyOptions("Just a statement")).toEqual([]);
  });

  it("extracts bold options with dash separator", () => {
    const text = `Which approach would you prefer?
- **Option A** — Do the first thing
- **Option B** — Do the second thing
- **Option C** — Do the third thing`;
    expect(extractClarifyOptions(text)).toEqual([
      "Option A",
      "Option B",
      "Option C",
    ]);
  });

  it("extracts bullet options with dash separator", () => {
    const text = `Would you like to analyze by?
- Region — Geographic breakdown
- Time — Temporal trends`;
    expect(extractClarifyOptions(text)).toEqual(["Region", "Time"]);
  });

  it("returns empty when fewer than 2 options", () => {
    const text = `Do you want this?
- **Only one option** — Just this`;
    expect(extractClarifyOptions(text)).toEqual([]);
  });

  it("ignores analysis text with colon separators", () => {
    const text = `Here are the results?
The data shows:
- Revenue: $1M
- Growth: 15%`;
    // These use ":" not "—" so they shouldn't match
    expect(extractClarifyOptions(text)).toEqual([]);
  });
});
