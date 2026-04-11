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

  it("extracts bare options without bullets or bold (em-dash)", () => {
    const text = `Which scenario would you like to analyze?

reference — Baseline EV adoption following current trends
efs_high_ldv — Aggressive electrification of light-duty vehicles
ldv_sales_evs_2035 — All new light-duty vehicle sales are electric by 2035`;
    expect(extractClarifyOptions(text)).toEqual([
      "reference",
      "efs_high_ldv",
      "ldv_sales_evs_2035",
    ]);
  });

  it("extracts bare options with regular hyphen separator", () => {
    const text = `Which scenario would you like?

reference - Baseline EV adoption
efs_high_ldv - High electrification
ldv_sales_evs_2035 - All EV sales by 2035`;
    expect(extractClarifyOptions(text)).toEqual([
      "reference",
      "efs_high_ldv",
      "ldv_sales_evs_2035",
    ]);
  });

  it("extracts numbered list options", () => {
    const text = `Which scenario would you like?

1. **reference** - Baseline EV adoption
2. **efs_high_ldv** - High electrification
3. **ldv_sales_evs_2035** - All EV sales by 2035`;
    expect(extractClarifyOptions(text)).toEqual([
      "reference",
      "efs_high_ldv",
      "ldv_sales_evs_2035",
    ]);
  });

  it("extracts numbered list options with colon separator", () => {
    const text = `Which scenario do you prefer?

1. reference: Baseline EV adoption
2. efs_high_ldv: High electrification
3. ldv_sales_evs_2035: All EV sales by 2035`;
    expect(extractClarifyOptions(text)).toEqual([
      "reference",
      "efs_high_ldv",
      "ldv_sales_evs_2035",
    ]);
  });

  it("extracts backtick-wrapped options", () => {
    const text = `Which scenario would you like?

- \`reference\` — Baseline EV adoption
- \`efs_high_ldv\` — High electrification
- \`ldv_sales_evs_2035\` — All EV sales by 2035`;
    expect(extractClarifyOptions(text)).toEqual([
      "reference",
      "efs_high_ldv",
      "ldv_sales_evs_2035",
    ]);
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
