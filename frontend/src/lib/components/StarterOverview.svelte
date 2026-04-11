<script lang="ts">
  import { sendMessage } from "$lib/api/chat";
  import { escapeHtml } from "$lib/utils/search";

  interface Props {
    kind: string;
    overview: Record<string, unknown>;
  }

  let { kind, overview }: Props = $props();

  interface OverviewAction {
    label: string;
    prompt: string;
  }

  interface MetricDef {
    label: string;
    value: string;
  }

  interface SectionDef {
    title: string;
    items: unknown[];
    empty: string;
    render: (item: unknown) => string;
    rich?: boolean;
    note?: boolean;
  }

  // Build the overview config based on kind
  let config = $derived.by(() => {
    switch (kind) {
      case "profile":
        return buildDatasetConfig(overview);
      case "measures":
        return buildMeasureConfig(overview);
      case "dimensions":
        return buildDimensionConfig(overview);
      case "quality":
        return buildQualityConfig(overview);
      case "trend":
        return buildTrendConfig(overview);
      default:
        return {
          title: "Overview",
          description: "",
          metrics: [],
          sections: [],
          actions: [],
        };
    }
  });

  // ── Config builders ──

  function buildDatasetConfig(o: Record<string, unknown>) {
    const largestTables = (o.largest_tables || []) as Array<Record<string, unknown>>;
    const dateColumns = (o.date_columns || []) as Array<Record<string, unknown>>;
    const measureColumns = (o.measure_columns || []) as Array<Record<string, unknown>>;
    const dimensionColumns = (o.dimension_columns || []) as Array<Record<string, unknown>>;
    const qualityFlags = (o.quality_flags || []) as string[];
    const actions: OverviewAction[] = [];

    if (largestTables.length) {
      actions.push({
        label: `Profile ${largestTables[0].name}`,
        prompt: `Profile the \`${largestTables[0].name}\` table and highlight its most important columns.`,
      });
    }
    if (dateColumns.length && measureColumns.length) {
      actions.push({
        label: "Build a First Trend",
        prompt: `Create a trend chart of \`${measureColumns[0].table}.${measureColumns[0].column}\` over \`${dateColumns[0].table}.${dateColumns[0].column}\` and explain why this is a good starting view.`,
      });
    }

    return {
      title: "Dataset profile",
      description: "A deterministic overview of the schema to help you get oriented before asking follow-up questions.",
      metrics: [
        { label: "Tables", value: num(o.table_count) },
        { label: "Columns", value: num(o.total_columns) },
        { label: "Rows", value: num(o.total_rows) },
      ],
      sections: [
        listSection("Largest tables", largestTables, "No table metadata available.",
          (i: Record<string, unknown>) => `<strong>${esc(i.name)}</strong> <span>${num(i.row_count)} rows</span> <span>${i.column_count} cols</span>`),
        listSection("Date coverage", dateColumns, "No obvious date columns detected.",
          (i: Record<string, unknown>) => `<strong>${esc(i.table)}.${esc(i.column)}</strong> <span>${esc(i.min || "?")} → ${esc(i.max || "?")}</span>`),
        listSection("Measure candidates", measureColumns, "No obvious numeric measure columns detected.",
          (i: Record<string, unknown>) => `<strong>${esc(i.table)}.${esc(i.column)}</strong> <span>${esc(i.dtype || "unknown")}</span>`),
        listSection("Dimension candidates", dimensionColumns, "No obvious text dimensions detected.",
          (i: Record<string, unknown>) => {
            const samples = (i.sample_values as string[] || []).join(", ");
            const stats = [];
            if (i.distinct_count != null) stats.push(`${i.distinct_count} distinct`);
            if (i.null_rate != null) stats.push(`${i.null_rate}% null`);
            return `<strong>${esc(i.table)}.${esc(i.column)}</strong> <span>${esc(stats.join(" · ") || "")}</span> <span>${esc(samples ? `Samples: ${samples}` : "")}</span>`;
          }, { rich: true }),
        ...(qualityFlags.length ? [listSection("Quick notes", qualityFlags, "",
          (i: string) => `<span>${esc(i)}</span>`, { note: true })] : []),
      ],
      actions,
    };
  }

  function buildMeasureConfig(o: Record<string, unknown>) {
    const measures = (o.measures || []) as Array<Record<string, unknown>>;
    const notes = (o.notes || []) as string[];
    const actions: OverviewAction[] = [];

    if (measures.length) {
      const p = measures[0];
      const label = (p.display_name || `${p.table}.${p.column}`) as string;
      actions.push({
        label: "Use Default Aggregation",
        prompt: `Analyze \`${label}\` using its default \`${p.default_aggregation}\` aggregation. Use this rollup shape as a guide: \`${p.recommended_rollup_sql}\`. Explain why that aggregation matches the metric.`,
      });
    }

    return {
      title: "Key measures",
      description: "An energy-aware pass over likely measures, their default aggregations, and the metrics that need extra care.",
      metrics: [
        { label: "Tables", value: num(o.table_count) },
        { label: "Measures", value: String(measures.length) },
        { label: "Guardrails", value: String(measures.filter(m => ((m.forbidden_aggregations as string[]) || []).length).length) },
      ],
      sections: [
        listSection("Measure candidates", measures, "No obvious numeric measures detected.",
          (i: Record<string, unknown>) => {
            const additive = [];
            if (i.additive_across_category) additive.push("category");
            if (i.additive_across_time) additive.push("time");
            return `<strong>${esc(i.table)}.${esc(i.column)}</strong> <span>${esc(i.role)} · default ${esc(String(i.default_aggregation).toUpperCase())}</span> <span>${esc(additive.length ? "Additive: " + additive.join(" + ") : "Not safely additive")}</span>`;
          }, { rich: true }),
        listSection("Aggregation guidance", measures, "No aggregation guidance available.",
          (i: Record<string, unknown>) => {
            const allowed = ((i.allowed_aggregations as string[]) || []).join(", ");
            const forbidden = (i.forbidden_aggregations as string[]) || [];
            return `<strong>${esc(i.table)}.${esc(i.column)}</strong> <span>Allowed: ${esc(allowed)}</span>${forbidden.length ? ` <span>Avoid: ${esc(forbidden.join(", "))}</span>` : ""}`;
          }, { rich: true }),
        ...(notes.length ? [listSection("Quick notes", notes, "", (i: string) => `<span>${esc(i)}</span>`, { note: true })] : []),
      ],
      actions,
    };
  }

  function buildDimensionConfig(o: Record<string, unknown>) {
    const dims = (o.dimension_columns || []) as Array<Record<string, unknown>>;
    const dates = (o.date_columns || []) as Array<Record<string, unknown>>;
    const measures = (o.measure_columns || []) as Array<Record<string, unknown>>;
    const breakdowns = (o.suggested_breakdowns || []) as Array<Record<string, unknown>>;
    const joinHints = (o.join_hints || []) as string[];
    const actions: OverviewAction[] = [];

    if (breakdowns.length) {
      actions.push({
        label: "Run Top Breakdown",
        prompt: `Show the most useful breakdown using \`${breakdowns[0].table}.${breakdowns[0].column}\` and explain what stands out.`,
      });
    }

    return {
      title: "Key dimensions",
      description: "A deterministic pass over likely grouping fields, common breakdowns, and join hints.",
      metrics: [
        { label: "Tables", value: num(o.table_count) },
        { label: "Dimensions", value: String(dims.length) },
        { label: "Measures", value: String(measures.length) },
      ],
      sections: [
        listSection("Best grouping columns", dims, "No strong text dimensions detected.",
          (i: Record<string, unknown>) => {
            const stats = [];
            if (i.distinct_count != null) stats.push(`${i.distinct_count} distinct`);
            if (i.null_rate != null) stats.push(`${i.null_rate}% null`);
            const samples = ((i.sample_values as string[]) || []).join(", ");
            return `<strong>${esc(i.table)}.${esc(i.column)}</strong> <span>${esc(stats.join(" · "))}</span>${samples ? ` <span>Samples: ${esc(samples)}</span>` : ""}`;
          }, { rich: true }),
        listSection("Suggested breakdowns", breakdowns, "No obvious breakdown suggestions yet.",
          (i: Record<string, unknown>) => `<strong>${esc(i.table)}.${esc(i.column)}</strong> <span>${esc(i.reason || "")}</span>`),
        listSection("Date columns", dates, "No obvious date columns detected.",
          (i: Record<string, unknown>) => `<strong>${esc(i.table)}.${esc(i.column)}</strong> <span>${esc(i.min || "?")} → ${esc(i.max || "?")}</span>`),
        ...(joinHints.length ? [listSection("Join hints", joinHints, "", (i: string) => `<span>${esc(i)}</span>`, { note: true })] : []),
      ],
      actions,
    };
  }

  function buildQualityConfig(o: Record<string, unknown>) {
    const nullCols = (o.null_columns || []) as Array<Record<string, unknown>>;
    const numericFlags = (o.numeric_flags || []) as Array<Record<string, unknown>>;
    const dateCols = (o.date_columns || []) as Array<Record<string, unknown>>;
    const notes = (o.notes || []) as string[];
    const actions: OverviewAction[] = [];

    if (nullCols.length) {
      actions.push({
        label: "Investigate Nulls",
        prompt: `Investigate why \`${nullCols[0].table}.${nullCols[0].column}\` has ${nullCols[0].null_rate}% null values and show how those nulls are distributed.`,
      });
    }

    return {
      title: "Data quality audit",
      description: "A deterministic first pass over null-heavy columns, numeric range anomalies, and date coverage.",
      metrics: [
        { label: "Tables", value: num(o.table_count) },
        { label: "Null Issues", value: String(nullCols.length) },
        { label: "Range Flags", value: String(numericFlags.length) },
      ],
      sections: [
        listSection("Null-heavy columns", nullCols, "No null-heavy columns detected.",
          (i: Record<string, unknown>) => `<strong>${esc(i.table)}.${esc(i.column)}</strong> <span>${i.null_rate || 0}% null</span> <span>${i.null_count || 0} nulls</span>`, { rich: true }),
        listSection("Numeric range flags", numericFlags, "No obvious numeric range issues detected.",
          (i: Record<string, unknown>) => `<strong>${esc(i.table)}.${esc(i.column)}</strong> <span>${esc(i.issue || "")}</span>`),
        listSection("Date coverage", dateCols, "No obvious date columns detected.",
          (i: Record<string, unknown>) => `<strong>${esc(i.table)}.${esc(i.column)}</strong> <span>${esc(i.min || "?")} → ${esc(i.max || "?")}</span>`),
        ...(notes.length ? [listSection("Quick notes", notes, "", (i: string) => `<span>${esc(i)}</span>`, { note: true })] : []),
      ],
      actions,
    };
  }

  function buildTrendConfig(o: Record<string, unknown>) {
    const candidates = (o.trend_candidates || []) as Array<Record<string, unknown>>;
    const breakouts = (o.breakout_dimensions || []) as Array<Record<string, unknown>>;
    const chartRecs = (o.chart_recommendations || []) as Array<Record<string, unknown>>;
    const notes = (o.notes || []) as string[];
    const actions: OverviewAction[] = [];

    if (candidates.length) {
      const c = candidates[0];
      actions.push({
        label: "Create Starter Chart",
        prompt: `Create a line chart of \`${String(c.aggregation || "sum").toUpperCase()}(${c.table}.${c.measure_column})\` over \`${c.table}.${c.date_column}\`.`,
      });
    }

    return {
      title: "Trend chart ideas",
      description: "A deterministic pass over likely date columns, measures, and chart setups worth trying first.",
      metrics: [
        { label: "Tables", value: num(o.table_count) },
        { label: "Trend Pairs", value: String(candidates.length) },
        { label: "Breakouts", value: String(breakouts.length) },
      ],
      sections: [
        listSection("Best time-series pairs", candidates, "No obvious date/measure pairs detected.",
          (i: Record<string, unknown>) => `<strong>${esc(String(i.aggregation || "sum").toUpperCase())}(${esc(i.table)}.${esc(i.measure_column)})</strong> <span>by ${esc(i.date_column)}</span> <span>${esc(i.date_range || "")}</span>`, { rich: true }),
        listSection("Chart recommendations", chartRecs, "No starter chart recommendations available.",
          (i: Record<string, unknown>) => `<strong>${esc(i.title || "")}</strong> <span>${esc(i.chart_type || "")} · ${esc(String(i.aggregation || "").toUpperCase())}</span> <span>${esc(i.reason || "")}</span>`, { rich: true }),
        listSection("Category breakouts", breakouts, "No obvious category breakouts detected.",
          (i: Record<string, unknown>) => {
            const stats = [];
            if (i.distinct_count != null) stats.push(`${i.distinct_count} distinct`);
            if (i.null_rate != null) stats.push(`${i.null_rate}% null`);
            return `<strong>${esc(i.table)}.${esc(i.column)}</strong> <span>${esc(stats.join(" · "))}</span>`;
          }),
        ...(notes.length ? [listSection("Quick notes", notes, "", (i: string) => `<span>${esc(i)}</span>`, { note: true })] : []),
      ],
      actions,
    };
  }

  // ── Helpers ──

  function esc(v: unknown): string {
    return escapeHtml(String(v ?? ""));
  }

  function num(v: unknown): string {
    return Number(v || 0).toLocaleString();
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function listSection(title: string, items: any[], empty: string, render: (item: any) => string, opts?: { rich?: boolean; note?: boolean }): SectionDef {
    return { title, items, empty, render, rich: opts?.rich, note: opts?.note };
  }

  function handleAction(prompt: string) {
    sendMessage(prompt);
  }
</script>

<div
  class="border border-border bg-surface shadow-default min-w-0 overflow-x-auto mx-auto w-full mb-4 animate-fade-in"
  style="max-width: min(920px, 92%); padding: 18px 18px 16px; border-radius: var(--radius);"
>
  <!-- Header -->
  <div style="margin-bottom: 14px;">
    <span
      class="inline-flex font-bold uppercase"
      style="margin-bottom: 8px; color: var(--orange); font-size: 0.72rem;
             letter-spacing: 0.05em;"
    >
      Starter result
    </span>
    <h3 class="text-text-primary" style="margin: 0 0 6px; font-size: 1.15rem; line-height: 1.2; font-weight: 600;">
      {config.title}
    </h3>
    {#if config.description}
      <p class="text-text-secondary" style="margin: 0; font-size: 0.85rem;">
        {config.description}
      </p>
    {/if}
  </div>

  <!-- Metrics -->
  {#if config.metrics.length}
    <div
      class="grid"
      style="grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px;"
    >
      {#each config.metrics as metric}
        <div
          class="border border-border"
          style="padding: 12px 14px; border-radius: 12px;
                 background: color-mix(in srgb, var(--bg) 70%, var(--surface));"
        >
          <span
            class="block text-text-secondary"
            style="font-size: 0.76rem; margin-bottom: 4px; text-transform: uppercase;
                   letter-spacing: 0.04em;"
          >
            {metric.label}
          </span>
          <span class="text-text-primary" style="font-size: 1.1rem; font-weight: 600;">
            {metric.value}
          </span>
        </div>
      {/each}
    </div>
  {/if}

  <!-- Sections -->
  {#each config.sections as section}
    <div style="margin-top: 14px;">
      <div
        class="text-text-secondary font-bold uppercase"
        style="margin-bottom: 8px; font-size: 0.84rem; letter-spacing: 0.04em;"
      >
        {section.title}
      </div>
      {#if section.items.length === 0}
        <div
          class="flex items-baseline text-text-secondary border border-border"
          style="padding: 10px 12px; border-radius: 10px; font-size: 0.84rem;
                 background: color-mix(in srgb, var(--surface) 92%, var(--bg) 8%);"
        >
          {section.empty}
        </div>
      {:else}
        <div class="grid" style="gap: 8px;">
          {#each section.items as item}
            <div
              class="flex items-baseline border border-border text-text-primary
                [&_strong]:font-semibold [&_span]:text-text-secondary
                {section.rich ? 'flex-col items-start' : 'flex-wrap'}"
              style="gap: 8px 12px; padding: 10px 12px; border-radius: 10px;
                     background: {section.note
                       ? 'color-mix(in srgb, var(--orange) 7%, var(--surface))'
                       : 'color-mix(in srgb, var(--surface) 92%, var(--bg) 8%)'};
                     font-size: 0.84rem;"
            >
              {@html section.render(item)}
            </div>
          {/each}
        </div>
      {/if}
    </div>
  {/each}

  <!-- Follow-up actions -->
  {#if config.actions.length}
    <div class="flex flex-wrap" style="gap: 10px; margin-top: 18px;">
      {#each config.actions as action}
        <button
          class="cursor-pointer transition-all duration-150 hover:text-teal"
          style="border: 1px solid color-mix(in srgb, var(--teal) 35%, var(--border));
                 background: color-mix(in srgb, var(--surface) 82%, var(--bg));
                 color: var(--text); border-radius: 999px; padding: 8px 14px;
                 font-family: inherit; font-size: 0.76rem; font-weight: 500;"
          onmouseenter={(e) => {
            e.currentTarget.style.borderColor = 'var(--teal)';
            e.currentTarget.style.background = 'color-mix(in srgb, var(--teal) 8%, var(--surface))';
          }}
          onmouseleave={(e) => {
            e.currentTarget.style.borderColor = 'color-mix(in srgb, var(--teal) 35%, var(--border))';
            e.currentTarget.style.background = 'color-mix(in srgb, var(--surface) 82%, var(--bg))';
          }}
          onclick={() => handleAction(action.prompt)}
        >
          {action.label}
        </button>
      {/each}
    </div>
  {/if}
</div>
