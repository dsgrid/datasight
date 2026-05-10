<script lang="ts">
  import type { TidyProposal } from "$lib/api/tidy";

  interface Props {
    proposal: TidyProposal;
    /** Cap row count so the SVG stays compact on big reshapes. */
    maxRows?: number;
  }

  let { proposal, maxRows = 6 }: Props = $props();

  // Layout constants. Tiles stack the column name (top) above the
  // dimension-value sub-label (bottom) so long names don't collide with
  // the sub the way they did when both shared a single line.
  const TILE_W = 160;
  const TILE_H = 24;
  const ROW_H = 30;
  const PAD_TOP = 26;
  const COL_LEFT_X = 12;
  const W = COL_LEFT_X * 2 + TILE_W * 2 + 80; // tiles + 80px connector gutter
  const COL_RIGHT_X = W - COL_LEFT_X;
  const LABEL_DX = 6;

  let columnMappings = $derived(proposal.column_mappings ?? []);
  let dimensions = $derived(proposal.dimensions ?? []);
  let idColumns = $derived(proposal.id_columns ?? []);
  let truncated = $derived(columnMappings.length > maxRows);
  let visibleMappings = $derived(
    truncated ? columnMappings.slice(0, maxRows) : columnMappings,
  );

  // Right-hand long-form columns: id columns first, then dimensions, then
  // the value column. Drawing them as a single label string keeps the
  // diagram compact while still naming the resulting schema.
  let longColumns = $derived([
    ...idColumns.map((c) => ({ name: c, kind: "id" })),
    ...dimensions.map((d) => ({ name: d.name, kind: "dim" })),
    { name: proposal.value_column, kind: "value" },
  ]);

  let leftRows = $derived([
    ...idColumns.map((c) => ({ kind: "id" as const, label: c, sub: "" })),
    ...visibleMappings.map((m) => ({
      kind: "measure" as const,
      label: m.column,
      sub: dimensions.map((d) => m.dimension_values[d.name]).join(" / "),
    })),
  ]);

  let rightRows = $derived(longColumns);

  let height = $derived(
    PAD_TOP + Math.max(leftRows.length, rightRows.length) * ROW_H + 18,
  );

  function leftY(index: number): number {
    return PAD_TOP + index * ROW_H;
  }

  function rightY(index: number): number {
    return PAD_TOP + index * ROW_H;
  }

  function valueRowIndex(): number {
    return rightRows.length - 1;
  }

  // Curved connector from a left source column to the long-form value
  // column. Uses a cubic Bezier so the curve scales with the vertical
  // delta — flat for adjacent rows, more pronounced when the source row
  // is far from the value row.
  function curvePath(srcY: number, dstY: number): string {
    const x1 = COL_LEFT_X + TILE_W;
    const x2 = COL_RIGHT_X - TILE_W;
    const cx1 = (x1 + x2) / 2;
    return `M ${x1} ${srcY} C ${cx1} ${srcY}, ${cx1} ${dstY}, ${x2} ${dstY}`;
  }

  function straightPath(srcY: number, dstY: number): string {
    const x1 = COL_LEFT_X + TILE_W;
    const x2 = COL_RIGHT_X - TILE_W;
    return `M ${x1} ${srcY} L ${x2} ${dstY}`;
  }

  // Connector anchor sits at the visual middle of the row (between
  // stacked label and sub).
  function rowAnchorY(row_y: number): number {
    return row_y + 1;
  }
</script>

<svg
  class="melt-diagram"
  viewBox="0 0 {W} {height}"
  preserveAspectRatio="xMidYMid meet"
  role="img"
  aria-label="Wide-to-long reshape diagram for {proposal.table}"
>
  <!-- Column headers -->
  <text x={COL_LEFT_X} y={12} class="hdr">{proposal.table} (wide)</text>
  <text x={COL_RIGHT_X} y={12} text-anchor="end" class="hdr">
    {proposal.target_object_name} (long)
  </text>

  <!-- Connector lines: id columns straight across, measures curve into the value column -->
  {#each leftRows as row, i (row.label + i)}
    {#if row.kind === "id"}
      <path
        d={straightPath(rowAnchorY(leftY(i)), rowAnchorY(rightY(i)))}
        class="conn conn-id"
        fill="none"
      />
    {:else}
      <path
        d={curvePath(
          rowAnchorY(leftY(i)),
          rowAnchorY(rightY(valueRowIndex())),
        )}
        class="conn conn-measure"
        fill="none"
      />
    {/if}
  {/each}

  <!-- Left column tiles. Label sits on the top half of the tile, sub on
       the bottom half so long names don't fight the dimension-value sub
       for horizontal space. -->
  {#each leftRows as row, i (row.label + i)}
    <g transform="translate({COL_LEFT_X} {leftY(i)})">
      <title>{row.label}{row.sub ? ` → ${row.sub}` : ""}</title>
      <rect
        x={0}
        y={-(TILE_H / 2) + 2}
        width={TILE_W}
        height={TILE_H}
        rx={5}
        class={row.kind === "id" ? "tile tile-id" : "tile tile-measure"}
      />
      {#if row.sub}
        <text x={LABEL_DX} y={-2} class="lbl">{row.label}</text>
        <text x={LABEL_DX} y={10} class="sub">{row.sub}</text>
      {:else}
        <text x={LABEL_DX} y={4} class="lbl">{row.label}</text>
      {/if}
    </g>
  {/each}

  {#if truncated}
    <text
      x={COL_LEFT_X + LABEL_DX}
      y={leftY(visibleMappings.length + idColumns.length) + 4}
      class="more"
    >
      … and {columnMappings.length - visibleMappings.length} more
    </text>
  {/if}

  <!-- Right column tiles. Same stacked layout — kind ("dim", "value")
       sits below the column name. -->
  {#each rightRows as row, i (row.name)}
    <g transform="translate({COL_RIGHT_X - TILE_W} {rightY(i)})">
      <title>{row.name} ({row.kind})</title>
      <rect
        x={0}
        y={-(TILE_H / 2) + 2}
        width={TILE_W}
        height={TILE_H}
        rx={5}
        class={
          row.kind === "id"
            ? "tile tile-id"
            : row.kind === "dim"
              ? "tile tile-dim"
              : "tile tile-value"
        }
      />
      {#if row.kind === "id"}
        <text x={LABEL_DX} y={4} class="lbl">{row.name}</text>
      {:else}
        <text x={LABEL_DX} y={-2} class="lbl">{row.name}</text>
        <text x={LABEL_DX} y={10} class="sub">{row.kind}</text>
      {/if}
    </g>
  {/each}
</svg>

<style>
  .melt-diagram {
    display: block;
    width: 100%;
    max-width: 520px;
    height: auto;
    font-family: inherit;
  }

  .hdr {
    font-size: 9px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    fill: var(--text-secondary);
  }

  .lbl {
    font-family: "JetBrains Mono", monospace;
    font-size: 9.5px;
    fill: var(--text);
    dominant-baseline: middle;
  }

  .sub {
    font-family: "JetBrains Mono", monospace;
    font-size: 8px;
    fill: var(--text-secondary);
    dominant-baseline: middle;
  }

  .more {
    font-size: 9px;
    font-style: italic;
    fill: var(--text-secondary);
    dominant-baseline: middle;
  }

  .tile {
    stroke: var(--border);
    stroke-width: 0.6;
  }
  .tile-id {
    fill: color-mix(in srgb, var(--text-secondary) 8%, var(--surface));
  }
  .tile-measure {
    fill: color-mix(in srgb, var(--orange) 10%, var(--surface));
    stroke: color-mix(in srgb, var(--orange) 30%, var(--border));
  }
  .tile-dim {
    fill: color-mix(in srgb, var(--teal) 12%, var(--surface));
    stroke: color-mix(in srgb, var(--teal) 36%, var(--border));
  }
  .tile-value {
    fill: color-mix(in srgb, var(--orange) 18%, var(--surface));
    stroke: color-mix(in srgb, var(--orange) 44%, var(--border));
  }

  .conn {
    stroke-width: 1;
    opacity: 0.7;
  }
  .conn-id {
    stroke: color-mix(in srgb, var(--text-secondary) 50%, transparent);
    stroke-dasharray: 3 2;
  }
  .conn-measure {
    stroke: color-mix(in srgb, var(--orange) 60%, transparent);
  }
</style>
