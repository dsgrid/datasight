<script lang="ts">
  import { tick } from "svelte";

  interface PlotlySpec {
    data?: unknown[];
    layout?: Record<string, unknown>;
  }

  export interface PlotlyPoint {
    curveNumber?: number;
    pointNumber?: number;
    x?: unknown;
    y?: unknown;
    label?: unknown;
    value?: unknown;
  }

  interface Props {
    plotlySpec?: unknown;
    html?: string;
    title?: string;
    className?: string;
    iframeClassName?: string;
    onPointClick?: (point: PlotlyPoint) => void | Promise<void>;
  }

  let {
    plotlySpec,
    html = "",
    title = "Chart",
    className = "w-full h-[480px] border border-border bg-surface",
    iframeClassName = className,
    onPointClick,
  }: Props = $props();

  let chartEl = $state<HTMLDivElement | null>(null);
  let iframeEl = $state<HTMLIFrameElement | null>(null);
  let renderError = $state("");
  let isRendering = $state(false);

  let spec = $derived(asPlotlySpec(plotlySpec));

  type PlotlyElement = HTMLDivElement & {
    on?: (event: string, handler: (evt: { points?: PlotlyPoint[] }) => void) => void;
    removeAllListeners?: (event: string) => void;
  };

  function asPlotlySpec(value: unknown): PlotlySpec | null {
    if (!value || typeof value !== "object" || Array.isArray(value)) return null;
    const candidate = value as PlotlySpec;
    return Array.isArray(candidate.data) ? candidate : null;
  }

  function cloneSpec(value: PlotlySpec): PlotlySpec {
    return JSON.parse(JSON.stringify(value)) as PlotlySpec;
  }

  function currentTheme() {
    return document.documentElement.getAttribute("data-theme") || "light";
  }

  function themedLayout(layout: Record<string, unknown>, theme: string) {
    const dark = theme === "dark";
    const font = { color: dark ? "#e6edf3" : "#1a1a1a" };
    const axis = dark
      ? { gridcolor: "#30363d", linecolor: "#30363d", zerolinecolor: "#30363d" }
      : { gridcolor: "#eee", linecolor: "#ddd", zerolinecolor: "#ddd" };
    return {
      ...layout,
      autosize: true,
      paper_bgcolor: dark ? "#161b22" : "white",
      plot_bgcolor: dark ? "#161b22" : "white",
      font: { ...(layout.font as Record<string, unknown> | undefined), ...font },
      xaxis: { ...(layout.xaxis as Record<string, unknown> | undefined), ...axis },
      yaxis: { ...(layout.yaxis as Record<string, unknown> | undefined), ...axis },
    };
  }

  function animationFrame(): Promise<void> {
    return new Promise((resolve) => requestAnimationFrame(() => resolve()));
  }

  async function waitForLayout(el: HTMLElement): Promise<void> {
    for (let attempt = 0; attempt < 20; attempt++) {
      await animationFrame();
      const rect = el.getBoundingClientRect();
      if (rect.width > 0 && rect.height > 0) return;
    }
  }

  function syncThemeToIframe() {
    if (!iframeEl?.contentWindow) return;
    try {
      iframeEl.contentWindow.postMessage(
        { type: "theme-change", theme: currentTheme() },
        "*",
      );
    } catch {
      // cross-origin safety
    }
  }

  $effect(() => {
    const iframe = iframeEl;
    if (!iframe || spec) return;
    const activeIframe = iframe;

    function handleMessage(event: MessageEvent) {
      const sourceWindow = activeIframe.contentWindow;
      if (!sourceWindow || event.source !== sourceWindow) return;
      if (event.data?.type !== "datasight-plotly-click") return;
      void onPointClick?.(event.data.point as PlotlyPoint);
    }

    window.addEventListener("message", handleMessage);
    return () => window.removeEventListener("message", handleMessage);
  });

  $effect(() => {
    const chart = chartEl;
    const activeSpec = spec;
    if (!chart || !activeSpec) return;
    const renderSpec: PlotlySpec = activeSpec;
    const plotEl = chart as PlotlyElement;

    let cancelled = false;
    let hasRendered = false;
    let resizeObserver: ResizeObserver | null = null;
    let themeObserver: MutationObserver | null = null;

    async function render() {
      const chartNode = chart;
      if (!chartNode) return;
      await tick();
      await waitForLayout(chartNode);
      if (cancelled) return;
      isRendering = true;
      try {
        const Plotly = (await import("plotly.js-dist-min")).default;
        if (cancelled) return;
        const cloned = cloneSpec(renderSpec);
        const layout = themedLayout(cloned.layout || {}, currentTheme());
        plotEl.removeAllListeners?.("plotly_click");
        await Plotly.react(plotEl, cloned.data || [], layout, {
          responsive: true,
          displayModeBar: true,
          modeBarButtonsToRemove: ["lasso2d", "select2d"],
          displaylogo: false,
        });
        if (cancelled) return;
        await animationFrame();
        if (!cancelled) Plotly.Plots?.resize(plotEl);
        plotEl.on?.("plotly_click", (evt: { points?: PlotlyPoint[] }) => {
          const point = evt?.points?.[0];
          if (point) void onPointClick?.(point);
        });
        hasRendered = true;
        renderError = "";
      } catch (error) {
        renderError = error instanceof Error ? error.message : String(error);
      } finally {
        if (!cancelled) isRendering = false;
      }
    }

    render();
    resizeObserver = new ResizeObserver(() => {
      if (!hasRendered) return;
      void import("plotly.js-dist-min").then(({ default: Plotly }) => {
        if (!cancelled) Plotly.Plots?.resize(plotEl);
      });
    });
    resizeObserver.observe(chart);
    themeObserver = new MutationObserver(() => void render());
    themeObserver.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme"],
    });

    return () => {
      cancelled = true;
      resizeObserver?.disconnect();
      themeObserver?.disconnect();
      plotEl.removeAllListeners?.("plotly_click");
      void import("plotly.js-dist-min").then(({ default: Plotly }) => {
        try {
          Plotly.purge(plotEl);
        } catch {
          // ignore cleanup failures
        }
      });
    };
  });
</script>

{#if spec}
  <div class="relative {className}">
    <div bind:this={chartEl} class="w-full h-full min-h-[300px]"></div>
    {#if isRendering}
      <div class="absolute inset-0 flex items-center justify-center bg-surface/80 p-4 text-center">
        <div class="text-xs text-text-secondary">Rendering chart...</div>
      </div>
    {/if}
    {#if renderError}
      <div class="absolute inset-0 flex items-center justify-center bg-surface/95 p-4 text-center">
        <div>
          <div class="text-xs font-medium text-red-600">Chart render failed</div>
          <div class="mt-1 text-[11px] text-text-secondary break-words">{renderError}</div>
        </div>
      </div>
    {/if}
  </div>
{:else if html}
  <iframe
    bind:this={iframeEl}
    sandbox="allow-scripts allow-same-origin allow-downloads"
    srcdoc={html}
    {title}
    onload={syncThemeToIframe}
    class={iframeClassName}
  ></iframe>
{:else}
  <div class="flex items-center justify-center {className}">
    <div class="text-xs text-text-secondary">Loading chart...</div>
  </div>
{/if}
