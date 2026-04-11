<script lang="ts">
  import { sanitizeHtml } from "$lib/utils/markdown";
  import { onMount } from "svelte";

  interface Props {
    html: string;
    title?: string;
    onPin?: () => void;
    onBookmark?: () => void;
    onSaveReport?: () => void;
    onDelete?: () => void;
  }

  let { html, title, onPin, onBookmark, onSaveReport, onDelete }: Props =
    $props();

  let containerEl = $state<HTMLElement | null>(null);
  let bookmarked = $state(false);
  let reportSaved = $state(false);
  let currentPage = $state(0);

  const PAGE_SIZE = 25;

  let sanitized = $derived(sanitizeHtml(html));

  /** Initialize table interactivity after mount. */
  onMount(() => {
    if (containerEl) {
      bindTableSort(containerEl);
      paginateTable(containerEl);
    }
  });

  function bindTableSort(root: HTMLElement) {
    root.querySelectorAll<HTMLElement>("th[data-col]").forEach((th) => {
      th.style.cursor = "pointer";
      th.addEventListener("click", () => sortColumn(th));
    });
  }

  function sortColumn(th: HTMLElement) {
    const table = th.closest("table");
    if (!table) return;
    const colIdx = parseInt(th.dataset.col || "0");
    const tbody = table.querySelector("tbody");
    if (!tbody) return;

    const wasAsc = th.classList.contains("sort-asc");
    table.querySelectorAll("th").forEach((h) => {
      h.classList.remove("sort-asc", "sort-desc");
      const arrow = h.querySelector(".sort-arrow");
      if (arrow) arrow.textContent = "";
    });

    const asc = !wasAsc;
    th.classList.add(asc ? "sort-asc" : "sort-desc");
    const arrow = th.querySelector(".sort-arrow");
    if (arrow) arrow.textContent = asc ? "\u25B2" : "\u25BC";

    const rows = Array.from(tbody.querySelectorAll("tr"));
    rows.sort((a, b) => {
      const aText = a.children[colIdx]?.textContent ?? "";
      const bText = b.children[colIdx]?.textContent ?? "";
      const aNum = parseFloat(aText.replace(/,/g, ""));
      const bNum = parseFloat(bText.replace(/,/g, ""));
      if (!isNaN(aNum) && !isNaN(bNum)) {
        return asc ? aNum - bNum : bNum - aNum;
      }
      return asc ? aText.localeCompare(bText) : bText.localeCompare(aText);
    });
    rows.forEach((r) => tbody.appendChild(r));
    currentPage = 0;
    applyPagination();
  }

  function paginateTable(root: HTMLElement) {
    currentPage = 0;
    applyPagination();
  }

  function applyPagination() {
    if (!containerEl) return;
    const tbody = containerEl.querySelector("tbody");
    if (!tbody) return;

    const rows = Array.from(
      tbody.querySelectorAll<HTMLElement>("tr:not(.filtered-out)"),
    );
    const start = currentPage * PAGE_SIZE;
    const end = start + PAGE_SIZE;

    let visibleIdx = 0;
    tbody.querySelectorAll<HTMLElement>("tr").forEach((row) => {
      if (row.classList.contains("filtered-out")) {
        row.style.display = "none";
        return;
      }
      row.style.display =
        visibleIdx >= start && visibleIdx < end ? "" : "none";
      visibleIdx++;
    });
  }

  let totalRows = $derived.by(() => {
    if (!containerEl) return 0;
    const tbody = containerEl.querySelector("tbody");
    if (!tbody) return 0;
    return tbody.querySelectorAll("tr:not(.filtered-out)").length;
  });

  let totalPages = $derived(Math.max(1, Math.ceil(totalRows / PAGE_SIZE)));

  function prevPage() {
    if (currentPage > 0) {
      currentPage--;
      applyPagination();
    }
  }

  function nextPage() {
    if (currentPage < totalPages - 1) {
      currentPage++;
      applyPagination();
    }
  }

  function handleBookmark() {
    onBookmark?.();
    bookmarked = true;
    setTimeout(() => (bookmarked = false), 1200);
  }

  function handleSaveReport() {
    onSaveReport?.();
    reportSaved = true;
    setTimeout(() => (reportSaved = false), 1200);
  }

  function handleFilter(e: Event) {
    if (!containerEl) return;
    const input = e.target as HTMLInputElement;
    const term = input.value.toLowerCase();
    const tbody = containerEl.querySelector("tbody");
    if (!tbody) return;

    tbody.querySelectorAll("tr").forEach((row) => {
      const match = !term || row.textContent?.toLowerCase().includes(term);
      row.classList.toggle("filtered-out", !match);
    });
    currentPage = 0;
    applyPagination();
  }

  function exportCsv() {
    if (!containerEl) return;
    const table = containerEl.querySelector("table");
    if (!table) return;

    const headers = Array.from(table.querySelectorAll("thead th")).map(
      (th) => csvCell(th.textContent?.trim() || ""),
    );
    const lines = [headers.join(",")];

    table.querySelectorAll("tbody tr").forEach((row) => {
      if (row.classList.contains("filtered-out")) return;
      const cells = Array.from(row.querySelectorAll("td")).map((td) =>
        csvCell(td.textContent?.trim() || ""),
      );
      lines.push(cells.join(","));
    });

    const blob = new Blob([lines.join("\n")], {
      type: "text/csv;charset=utf-8;",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = (title || "datasight-export").replace(/[^a-z0-9]+/gi, "-") + ".csv";
    a.click();
    URL.revokeObjectURL(url);
  }

  function csvCell(text: string): string {
    if (/[",\n\r]/.test(text)) {
      return '"' + text.replace(/"/g, '""') + '"';
    }
    return text;
  }
</script>

<div class="tool-result group min-w-0 w-full animate-fade-in" style="position: relative; margin-bottom: 16px;">
  <!-- Floating action buttons (visible on hover) -->
  {#if onBookmark}
    <button
      class="tool-action-btn opacity-0 group-hover:opacity-100"
      style="right: 112px;"
      onclick={handleBookmark}
    >
      {bookmarked ? "Saved!" : "Bookmark"}
    </button>
  {/if}
  {#if onPin}
    <button
      class="tool-action-btn opacity-0 group-hover:opacity-100"
      style="right: 40px;"
      onclick={onPin}
    >
      Pin
    </button>
  {/if}
  {#if onDelete}
    <button
      class="tool-action-btn tool-action-btn-delete opacity-0 group-hover:opacity-100"
      style="right: 4px;"
      onclick={onDelete}
    >
      &times;
    </button>
  {/if}

  <!-- Table wrapper (toolbar + table in one bordered container) -->
  <div
    class="border border-border bg-surface overflow-hidden"
    style="border-radius: var(--radius); box-shadow: var(--shadow);"
  >
    <!-- Toolbar -->
    <div
      class="flex items-center border-b border-border bg-surface"
      style="gap: 8px; padding: 6px 12px;"
    >
      <input
        type="text"
        placeholder="Filter rows..."
        oninput={handleFilter}
        class="border border-border bg-bg text-text-primary
          placeholder:text-text-secondary
          focus:outline-none focus:border-teal focus:shadow-[0_0_0_1px_rgba(21,168,168,0.2)]"
        style="flex: 1; max-width: 280px; padding: 4px 8px; border-radius: 6px;
               font-family: inherit; font-size: 0.78rem;"
      />

      {#if totalRows > PAGE_SIZE}
        <button
          class="page-btn"
          disabled={currentPage === 0}
          onclick={prevPage}
        >
          Prev
        </button>
        <span style="font-size: 0.75rem; color: var(--text-secondary);">
          Page {currentPage + 1} of {totalPages}
          ({totalRows} rows)
        </span>
        <button
          class="page-btn"
          disabled={currentPage >= totalPages - 1}
          onclick={nextPage}
        >
          Next
        </button>
      {:else}
        <span style="font-size: 0.75rem; color: var(--text-secondary);">
          {totalRows} row{totalRows !== 1 ? "s" : ""}
        </span>
      {/if}

      <button
        class="export-csv-btn ml-auto"
        onclick={exportCsv}
      >
        Download CSV
      </button>
    </div>

    <!-- Table content -->
    <div
      bind:this={containerEl}
      class="result-table overflow-x-auto
        [&_table]:w-full [&_table]:border-collapse
        [&_th]:text-left
        [&_td]:border-b [&_td]:border-border"
      style="font-family: 'JetBrains Mono', ui-monospace, monospace; font-size: 0.82rem;"
    >
      {@html sanitized}
    </div>
  </div>
</div>
