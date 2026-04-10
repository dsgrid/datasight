// ---------------------------------------------------------------------------
// Interactive tables — sort & filter
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------
const PAGE_SIZE = 25;

function paginateTable(wrap) {
  const tbody = wrap.querySelector('tbody');
  if (!tbody) return;
  bindTableEvents(wrap.parentElement || wrap);
  wrap.dataset.page = '0';
  applyPage(wrap);
}

function applyPage(wrap) {
  const tbody = wrap.querySelector('tbody');
  if (!tbody) return;
  const page = parseInt(wrap.dataset.page || '0');
  const rows = Array.from(tbody.querySelectorAll('tr:not(.filtered-out)'));
  const totalRows = rows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
  const start = page * PAGE_SIZE;
  const end = start + PAGE_SIZE;

  // Hide/show all tbody rows based on pagination
  const allRows = tbody.querySelectorAll('tr');
  let visibleIdx = 0;
  allRows.forEach(row => {
    if (row.classList.contains('filtered-out')) {
      row.classList.add('paginated-out');
      return;
    }
    if (visibleIdx >= start && visibleIdx < end) {
      row.classList.remove('paginated-out');
    } else {
      row.classList.add('paginated-out');
    }
    visibleIdx++;
  });

  // Render pagination controls
  const container = wrap.querySelector('.table-pagination');
  if (!container) return;

  const totalDataRows = parseInt(wrap.dataset.totalRows || totalRows);
  const displayedRows = Math.min(totalDataRows, allRows.length);

  if (totalRows <= PAGE_SIZE) {
    // No pagination needed — just show row count
    container.innerHTML = '<span class="page-info">' + totalRows + ' row' + (totalRows !== 1 ? 's' : '') +
      (totalDataRows > displayedRows ? ' (showing ' + displayedRows + ' of ' + totalDataRows + ')' : '') + '</span>';
    return;
  }

  container.innerHTML =
    '<button class="page-btn"' + (page === 0 ? ' disabled' : '') +
      ' data-page="' + (page - 1) + '">Prev</button>' +
    '<span class="page-info">Page ' + (page + 1) + ' of ' + totalPages +
      ' (' + totalRows + ' rows)</span>' +
    '<button class="page-btn"' + (page >= totalPages - 1 ? ' disabled' : '') +
      ' data-page="' + (page + 1) + '">Next</button>';
}

function goToPage(btn, page) {
  const wrap = btn.closest('.result-table-wrap');
  if (!wrap) return;
  wrap.dataset.page = String(page);
  applyPage(wrap);
}

function sortTable(th) {
  const table = th.closest('table');
  const colIdx = parseInt(th.dataset.col);
  const tbody = table.querySelector('tbody');
  const rows = Array.from(tbody.querySelectorAll('tr'));

  // Determine sort direction
  const wasAsc = th.classList.contains('sort-asc');
  table.querySelectorAll('th').forEach(h => {
    h.classList.remove('sort-asc', 'sort-desc');
    h.querySelector('.sort-arrow').textContent = '';
  });

  const asc = !wasAsc;
  th.classList.add(asc ? 'sort-asc' : 'sort-desc');
  th.querySelector('.sort-arrow').textContent = asc ? '\u25B2' : '\u25BC';

  rows.sort((a, b) => {
    const aText = a.children[colIdx]?.textContent ?? '';
    const bText = b.children[colIdx]?.textContent ?? '';
    const aNum = parseFloat(aText.replace(/,/g, ''));
    const bNum = parseFloat(bText.replace(/,/g, ''));
    if (!isNaN(aNum) && !isNaN(bNum)) {
      return asc ? aNum - bNum : bNum - aNum;
    }
    return asc ? aText.localeCompare(bText) : bText.localeCompare(aText);
  });

  rows.forEach(r => tbody.appendChild(r));
  const wrap = th.closest('.result-table-wrap');
  if (wrap) { wrap.dataset.page = '0'; applyPage(wrap); }
}

function bindTableEvents(root) {
  root.querySelectorAll('.result-table-wrap').forEach(function(wrap) {
    var filter = wrap.querySelector('.table-filter');
    if (filter && !filter.dataset.bound) {
      filter.addEventListener('input', function() { filterTable(filter); });
      filter.dataset.bound = '1';
    }
    var csvBtn = wrap.querySelector('.export-csv-btn');
    if (csvBtn && !csvBtn.dataset.bound) {
      csvBtn.addEventListener('click', function() { exportTableCsv(csvBtn); });
      csvBtn.dataset.bound = '1';
    }
    wrap.querySelectorAll('th[data-col]').forEach(function(th) {
      if (!th.dataset.bound) {
        th.addEventListener('click', function() { sortTable(th); });
        th.dataset.bound = '1';
      }
    });
  });
}

function filterTable(input) {
  const wrap = input.closest('.result-table-wrap');
  const tbody = wrap.querySelector('tbody');
  if (!tbody) return;
  const term = input.value.toLowerCase();
  const rows = tbody.querySelectorAll('tr');

  rows.forEach(row => {
    const text = row.textContent.toLowerCase();
    const match = !term || text.includes(term);
    row.classList.toggle('filtered-out', !match);
  });

  // Reset to first page and re-paginate
  wrap.dataset.page = '0';
  applyPage(wrap);
}

function exportTableCsv(btn) {
  const wrap = btn.closest('.result-table-wrap');
  if (!wrap) return;
  const table = wrap.querySelector('.result-table');
  if (!table) return;

  function csvCell(text) {
    if (/[",\n\r]/.test(text)) {
      return '"' + text.replace(/"/g, '""') + '"';
    }
    return text;
  }

  const headers = Array.from(table.querySelectorAll('thead th'))
    .map(th => csvCell(th.textContent.trim()));
  const lines = [headers.join(',')];

  table.querySelectorAll('tbody tr').forEach(row => {
    if (row.classList.contains('filtered-out')) return;
    const cells = Array.from(row.querySelectorAll('td'))
      .map(td => csvCell(td.textContent.trim()));
    lines.push(cells.join(','));
  });

  const csv = lines.join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  const resultEl = wrap.closest('.tool-result');
  const title = (resultEl && resultEl.dataset.title) ? slugify(resultEl.dataset.title) : 'datasight-export';
  a.download = title + '.csv';
  a.click();
  URL.revokeObjectURL(url);

  btn.textContent = 'Downloaded!';
  setTimeout(() => { btn.textContent = 'Download CSV'; }, 1500);
}

