(function (global) {
  function escapeHtml(text) {
    return String(text || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function scoreFuzzySubsequence(query, value) {
    const q = String(query || '').toLowerCase().replace(/\s+/g, '');
    const lower = String(value || '').toLowerCase();
    if (!q || !lower) return -1;

    let qIdx = 0;
    let firstMatch = -1;
    let lastMatch = -1;
    let contiguousBonus = 0;

    for (let i = 0; i < lower.length && qIdx < q.length; i += 1) {
      if (lower[i] === q[qIdx]) {
        if (firstMatch === -1) firstMatch = i;
        if (lastMatch === i - 1) contiguousBonus += 2;
        lastMatch = i;
        qIdx += 1;
      }
    }

    if (qIdx !== q.length) return -1;

    const span = Math.max((lastMatch - firstMatch) + 1, q.length);
    return Math.max(25, 55 - (span - q.length) - firstMatch + contiguousBonus);
  }

  function scorePaletteResult(query, haystacks, baseScore) {
    if (!query) return baseScore;
    const q = String(query).toLowerCase();
    let best = -1;
    (haystacks || []).forEach(value => {
      const lower = String(value || '').toLowerCase();
      if (lower === q) best = Math.max(best, 120);
      else if (lower.startsWith(q)) best = Math.max(best, 90);
      else if (lower.split(/[\s._-]+/).some(part => part.startsWith(q))) best = Math.max(best, 82);
      else if (lower.includes(q)) best = Math.max(best, 60);
      else best = Math.max(best, scoreFuzzySubsequence(q, lower));
    });
    return best >= 0 ? baseScore + best : -1;
  }

  function highlightMatch(text, query) {
    if (!query) return escapeHtml(text);

    const source = String(text || '');
    const lowerText = source.toLowerCase();
    const normalizedQuery = String(query || '').toLowerCase();
    const idx = lowerText.indexOf(normalizedQuery);
    if (idx === -1) return escapeHtml(source);

    const before = escapeHtml(source.slice(0, idx));
    const match = escapeHtml(source.slice(idx, idx + normalizedQuery.length));
    const after = escapeHtml(source.slice(idx + normalizedQuery.length));
    return before + '<mark class="schema-match">' + match + '</mark>' + after;
  }

  function getVisibleSchemaEntries(tables, query) {
    const normalizedQuery = String(query || '').trim().toLowerCase();
    if (!normalizedQuery) {
      return (tables || []).map(table => ({ table, tableMatches: false, matchingColumns: [] }));
    }

    return (tables || [])
      .map(table => {
        const tableMatches = String(table.name || '').toLowerCase().includes(normalizedQuery);
        const matchingColumns = (table.columns || []).filter(column =>
          String(column.name || '').toLowerCase().includes(normalizedQuery)
        );
        if (!tableMatches && matchingColumns.length === 0) return null;
        return { table, tableMatches, matchingColumns };
      })
      .filter(Boolean);
  }

  const helpers = {
    escapeHtml,
    scoreFuzzySubsequence,
    scorePaletteResult,
    highlightMatch,
    getVisibleSchemaEntries,
  };

  global.DatasightSearchHelpers = helpers;
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = helpers;
  }
})(typeof globalThis !== 'undefined' ? globalThis : this);
