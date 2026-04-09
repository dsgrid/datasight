const test = require('node:test');
const assert = require('node:assert/strict');

const helpers = require('../src/datasight/web/static/search_helpers.js');

test('scoreFuzzySubsequence matches compact abbreviations', () => {
  const score = helpers.scoreFuzzySubsequence('orddt', 'order_date');
  assert.ok(score > 0);
});

test('scorePaletteResult prefers word-boundary and fuzzy matches', () => {
  const exact = helpers.scorePaletteResult('quality', ['Audit Nulls and Outliers', 'Quality'], 780);
  const fuzzy = helpers.scorePaletteResult('ord dt', ['orders order_date'], 560);
  const miss = helpers.scorePaletteResult('zzz', ['orders order_date'], 560);

  assert.ok(exact > 780);
  assert.ok(fuzzy > 560);
  assert.equal(miss, -1);
});

test('highlightMatch escapes html and wraps the matched text', () => {
  const html = helpers.highlightMatch('<orders>', 'ord');
  assert.equal(html, '&lt;<mark class="schema-match">ord</mark>ers&gt;');
});

test('getVisibleSchemaEntries keeps matching tables and matching columns', () => {
  const visible = helpers.getVisibleSchemaEntries(
    [
      {
        name: 'orders',
        columns: [
          { name: 'order_date', dtype: 'DATE' },
          { name: 'customer_state', dtype: 'TEXT' },
        ],
      },
      {
        name: 'products',
        columns: [{ name: 'category', dtype: 'TEXT' }],
      },
    ],
    'ord'
  );

  assert.equal(visible.length, 1);
  assert.equal(visible[0].table.name, 'orders');
  assert.equal(visible[0].tableMatches, true);
});

test('getVisibleSchemaEntries auto-expands tables on column-only matches', () => {
  const visible = helpers.getVisibleSchemaEntries(
    [
      {
        name: 'orders',
        columns: [
          { name: 'order_date', dtype: 'DATE' },
          { name: 'customer_state', dtype: 'TEXT' },
        ],
      },
    ],
    'state'
  );

  assert.equal(visible.length, 1);
  assert.equal(visible[0].table.name, 'orders');
  assert.equal(visible[0].tableMatches, false);
  assert.deepEqual(
    visible[0].matchingColumns.map(column => column.name),
    ['customer_state']
  );
});
