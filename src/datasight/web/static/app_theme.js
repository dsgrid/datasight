// ---------------------------------------------------------------------------
// Theme
// ---------------------------------------------------------------------------
function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  localStorage.setItem('datasight-theme', theme);

  // Swap highlight.js stylesheet
  const hljsLink = document.getElementById('hljs-theme');
  hljsLink.href = theme === 'dark'
    ? 'https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github-dark.min.css'
    : 'https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github.min.css';

  // Toggle icon visibility
  document.getElementById('theme-icon-sun').style.display = theme === 'dark' ? 'block' : 'none';
  document.getElementById('theme-icon-moon').style.display = theme === 'dark' ? 'none' : 'block';

  // Broadcast theme to chart iframes (chat + dashboard)
  document.querySelectorAll('.tool-result iframe, #dashboard-grid iframe').forEach(iframe => {
    try { iframe.contentWindow.postMessage({ type: 'theme-change', theme: theme }, '*'); } catch(e) {}
  });
}

function toggleTheme() {
  const current = document.documentElement.getAttribute('data-theme') || 'dark';
  applyTheme(current === 'dark' ? 'light' : 'dark');
}

// Auto-follow OS theme changes when user hasn't explicitly chosen
window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
  if (!localStorage.getItem('datasight-theme')) {
    applyTheme(e.matches ? 'dark' : 'light');
  }
});

