// ---------------------------------------------------------------------------
// Sidebar resize
// ---------------------------------------------------------------------------
(function() {
  const handle = document.getElementById('sidebar-resize-handle');
  const sidebar = document.getElementById('sidebar');
  if (!handle || !sidebar) return;
  let dragging = false;

  handle.addEventListener('mousedown', function(e) {
    e.preventDefault();
    dragging = true;
    handle.classList.add('active');
    sidebar.style.transition = 'none';
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  });

  function onMouseMove(e) {
    if (!dragging) return;
    const newWidth = Math.max(200, Math.min(e.clientX, window.innerWidth * 0.6));
    document.documentElement.style.setProperty('--sidebar-width', newWidth + 'px');
  }

  function onMouseUp() {
    dragging = false;
    handle.classList.remove('active');
    sidebar.style.transition = '';
    document.removeEventListener('mousemove', onMouseMove);
    document.removeEventListener('mouseup', onMouseUp);
  }
})();

// ---------------------------------------------------------------------------
// Export mode
// ---------------------------------------------------------------------------
function toggleExportMode() {
  exportMode = !exportMode;
  const btn = document.getElementById('export-toggle');
  btn.classList.toggle('active', exportMode);

  if (exportMode) {
    exportExcludeIndices.clear();
    addExportCheckboxes();
    showExportBar();
  } else {
    removeExportCheckboxes();
    hideExportBar();
  }
}

function addExportCheckboxes() {
  let turnIdx = 0;
  // Group turns: user message + tools + assistant response(s) until next user
  // This matches the backend's turn-based exclusion semantics
  const children = Array.from(messagesEl.children);
  let i = 0;
  while (i < children.length) {
    const el = children[i];
    // Only process user message rows (turns are user-initiated)
    if (el.classList.contains('message-row') && el.classList.contains('user')) {
      const idx = turnIdx;
      // Collect user message and ALL following siblings until the next user message
      // This includes tools, assistant responses, suggestions, etc.
      const block = [el];
      let j = i + 1;
      while (j < children.length) {
        const child = children[j];
        // Stop at the next user message (start of next turn)
        if (child.classList.contains('message-row') && child.classList.contains('user')) {
          break;
        }
        block.push(child);
        j++;
      }

      const btn = document.createElement('button');
      btn.className = 'export-trash-btn';
      btn.dataset.msgIdx = idx;
      btn.title = 'Exclude from export';
      btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M2 4h12M5.33 4V2.67a1.33 1.33 0 0 1 1.34-1.34h2.66a1.33 1.33 0 0 1 1.34 1.34V4M12.67 4v9.33a1.33 1.33 0 0 1-1.34 1.34H4.67a1.33 1.33 0 0 1-1.34-1.34V4"/></svg>';
      btn.onclick = function(e) {
        e.stopPropagation();
        const isExcluded = exportExcludeIndices.has(idx);
        if (isExcluded) {
          exportExcludeIndices.delete(idx);
          block.forEach(b => b.classList.remove('export-excluded'));
          btn.classList.remove('active');
          btn.title = 'Exclude from export';
        } else {
          exportExcludeIndices.add(idx);
          block.forEach(b => b.classList.add('export-excluded'));
          btn.classList.add('active');
          btn.title = 'Restore to export';
        }
      };
      el.appendChild(btn);
      turnIdx++;
      i = j;
    } else {
      i++;
    }
  }
}

function removeExportCheckboxes() {
  messagesEl.querySelectorAll('.export-trash-btn').forEach(el => el.remove());
  messagesEl.querySelectorAll('.export-excluded').forEach(el => el.classList.remove('export-excluded'));
}

function showExportBar() {
  if (document.getElementById('export-bar')) return;
  const bar = document.createElement('div');
  bar.id = 'export-bar';
  bar.innerHTML =
    '<span>Select messages to include in export</span>' +
    '<div>' +
      '<button class="export-bar-btn cancel" data-export-action="cancel">Cancel</button>' +
      '<button class="export-bar-btn confirm" data-export-action="confirm">Export HTML</button>' +
    '</div>';
  document.querySelector('.chat-area').appendChild(bar);
}

function hideExportBar() {
  const bar = document.getElementById('export-bar');
  if (bar) bar.remove();
}

async function doExport() {
  try {
    const resp = await fetch('/api/export/' + sessionId, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ exclude_indices: Array.from(exportExcludeIndices) }),
    });
    if (!resp.ok) throw new Error('Export failed');
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'datasight-export.html';
    a.click();
    URL.revokeObjectURL(url);
    toggleExportMode();
  } catch (e) {
    console.error('Export failed:', e);
    showToast('Export failed. Please try again.', 'error');
  }
}

// ---------------------------------------------------------------------------
// Keyboard shortcuts
// ---------------------------------------------------------------------------
function showShortcutsModal() {
  if (shortcutsModalOpen) { hideShortcutsModal(); return; }
  shortcutsModalOpen = true;
  const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0;
  const mod = isMac ? '&#8984;' : 'Ctrl';
  const overlay = document.createElement('div');
  overlay.id = 'shortcuts-modal-overlay';
  overlay.onclick = (e) => { if (e.target === overlay) hideShortcutsModal(); };
  overlay.innerHTML =
    '<div class="shortcuts-modal">' +
      '<div class="shortcuts-modal-header">' +
        '<span>Keyboard Shortcuts</span>' +
        '<button class="shortcuts-close">&times;</button>' +
      '</div>' +
      '<div class="shortcuts-list">' +
        '<div class="shortcut-row"><kbd>/</kbd><span>Focus question input</span></div>' +
        '<div class="shortcut-row"><kbd>' + mod + '</kbd>+<kbd>K</kbd><span>Open command palette</span></div>' +
        '<div class="shortcut-row"><kbd>' + mod + '</kbd>+<kbd>B</kbd><span>Toggle sidebar</span></div>' +
        '<div class="shortcut-row"><kbd>N</kbd><span>New conversation</span></div>' +
        '<div class="shortcut-row"><kbd>D</kbd><span>Toggle dashboard view</span></div>' +
        '<div class="shortcut-row"><kbd>&#8592;&#8593;&#8594;&#8595;</kbd><span>Navigate dashboard cards</span></div>' +
        '<div class="shortcut-row"><kbd>Enter</kbd><span>Fullscreen selected card</span></div>' +
        '<div class="shortcut-row"><kbd>Escape</kbd><span>Exit fullscreen / close modal</span></div>' +
        '<div class="shortcut-row"><kbd>?</kbd><span>Show this help</span></div>' +
      '</div>' +
    '</div>';
  document.body.appendChild(overlay);
}

function hideShortcutsModal() {
  shortcutsModalOpen = false;
  const overlay = document.getElementById('shortcuts-modal-overlay');
  if (overlay) overlay.remove();
}

document.addEventListener('keydown', function(e) {
  const tag = document.activeElement.tagName;
  const isInput = tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || document.activeElement.isContentEditable;
  const mod = e.metaKey || e.ctrlKey;

  if (commandPaletteOpen) {
    if (e.key === 'Escape') {
      e.preventDefault();
      closeCommandPalette();
      return;
    }
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (commandPaletteResults.length > 0) {
        commandPaletteSelectedIdx = Math.min(commandPaletteSelectedIdx + 1, commandPaletteResults.length - 1);
        renderCommandPalette();
      }
      return;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      if (commandPaletteResults.length > 0) {
        commandPaletteSelectedIdx = Math.max(commandPaletteSelectedIdx - 1, 0);
        renderCommandPalette();
      }
      return;
    }
    if (e.key === 'Enter') {
      e.preventDefault();
      executeCommandPaletteResult(commandPaletteSelectedIdx);
      return;
    }
  }

  // Escape: exit fullscreen, close modals, or blur input
  if (e.key === 'Escape') {
    if (fullscreenCardId !== null) { exitCardFullscreen(); return; }
    if (shortcutsModalOpen) { hideShortcutsModal(); return; }
    if (isInput) { document.activeElement.blur(); return; }
    return;
  }

  // Mod+K: open command palette
  if (mod && e.key === 'k') {
    e.preventDefault();
    if (commandPaletteOpen) closeCommandPalette();
    else openCommandPalette();
    return;
  }

  // Mod+B: toggle sidebar
  if (mod && e.key === 'b' && !e.shiftKey) {
    e.preventDefault();
    toggleSidebar();
    return;
  }

  // Shortcuts below only apply when not typing in an input
  if (isInput) return;

  // N: new conversation
  if (e.key === 'n' && !mod && !e.shiftKey && !e.altKey) {
    clearChat();
    return;
  }

  // / : focus input
  if (e.key === '/') {
    e.preventDefault();
    inputEl.focus();
    return;
  }

  // ? : show shortcuts help
  if (e.key === '?') {
    e.preventDefault();
    showShortcutsModal();
    return;
  }

  // D: toggle dashboard view
  if (e.key === 'd' && !mod && !e.shiftKey && !e.altKey) {
    e.preventDefault();
    switchView(currentView === 'dashboard' ? 'chat' : 'dashboard');
    return;
  }

  // Arrow keys: navigate dashboard cards (only when in dashboard view)
  if (currentView === 'dashboard' && pinnedItems.length > 0) {
    const cols = dashboardColumns > 0 ? dashboardColumns : Math.floor(document.getElementById('dashboard-grid').offsetWidth / 466) || 1;
    if (e.key === 'ArrowRight') {
      e.preventDefault();
      selectedCardIdx = Math.min(selectedCardIdx + 1, pinnedItems.length - 1);
      if (selectedCardIdx < 0) selectedCardIdx = 0;
      renderDashboard();
      return;
    }
    if (e.key === 'ArrowLeft') {
      e.preventDefault();
      selectedCardIdx = Math.max(selectedCardIdx - 1, 0);
      renderDashboard();
      return;
    }
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      const newIdx = selectedCardIdx + cols;
      if (newIdx < pinnedItems.length) selectedCardIdx = newIdx;
      else if (selectedCardIdx < 0) selectedCardIdx = 0;
      renderDashboard();
      return;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      const newIdx = selectedCardIdx - cols;
      if (newIdx >= 0) selectedCardIdx = newIdx;
      renderDashboard();
      return;
    }
    // Enter: toggle fullscreen on selected card
    if (e.key === 'Enter' && selectedCardIdx >= 0 && selectedCardIdx < pinnedItems.length) {
      e.preventDefault();
      toggleCardFullscreen(pinnedItems[selectedCardIdx].id);
      return;
    }
    // Delete/Backspace: unpin selected card
    if ((e.key === 'Delete' || e.key === 'Backspace') && selectedCardIdx >= 0 && selectedCardIdx < pinnedItems.length) {
      e.preventDefault();
      unpinItem(pinnedItems[selectedCardIdx].id);
      if (selectedCardIdx >= pinnedItems.length) selectedCardIdx = pinnedItems.length - 1;
      return;
    }
  }
});

