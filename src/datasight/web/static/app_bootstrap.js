(function () {
  async function initApp() {
    applyTheme(localStorage.getItem('datasight-theme') || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'));
    restoreCollapsedSections();
    bindSidebarListActions();
    bindAppEvents();

    try {
      const projectData = await fetchJson('/api/project');
      projectLoaded = projectData.loaded;
      currentProjectPath = projectData.path;
      updateInspectScopeLabel();

      if (projectLoaded) {
        hideLanding();
        if (projectData.is_ephemeral) {
          updateSessionIndicator('explore', projectData.tables?.length || '?');
        } else {
          updateSessionIndicator('project', projectData.name || projectData.path?.split('/').pop());
        }
        loadSchema();
        loadQueries();
        loadRecipes();
        await loadMeasureOverridesEditor();
        loadSettings();
        loadConversations();
        loadBookmarks();
        loadReports();
        loadDashboard();
        loadProjectHealth();
        restoreSession();
        return;
      }
    } catch (e) {
      console.error('Failed to check project status:', e);
      showToast('Failed to connect to server.', 'error');
    }

    showLanding();
    await initLanding();
    updateInspectScopeLabel();
    loadSettings();
  }

  window.initApp = initApp;
  initApp();
})();
