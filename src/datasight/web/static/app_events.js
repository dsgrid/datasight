(function (global) {
  const dom = global.DatasightDom;

  function stopAndRun(handler) {
    return event => {
      event.preventDefault();
      event.stopPropagation();
      handler(event);
    };
  }

  function bindAppEvents() {
    if (document.body.dataset.appEventsBound === 'true') return;
    document.body.dataset.appEventsBound = 'true';

    dom.bindClick('#sidebar-toggle', () => toggleSidebar());
    dom.bindClick('.brand-lockup', () => toggleProjectsPanel());
    dom.bindClick('#projects-toggle', () => toggleProjectsPanel());
    dom.bindClick('#tab-chat', () => switchView('chat'));
    dom.bindClick('#tab-dashboard', () => switchView('dashboard'));
    dom.bindClick('#summarize-btn', () => summarizeDataset());
    dom.bindClick('#sql-panel-toggle', () => toggleRightPanel());
    dom.bindClick('#new-chat-btn', () => clearChat());
    dom.bindClick('#theme-toggle', () => toggleTheme());
    dom.bindClick('#export-toggle', () => toggleExportMode());
    dom.bindClick('#settings-toggle', () => toggleSettingsPanel());

    dom.bindChange('#landing-llm-provider', () => onLandingProviderChange());
    dom.bindClick('#landing-llm-connect-btn', () => connectLlmFromLanding());
    dom.bindClick('.landing-starter[data-starter-id]', event => {
      const starterId = event.currentTarget.getAttribute('data-starter-id');
      landingChooseStarter(starterId);
    });
    dom.bindEnter('#landing-explore-input', () => landingExplore());
    dom.bindClick('#landing-explore-btn', () => landingExplore());
    dom.bindEnter('#landing-project-input', () => landingOpenProject());
    dom.bindClick('#landing-open-btn', () => landingOpenProject());

    dom.bindInput('#schema-search-input', event => handleSchemaSearchInput(event.currentTarget.value));
    dom.bindEnter('#add-files-input', () => addFilesFromSidebar());
    dom.bindClick('#add-files-btn', () => addFilesFromSidebar());
    dom.bindClick('#queries-section .sidebar-header', () => toggleSidebarSection('queries-section'));
    dom.bindClick('#inspect-section .sidebar-header', () => toggleSidebarSection('inspect-section'));
    dom.bindClick('#recipes-section .sidebar-header', () => toggleSidebarSection('recipes-section'));
    dom.bindClick('#measures-editor-section .sidebar-header', () => toggleSidebarSection('measures-editor-section'));
    dom.bindClick('#bookmarks-section .sidebar-header', () => toggleSidebarSection('bookmarks-section'));
    dom.bindClick('#reports-section .sidebar-header', () => toggleSidebarSection('reports-section'));
    dom.bindClick('#conversations-section .sidebar-header', () => toggleSidebarSection('conversations-section'));
    dom.bindClick('.inspect-action-btn[data-kind][data-scope]', event => {
      const btn = event.currentTarget;
      runInspectAction(btn.getAttribute('data-kind'), btn.getAttribute('data-scope'));
    });
    dom.bindChange('#measure-builder-mode', () => updateMeasureBuilderMode());
    dom.bindChange('#measure-builder-table', event => populateWeightColumnOptions(event.currentTarget.value, ''));
    dom.bindChange('#measure-builder-select', () => applyMeasureBuilderSelection());
    dom.bindClick('#measure-insert-btn', () => insertMeasureOverride());
    dom.bindClick('#measure-validate-btn', () => validateMeasureOverrides());
    dom.bindClick('#measure-reload-btn', () => loadMeasureOverridesEditor());
    dom.bindClick('#measure-save-btn', stopAndRun(() => saveMeasureOverrides()));
    dom.bindClick('#measure-save-secondary-btn', () => saveMeasureOverrides());
    dom.bindClick('#bookmarks-clear-btn', stopAndRun(() => clearAllBookmarks()));
    dom.bindClick('#reports-clear-btn', stopAndRun(() => clearAllReports()));
    dom.bindClick('#conversations-clear-btn', stopAndRun(() => clearAllConversations()));
    dom.bindClick('#stop-btn', () => stopGeneration());
    document.getElementById('chat-form')?.addEventListener('submit', event => handleSubmit(event));

    dom.bindClick('.layout-btn[data-cols]', event => setDashboardColumns(parseInt(event.currentTarget.getAttribute('data-cols'), 10)));
    dom.bindClick('#dashboard-note-btn', () => addDashboardNote());
    dom.bindClick('#dashboard-section-btn', () => addDashboardSection());
    dom.bindClick('#dashboard-sync-btn', () => syncChartScales());
    dom.bindClick('#dashboard-export-btn', () => exportDashboard());

    dom.bindClick('#projects-close-btn', () => toggleProjectsPanel());
    dom.bindEnter('#explore-path-input', () => exploreFromInput());
    dom.bindClick('#explore-btn', () => exploreFromInput());
    dom.bindEnter('#project-path-input', () => openProjectFromInput());
    dom.bindClick('#open-project-btn', () => openProjectFromInput());
    dom.bindClick('#projects-overlay', () => toggleProjectsPanel());
    dom.bindClick('#command-palette-overlay', () => closeCommandPalette());
    dom.bindInput('#command-palette-input', () => updateCommandPaletteResults());
    dom.bindClick('#command-palette-close', () => closeCommandPalette());
    dom.bindClick('#save-popover-btn', () => saveFromPopover());
    dom.bindClick('#save-popover-overlay', () => hideSavePopover());

    dom.bindClick('#settings-close-btn', () => toggleSettingsPanel());
    dom.bindChange('#llm-provider', () => onLlmProviderChange());
    dom.bindClick('#llm-save-btn', () => saveLlmSettings());
    dom.bindChange('#setting-confirm-sql', () => toggleConfirmSql());
    dom.bindChange('#setting-explain-sql', () => toggleExplainSql());
    dom.bindChange('#setting-clarify-sql', () => toggleClarifySql());
    dom.bindChange('#setting-show-cost', () => toggleShowCost());
    dom.bindClick('#project-health-refresh-btn', () => loadProjectHealth());
    dom.bindClick('#settings-overlay', () => toggleSettingsPanel());

    document.addEventListener('click', event => {
      const projectRemoveButton = event.target.closest('.project-item-remove[data-path]');
      if (projectRemoveButton) {
        event.preventDefault();
        event.stopPropagation();
        removeRecentProject(projectRemoveButton.getAttribute('data-path'));
        return;
      }

      const projectItem = event.target.closest('.project-item[data-path]');
      if (projectItem) {
        loadProjectFromList(projectItem.getAttribute('data-path'));
        return;
      }

      const removeFileButton = event.target.closest('.remove-file[data-index]');
      if (removeFileButton) {
        removeExplorePath(parseInt(removeFileButton.getAttribute('data-index'), 10));
        return;
      }

      const exampleButton = event.target.closest('.example-btn[data-example]');
      if (exampleButton) {
        sendExample(exampleButton.getAttribute('data-example'));
        return;
      }

      const summarizeButton = event.target.closest('.example-btn[data-action=\"summarize-dataset\"], #example-summarize-btn');
      if (summarizeButton) {
        summarizeDataset();
        return;
      }

      const saveProjectButton = event.target.closest('.indicator-action[data-action=\"save-project\"]');
      if (saveProjectButton) {
        showSavePopover();
        return;
      }

      const paletteItem = event.target.closest('.command-palette-item[data-result-idx]');
      if (paletteItem) {
        executeCommandPaletteResult(parseInt(paletteItem.getAttribute('data-result-idx'), 10));
        return;
      }

      const schemaActionButton = event.target.closest('.schema-action-btn[data-action]');
      if (schemaActionButton) {
        event.preventDefault();
        event.stopPropagation();
        const action = schemaActionButton.getAttribute('data-action');
        const table = schemaActionButton.getAttribute('data-table');
        const column = schemaActionButton.getAttribute('data-column');
        if (action === 'column-stats') openColumnStatsInSidebar(table, column);
        if (action === 'ask-column') askAboutColumn(table, column);
        if (action === 'preview-table-sidebar') previewTableInSidebar(table);
        if (action === 'ask-table') askAboutTable(table);
        return;
      }

      const previewButton = event.target.closest('.preview-btn[data-table]');
      if (previewButton) {
        event.preventDefault();
        event.stopPropagation();
        previewTable(previewButton.getAttribute('data-table'), previewButton);
        return;
      }

      const tableHeader = event.target.closest('.table-header[data-idx][data-table]');
      if (tableHeader) {
        toggleTable(tableHeader);
        return;
      }

      const columnItem = event.target.closest('.column-item[data-table][data-column]');
      if (columnItem) {
        toggleColumnStats(
          columnItem,
          columnItem.getAttribute('data-table'),
          columnItem.getAttribute('data-column')
        );
        return;
      }

      const sqlConfirmButton = event.target.closest('.sql-confirm-btn[data-request-id][data-sql-action]');
      if (sqlConfirmButton) {
        respondSqlConfirm(
          sqlConfirmButton,
          sqlConfirmButton.getAttribute('data-request-id'),
          sqlConfirmButton.getAttribute('data-sql-action')
        );
        return;
      }

      const pageButton = event.target.closest('.page-btn[data-page]');
      if (pageButton && !pageButton.disabled) {
        goToPage(pageButton, parseInt(pageButton.getAttribute('data-page'), 10));
        return;
      }

      const conversationItem = event.target.closest('.conversation-item[data-session-id]');
      if (conversationItem) {
        loadConversation(conversationItem.getAttribute('data-session-id'));
        return;
      }

      const exportActionButton = event.target.closest('.export-bar-btn[data-export-action]');
      if (exportActionButton) {
        const action = exportActionButton.getAttribute('data-export-action');
        if (action === 'cancel') toggleExportMode();
        if (action === 'confirm') doExport();
        return;
      }

      const shortcutsCloseButton = event.target.closest('.shortcuts-close');
      if (shortcutsCloseButton) {
        hideShortcutsModal();
        return;
      }

      const landingRecentItem = event.target.closest('.landing-recent-item[data-path]');
      if (landingRecentItem) {
        landingOpenRecentProject(landingRecentItem.getAttribute('data-path'));
        return;
      }
    });
  }

  global.bindAppEvents = bindAppEvents;
})(window);
