<script lang="ts">
  import { onMount } from "svelte";
  import Header from "$lib/components/Header.svelte";
  import LandingPage from "$lib/components/LandingPage.svelte";
  import SettingsPanel from "$lib/components/SettingsPanel.svelte";
  import ProjectsPanel from "$lib/components/ProjectsPanel.svelte";
  import Toast from "$lib/components/Toast.svelte";
  import ChatView from "$lib/components/ChatView.svelte";
  import DashboardView from "$lib/components/DashboardView.svelte";
  import Sidebar from "$lib/components/Sidebar.svelte";
  import MeasureEditorModal from "$lib/components/MeasureEditorModal.svelte";
  import CommandPalette from "$lib/components/CommandPalette.svelte";
  import ShortcutsModal from "$lib/components/ShortcutsModal.svelte";
  import QueryHistoryPanel from "$lib/components/QueryHistoryPanel.svelte";
  import ExportBar from "$lib/components/ExportBar.svelte";
  import { sessionStore } from "$lib/stores/session.svelte";
  import { settingsStore } from "$lib/stores/settings.svelte";
  import { schemaStore } from "$lib/stores/schema.svelte";
  import { dashboardStore } from "$lib/stores/dashboard.svelte";
  import { sidebarStore } from "$lib/stores/sidebar.svelte";
  import { chatStore } from "$lib/stores/chat.svelte";
  import { queriesStore } from "$lib/stores/queries.svelte";
  import { paletteStore } from "$lib/stores/palette.svelte";
  import { getProjectStatus } from "$lib/api/projects";
  import { loadSettings, loadLlmConfig } from "$lib/api/settings";
  import { loadSchema, loadQueries, loadRecipes } from "$lib/api/schema";
  import {
    loadBookmarks,
    loadReports,
    loadConversations,
  } from "$lib/api/saved";
  import { loadDashboard } from "$lib/api/dashboard";
  import { loadMeasureCatalog } from "$lib/api/measures";
  import { loadConversation } from "$lib/api/saved";
  import {
    loadDatasetOverview,
    loadMeasureOverview,
    loadDimensionOverview,
    loadQualityOverview,
    loadTrendOverview,
    loadTimeseriesOverview,
  } from "$lib/api/starters";

  let theme = $state(
    document.documentElement.getAttribute("data-theme") || "light",
  );
  let settingsOpen = $state(false);
  let projectsPanelOpen = $state(false);
  let measureEditorOpen = $state(false);
  let shortcutsOpen = $state(false);
  let sqlPanelOpen = $state(false);
  let exportMode = $state(false);
  let exportExcludeIndices = $state(new Set<number>());
  let booting = $state(true);
  let fromLanding = $state(false);

  function toggleTheme() {
    theme = theme === "light" ? "dark" : "light";
    document.documentElement.setAttribute("data-theme", theme);
    localStorage.setItem("datasight-theme", theme);
  }

  /** Load all data after a project is opened. */
  async function onProjectReady(path?: string) {
    sessionStore.projectLoaded = true;
    if (path) sessionStore.currentProjectPath = path;

    // Load everything in parallel
    await Promise.allSettled([
      loadSchema(),
      loadQueries(),
      loadRecipes(),
      loadBookmarks(),
      loadReports(),
      loadConversations(),
      loadDashboard(),
      loadMeasureCatalog(),
    ]);

    // Run pending starter if one was selected on landing page
    if (fromLanding) {
      await maybeRunPendingStarter();
      fromLanding = false;
    }
  }

  /** Execute a pending starter action (selected on landing page). */
  async function maybeRunPendingStarter() {
    const starter = sidebarStore.pendingStarterAction;
    if (!starter) return;

    const table = schemaStore.selectedTable || undefined;
    const loaders: Record<string, (t?: string) => Promise<{ overview: Record<string, unknown> }>> = {
      profile: loadDatasetOverview,
      measures: loadMeasureOverview,
      dimensions: loadDimensionOverview,
      quality: loadQualityOverview,
      trend: loadTrendOverview,
      timeseries: loadTimeseriesOverview,
    };

    const loader = loaders[starter];
    if (!loader) return;

    try {
      const result = await loader(table);
      if (result.overview) {
        chatStore.pushMessage({
          type: "starter_overview",
          kind: starter,
          overview: result.overview,
        });
      }
    } catch {
      // Starter failed silently
    }
  }

  /** Restore previous conversation from server if chat is empty. */
  async function maybeRestoreSession() {
    if (chatStore.messages.length > 0) return;

    try {
      const data = await loadConversation(sessionStore.sessionId);
      if (!data.events || (data.events as unknown[]).length === 0) return;

      for (const event of data.events as Array<Record<string, unknown>>) {
        const type = event.type as string;
        if (type === "user_message") {
          chatStore.pushMessage({
            type: "user_message",
            content: event.content as string,
          });
        } else if (type === "assistant_message") {
          chatStore.pushMessage({
            type: "assistant_message",
            content: event.content as string,
          });
        } else if (type === "tool_start") {
          chatStore.pushMessage({
            type: "tool_start",
            tool: event.tool as string,
            sql: (event.sql as string) || "",
          });
        } else if (type === "tool_result") {
          chatStore.pushMessage({
            type: "tool_result",
            html: event.html as string,
            title: (event.title as string) || "",
            resultType: (event.result_type as string) === "chart" ? "chart" : "table",
          });
        } else if (type === "tool_done") {
          chatStore.pushMessage({
            type: "tool_done",
            meta: (event.meta as { sql: string; tool: string }) || {
              sql: "",
              tool: "",
            },
          });
        } else if (type === "suggestions") {
          chatStore.pushMessage({
            type: "suggestions",
            suggestions: event.suggestions as string[],
          });
        }
      }
    } catch {
      // No saved conversation — that's fine
    }
  }

  /** Called when explore files succeeds. */
  async function onExplored() {
    fromLanding = true;
    sessionStore.projectLoaded = true;
    sessionStore.isEphemeralSession = true;

    await Promise.allSettled([loadSchema(), loadRecipes()]);

    // Run pending starter
    if (fromLanding) {
      await maybeRunPendingStarter();
      fromLanding = false;
    }
  }

  /** Called when a project is loaded from landing or projects panel. */
  async function onProjectLoaded(path: string) {
    fromLanding = true;
    sessionStore.isEphemeralSession = false;
    await onProjectReady(path);
  }

  onMount(async () => {
    // Bootstrap: check if a project is already loaded (server-side state)
    try {
      const [status] = await Promise.all([
        getProjectStatus(),
        loadSettings(),
        loadLlmConfig(),
      ]);

      if (status.loaded) {
        sessionStore.projectLoaded = true;
        sessionStore.currentProjectPath = status.path;
        sessionStore.isEphemeralSession = status.is_ephemeral;
        sessionStore.hasTimeSeries = Boolean(status.has_time_series);
        if (status.tables) {
          sessionStore.ephemeralTablesInfo = status.tables;
        }
        await onProjectReady(status.path ?? undefined);
        // Restore previous conversation on reload
        await maybeRestoreSession();
      }
    } catch {
      // Server not running — show landing page
    } finally {
      booting = false;
    }
  });

  function handleKeydown(e: KeyboardEvent) {
    const tag = (document.activeElement as HTMLElement)?.tagName;
    const isInput =
      tag === "INPUT" ||
      tag === "TEXTAREA" ||
      tag === "SELECT" ||
      (document.activeElement as HTMLElement)?.isContentEditable;

    // Mod shortcuts work even in inputs
    if (e.metaKey || e.ctrlKey) {
      if (e.key === ",") {
        e.preventDefault();
        settingsOpen = !settingsOpen;
      } else if (e.key === "k") {
        e.preventDefault();
        paletteStore.toggle();
      } else if (e.key === "b") {
        e.preventDefault();
        sidebarStore.toggleSidebar();
      }
      return;
    }

    // Escape always works
    if (e.key === "Escape") {
      if (dashboardStore.fullscreenCardId !== null) {
        dashboardStore.fullscreenCardId = null;
      } else if (shortcutsOpen) {
        shortcutsOpen = false;
      } else if (exportMode) {
        exportMode = false;
      } else if (document.activeElement instanceof HTMLElement) {
        document.activeElement.blur();
      }
      return;
    }

    // Remaining shortcuts only when not in an input
    if (isInput) return;

    if (e.key === "/") {
      e.preventDefault();
      // Focus chat input
      const input = document.querySelector<HTMLTextAreaElement>(
        "textarea[data-chat-input]",
      );
      input?.focus();
    } else if (e.key === "?") {
      e.preventDefault();
      shortcutsOpen = !shortcutsOpen;
    } else if (e.key === "n" || e.key === "N") {
      e.preventDefault();
      chatStore.clear();
    } else if (e.key === "d" || e.key === "D") {
      e.preventDefault();
      dashboardStore.currentView =
        dashboardStore.currentView === "chat" ? "dashboard" : "chat";
    } else if (
      dashboardStore.currentView === "dashboard" &&
      ["ArrowUp", "ArrowDown", "ArrowLeft", "ArrowRight"].includes(e.key)
    ) {
      e.preventDefault();
      const items = dashboardStore.pinnedItems;
      if (items.length === 0) return;
      const cols = dashboardStore.columns || Math.max(1, Math.floor(window.innerWidth / 500));
      let idx = dashboardStore.selectedCardIdx;
      if (idx < 0) idx = 0;
      else if (e.key === "ArrowRight") idx = Math.min(idx + 1, items.length - 1);
      else if (e.key === "ArrowLeft") idx = Math.max(idx - 1, 0);
      else if (e.key === "ArrowDown") idx = Math.min(idx + cols, items.length - 1);
      else if (e.key === "ArrowUp") idx = Math.max(idx - cols, 0);
      dashboardStore.selectedCardIdx = idx;
    } else if (
      dashboardStore.currentView === "dashboard" &&
      e.key === "Enter"
    ) {
      const idx = dashboardStore.selectedCardIdx;
      if (idx >= 0 && idx < dashboardStore.pinnedItems.length) {
        const item = dashboardStore.pinnedItems[idx];
        dashboardStore.fullscreenCardId =
          dashboardStore.fullscreenCardId === item.id ? null : item.id;
      }
    } else if (
      dashboardStore.currentView === "dashboard" &&
      (e.key === "Delete" || e.key === "Backspace")
    ) {
      const idx = dashboardStore.selectedCardIdx;
      if (idx >= 0 && idx < dashboardStore.pinnedItems.length) {
        dashboardStore.removeItem(dashboardStore.pinnedItems[idx].id);
      }
    }
  }
</script>

<svelte:window onkeydown={handleKeydown} />

<Header
  {theme}
  onToggleTheme={toggleTheme}
  onToggleSettings={() => (settingsOpen = !settingsOpen)}
  onToggleProjects={() => (projectsPanelOpen = !projectsPanelOpen)}
  onToggleSidebar={() => sidebarStore.toggleSidebar()}
  onToggleSqlPanel={() => (sqlPanelOpen = !sqlPanelOpen)}
  onToggleExport={() => (exportMode = !exportMode)}
  projectLoaded={sessionStore.projectLoaded}
  currentView={dashboardStore.currentView}
  onSwitchView={(v) => (dashboardStore.currentView = v)}
  isEphemeral={sessionStore.isEphemeralSession}
  projectName={sessionStore.currentProjectPath?.split("/").pop() ?? null}
  tableCount={sessionStore.ephemeralTablesInfo.length}
  {sqlPanelOpen}
/>

<main class="flex flex-1 overflow-hidden min-h-0"
  style="background: linear-gradient(180deg, color-mix(in srgb, var(--bg) 86%, var(--surface) 14%), var(--bg));">
  {#if booting}
    <div class="flex-1 flex items-center justify-center text-text-secondary">
      <div class="text-sm">Loading...</div>
    </div>
  {:else if sessionStore.projectLoaded}
    <Sidebar onOpenMeasureEditor={() => (measureEditorOpen = true)} />
    {#if dashboardStore.currentView === "chat"}
      <div class="flex flex-col flex-1 min-w-0 min-h-0 overflow-hidden">
        <ChatView />
        <ExportBar
          open={exportMode}
          excludeIndices={exportExcludeIndices}
          onCancel={() => (exportMode = false)}
          onExported={() => {
            exportMode = false;
            exportExcludeIndices = new Set();
          }}
        />
      </div>
    {:else}
      <DashboardView />
    {/if}
    <QueryHistoryPanel
      open={sqlPanelOpen}
      onClose={() => (sqlPanelOpen = false)}
    />
  {:else}
    <LandingPage {onProjectLoaded} {onExplored} />
  {/if}
</main>

<SettingsPanel open={settingsOpen} onClose={() => (settingsOpen = false)} />
<ProjectsPanel
  open={projectsPanelOpen}
  onClose={() => (projectsPanelOpen = false)}
  {onProjectLoaded}
/>
<MeasureEditorModal
  open={measureEditorOpen}
  onClose={() => (measureEditorOpen = false)}
/>
<CommandPalette
  onToggleSettings={() => (settingsOpen = !settingsOpen)}
  onToggleSidebar={() => sidebarStore.toggleSidebar()}
/>
<ShortcutsModal
  open={shortcutsOpen}
  onClose={() => (shortcutsOpen = false)}
/>
<Toast />
