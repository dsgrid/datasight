<script lang="ts">
  import { sidebarStore } from "$lib/stores/sidebar.svelte";
  import { sessionStore } from "$lib/stores/session.svelte";
  import { schemaStore } from "$lib/stores/schema.svelte";
  import { queriesStore } from "$lib/stores/queries.svelte";
  import SidebarSection from "./SidebarSection.svelte";
  import SchemaInspector from "./SchemaInspector.svelte";
  import QueriesList from "./QueriesList.svelte";
  import RecipesList from "./RecipesList.svelte";
  import BookmarksList from "./BookmarksList.svelte";
  import ReportsList from "./ReportsList.svelte";
  import ConversationsList from "./ConversationsList.svelte";
  import MeasureEditor from "./MeasureEditor.svelte";
  import AddFilesInput from "./AddFilesInput.svelte";

  interface Props {
    onOpenMeasureEditor: () => void;
  }

  let { onOpenMeasureEditor }: Props = $props();

  let sidebarWidth = $state(300);
  let dragging = $state(false);

  function startResize(e: MouseEvent) {
    e.preventDefault();
    dragging = true;

    function onMove(ev: MouseEvent) {
      sidebarWidth = Math.max(200, Math.min(ev.clientX, window.innerWidth * 0.6));
    }

    function onUp() {
      dragging = false;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    }

    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }
</script>

<aside
  class="relative flex-shrink-0 border-r border-border
    flex flex-col overflow-hidden transition-[margin] duration-250 ease-in-out
    {dragging ? '!transition-none' : ''}"
  style="width: {sidebarWidth}px;
    margin-left: {sidebarStore.sidebarOpen ? 0 : -sidebarWidth}px;
    background: linear-gradient(180deg, color-mix(in srgb, var(--surface) 92%, var(--bg) 8%), var(--surface));"
>
  <!-- Content -->
  <div class="flex-1 overflow-y-auto overflow-x-hidden min-w-0 sidebar-scroll" style="padding: 8px 0;">
    {#if sessionStore.isEphemeralSession}
      <div style="padding: 8px 12px; border-bottom: 1px solid var(--border);">
        <AddFilesInput />
      </div>
    {/if}

    <SidebarSection
      title="Tables"
      id="inspect-section"
      count={schemaStore.schemaData.length}
    >
      <SchemaInspector />
    </SidebarSection>

    <SidebarSection
      title="Queries"
      id="queries-section"
      count={queriesStore.sessionQueries.length}
    >
      <QueriesList />
    </SidebarSection>

    <SidebarSection
      title="Recipes"
      id="recipes-section"
      count={schemaStore.recipesCache.length}
    >
      <RecipesList />
    </SidebarSection>

    <SidebarSection
      title="Measures"
      id="measures-editor-section"
      count={sidebarStore.measureEditorCatalog.length}
    >
      <MeasureEditor onOpenModal={onOpenMeasureEditor} />
    </SidebarSection>

    <SidebarSection
      title="Bookmarks"
      id="bookmarks-section"
      count={sidebarStore.bookmarksCache.length}
    >
      <BookmarksList />
    </SidebarSection>

    <SidebarSection
      title="Reports"
      id="reports-section"
      count={sidebarStore.reportsCache.length}
    >
      <ReportsList />
    </SidebarSection>

    <SidebarSection
      title="Conversations"
      id="conversations-section"
      count={sidebarStore.conversationsCache.length}
    >
      <ConversationsList />
    </SidebarSection>
  </div>

  <!-- Resize handle -->
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div
    class="absolute top-0 right-0 w-1 h-full cursor-col-resize
      hover:bg-teal transition-colors duration-150 z-10
      {dragging ? 'bg-teal' : ''}"
    onmousedown={startResize}
  ></div>
</aside>
