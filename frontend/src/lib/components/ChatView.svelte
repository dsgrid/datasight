<script lang="ts">
  import MessageList from "./MessageList.svelte";
  import ChatInput from "./ChatInput.svelte";

  interface Props {
    exportMode?: boolean;
    excludeIndices?: Set<number>;
    onToggleExclude?: (idx: number) => void;
  }

  let {
    exportMode = false,
    excludeIndices = new Set<number>(),
    onToggleExclude,
  }: Props = $props();
</script>

<div class="flex flex-col flex-1 min-w-0 min-h-0 overflow-hidden"
  style="background: linear-gradient(180deg, color-mix(in srgb, var(--bg) 90%, var(--surface) 10%), color-mix(in srgb, var(--bg) 96%, var(--surface) 4%));">
  <MessageList {exportMode} {excludeIndices} {onToggleExclude} />
  <ChatInput />
</div>
