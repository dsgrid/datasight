<script lang="ts">
  import StarterGrid from "./StarterGrid.svelte";
  import ExploreCard from "./ExploreCard.svelte";
  import ProjectCard from "./ProjectCard.svelte";
  import DiscoveredFilesCard from "./DiscoveredFilesCard.svelte";

  interface Props {
    onProjectLoaded: (path: string) => void;
    onExplored: () => void;
  }

  let { onProjectLoaded, onExplored }: Props = $props();

  let exploreError = $state("");

  function handleExploreError(msg: string) {
    exploreError = msg;
  }

  function handleProjectError(msg: string) {
    console.error("Project load error:", msg);
  }
</script>

<div
  class="landing-page flex-1 flex items-center justify-center overflow-y-auto min-w-0"
  style="padding: 40px 20px;
         background:
           radial-gradient(circle at top center, color-mix(in srgb, var(--teal) 10%, transparent), transparent 30%),
           radial-gradient(circle at bottom right, color-mix(in srgb, var(--orange) 8%, transparent), transparent 28%),
           var(--bg);"
>
  <div style="max-width: 980px; width: 100%;">
    <!-- Header -->
    <div class="text-center" style="margin-bottom: 28px;">
      <div
        class="inline-flex items-center text-cream font-semibold"
        style="padding: 6px 12px; margin-bottom: 16px; border-radius: 999px;
               background: color-mix(in srgb, var(--navy) 92%, transparent);
               font-size: 0.74rem; letter-spacing: 0.03em;
               box-shadow: 0 10px 24px rgba(2,61,96,0.12);"
      >
        Structured starters, reusable SQL workflows, and narrative dashboards
      </div>
      <div>
        <img
          src="/datasight-icon.svg"
          alt="datasight"
          class="mx-auto"
          style="width: 56px; height: 56px; border-radius: 16px;
                 box-shadow: 0 12px 32px rgba(2,61,96,0.16); margin-bottom: 16px;"
        />
      </div>
      <h1
        class="font-bold landing-h1"
        style="font-size: clamp(2.2rem, 5vw, 3.1rem); letter-spacing: -0.02em; margin-bottom: 6px;"
      >
        data<span class="text-teal">sight</span>
      </h1>
      <p class="text-text-secondary" style="font-size: 1rem;">
        AI-powered database exploration with natural language
      </p>
    </div>

    <!-- Auto-discovered files in CWD -->
    <DiscoveredFilesCard {onExplored} onError={handleExploreError} />

    <!-- Starter Grid -->
    <StarterGrid />

    <!-- Explore + Project cards -->
    <div class="grid grid-cols-1 md:grid-cols-2" style="gap: 18px; margin-bottom: 24px;">
      <ExploreCard {onExplored} onError={handleExploreError} />
      <ProjectCard {onProjectLoaded} onError={handleProjectError} />
    </div>

    <!-- Footer -->
    <div class="text-center" style="font-size: 0.8rem;">
      <a
        href="https://dsgrid.github.io/datasight/use/tutorials/getting-started/"
        target="_blank"
        rel="noopener"
        class="text-text-secondary no-underline hover:text-teal hover:underline"
      >
        Or try the demo dataset
      </a>
    </div>
  </div>
</div>
