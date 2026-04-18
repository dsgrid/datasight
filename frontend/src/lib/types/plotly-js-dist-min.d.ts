declare module "plotly.js-dist-min" {
  const Plotly: {
    react: (
      root: HTMLElement | string,
      data: unknown[],
      layout?: Record<string, unknown>,
      config?: Record<string, unknown>,
    ) => Promise<unknown>;
    purge: (root: HTMLElement | string) => void;
    relayout: (
      root: HTMLElement | string,
      update: Record<string, unknown>,
    ) => Promise<unknown>;
    Plots?: {
      resize: (root: HTMLElement) => void;
    };
  };
  export default Plotly;
}
