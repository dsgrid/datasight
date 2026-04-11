/** Settings — query behavior toggles and LLM configuration. */

export interface LlmConfig {
  provider: string;
  model: string;
  base_url: string;
  has_api_key: boolean;
  connected: boolean;
  error?: string;
  env_keys?: Record<string, boolean>;
  env_models?: Record<string, string>;
}

function createSettingsStore() {
  let confirmSql = $state(false);
  let explainSql = $state(false);
  let clarifySql = $state(false);
  let showCost = $state(true);
  let llmConnected = $state(false);
  let llmConfig = $state<LlmConfig | null>(null);

  return {
    get confirmSql() {
      return confirmSql;
    },
    set confirmSql(v: boolean) {
      confirmSql = v;
    },
    get explainSql() {
      return explainSql;
    },
    set explainSql(v: boolean) {
      explainSql = v;
    },
    get clarifySql() {
      return clarifySql;
    },
    set clarifySql(v: boolean) {
      clarifySql = v;
    },
    get showCost() {
      return showCost;
    },
    set showCost(v: boolean) {
      showCost = v;
    },
    get llmConnected() {
      return llmConnected;
    },
    set llmConnected(v: boolean) {
      llmConnected = v;
    },
    get llmConfig() {
      return llmConfig;
    },
    set llmConfig(v: LlmConfig | null) {
      llmConfig = v;
    },

    applyFromApi(data: {
      confirm_sql: boolean;
      explain_sql: boolean;
      clarify_sql: boolean;
      show_cost: boolean;
    }) {
      confirmSql = data.confirm_sql;
      explainSql = data.explain_sql;
      clarifySql = data.clarify_sql;
      showCost = data.show_cost;
    },
  };
}

export const settingsStore = createSettingsStore();
