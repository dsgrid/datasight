/** Session and project state. */

export interface EphemeralTableInfo {
  name: string;
  source: string;
  row_count: number;
}

function createSessionStore() {
  const storedSessionId =
    localStorage.getItem("datasight-session") || crypto.randomUUID();
  localStorage.setItem("datasight-session", storedSessionId);

  let sessionId = $state(storedSessionId);
  let projectLoaded = $state(false);
  let currentProjectPath = $state<string | null>(null);
  let isEphemeralSession = $state(false);
  let explorePaths = $state<string[]>([]);
  let ephemeralTablesInfo = $state<EphemeralTableInfo[]>([]);
  let hasTimeSeries = $state(false);

  return {
    get sessionId() {
      return sessionId;
    },
    set sessionId(v: string) {
      sessionId = v;
      localStorage.setItem("datasight-session", v);
    },
    get projectLoaded() {
      return projectLoaded;
    },
    set projectLoaded(v: boolean) {
      projectLoaded = v;
    },
    get currentProjectPath() {
      return currentProjectPath;
    },
    set currentProjectPath(v: string | null) {
      currentProjectPath = v;
    },
    get isEphemeralSession() {
      return isEphemeralSession;
    },
    set isEphemeralSession(v: boolean) {
      isEphemeralSession = v;
    },
    get explorePaths() {
      return explorePaths;
    },
    set explorePaths(v: string[]) {
      explorePaths = v;
    },
    get ephemeralTablesInfo() {
      return ephemeralTablesInfo;
    },
    set ephemeralTablesInfo(v: EphemeralTableInfo[]) {
      ephemeralTablesInfo = v;
    },
    get hasTimeSeries() {
      return hasTimeSeries;
    },
    set hasTimeSeries(v: boolean) {
      hasTimeSeries = v;
    },

    /** Reset session for a new project or explore. */
    reset() {
      sessionId = crypto.randomUUID();
      localStorage.setItem("datasight-session", sessionId);
      projectLoaded = false;
      currentProjectPath = null;
      isEphemeralSession = false;
      explorePaths = [];
      ephemeralTablesInfo = [];
      hasTimeSeries = false;
    },
  };
}

export const sessionStore = createSessionStore();
