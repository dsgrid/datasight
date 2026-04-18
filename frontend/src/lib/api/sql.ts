/** Raw SQL execution + sqlglot validation for the SQL editor page. */

import { postJson } from "./client";
import { sessionStore } from "$lib/stores/session.svelte";

export interface SqlExecuteResult {
  html: string | null;
  row_count: number;
  elapsed_ms: number;
  error: string | null;
}

export interface SqlValidateResult {
  valid: boolean;
  errors: string[];
}

export interface SqlFormatResult {
  formatted: string;
  error: string | null;
}

export async function executeSql(sql: string): Promise<SqlExecuteResult> {
  return postJson<SqlExecuteResult>("/api/sql-execute", {
    sql,
    session_id: sessionStore.sessionId,
  });
}

export async function validateSql(sql: string): Promise<SqlValidateResult> {
  return postJson<SqlValidateResult>("/api/sql-validate", { sql });
}

export async function formatSql(sql: string): Promise<SqlFormatResult> {
  return postJson<SqlFormatResult>("/api/sql-format", { sql });
}
