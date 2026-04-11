/** Deterministic insight endpoints — dataset, measure, dimension, quality, trend overviews. */

import { fetchJson } from "./client";

export interface OverviewResponse {
  overview: Record<string, unknown>;
  cached: boolean;
  error?: string;
}

function overviewUrl(endpoint: string, table?: string): string {
  const params = table ? `?table=${encodeURIComponent(table)}` : "";
  return `/api/${endpoint}${params}`;
}

export async function loadDatasetOverview(
  table?: string,
): Promise<OverviewResponse> {
  return fetchJson<OverviewResponse>(overviewUrl("dataset-overview", table));
}

export async function loadMeasureOverview(
  table?: string,
): Promise<OverviewResponse> {
  return fetchJson<OverviewResponse>(overviewUrl("measure-overview", table));
}

export async function loadDimensionOverview(
  table?: string,
): Promise<OverviewResponse> {
  return fetchJson<OverviewResponse>(overviewUrl("dimension-overview", table));
}

export async function loadQualityOverview(
  table?: string,
): Promise<OverviewResponse> {
  return fetchJson<OverviewResponse>(overviewUrl("quality-overview", table));
}

export async function loadTrendOverview(
  table?: string,
): Promise<OverviewResponse> {
  return fetchJson<OverviewResponse>(overviewUrl("trend-overview", table));
}

export async function loadTimeseriesOverview(
  table?: string,
): Promise<OverviewResponse> {
  return fetchJson<OverviewResponse>(overviewUrl("timeseries-overview", table));
}

export async function loadQueryLog(
  n: number = 50,
): Promise<{ entries: unknown[] }> {
  return fetchJson(`/api/query-log?n=${n}`);
}
