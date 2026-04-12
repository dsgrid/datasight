# Try the time-validation demo

This tutorial walks you through the time-validation demo — a synthetic
energy-consumption dataset with intentional time-series errors planted in
it — so you can see what datasight's quality checks catch. Allow about
three minutes.

## What you'll load

The demo generates roughly 70 MB of synthetic hourly data with a mix of
realistic values and planted issues:

- missing hours (gaps in the hourly series)
- duplicate timestamps
- DST spring-forward and fall-back artifacts
- leap-year inconsistencies across groups

A `time_series.yaml` file ships with the demo declaring the expected
temporal structure, so `datasight quality` knows what "correct" looks
like.

## 1. Install datasight

```bash
pip install git+https://github.com/dsgrid/datasight.git
```

## 2. Generate the dataset

```bash
datasight demo time-validation ./tv-project
cd tv-project
```

Run `datasight demo time-validation --help` for generator options
(dataset size, planted-error rate, seed).

## 3. Detect the planted errors

```bash
datasight quality
```

The output includes **Time Series** and **Temporal Completeness** sections
listing each gap and duplicate the generator planted.

## 4. Explore interactively

Add an API key to `.env` (see [Set up a project](../../project-developer/set-up-project.md#configure))
and launch the web UI:

```bash
datasight run
```

Try questions like:

- *Are there any gaps in the load data?*
- *Which groups have the most duplicate timestamps?*
- *Show the hourly load profile for January 1*

## What's next

- [Declare time series](../how-to/declare-time-series.md) — write a
  `time_series.yaml` for your own data so `datasight quality` can audit
  completeness.
- [Audit data quality](../how-to/audit-data-quality.md) — the full set of
  deterministic quality commands.
