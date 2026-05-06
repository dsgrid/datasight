"""LLM-augmented tidy reshape advisor for ``datasight tidy review``.

Owns everything LLM-specific: the ``propose_reshapes`` tool schema, the
system + user prompts, the call wrapper, and a tolerant parser that turns
the model's tool-use output into validated :class:`TidySuggestion`
objects. The transactional apply pipeline and plan-file format live in
:mod:`datasight.tidy_review` and stay LLM-agnostic — the LLM advisor is
just one of three sources that can hand suggestions to that pipeline
(deterministic detector, plan file, LLM).

The model is given exactly one tool. If it has nothing to propose it must
still call the tool, with an empty ``proposals`` list. This keeps the
output path uniform: every successful call yields a structured
list-of-proposals, never free-form prose.

Validation runs in two passes, mirroring :mod:`tidy_review`:

- :func:`parse_llm_proposals` is forgiving — it parses each proposal in
  isolation, drops the malformed ones with a per-proposal warning, and
  returns the survivors. A single bad proposal does not torpedo the batch.
- :func:`validate_against_schema` from :mod:`tidy_review` cross-checks
  each survivor against the live database; the CLI runs that next.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from loguru import logger

from datasight.llm import LLMClient, TextBlock, ToolUseBlock
from datasight.tidy import TidySuggestion
from datasight.tidy_review import _parse_proposal


# ---------------------------------------------------------------------------
# Tool definition
# ---------------------------------------------------------------------------


PROPOSE_RESHAPES_TOOL: dict[str, Any] = {
    "name": "propose_reshapes",
    "description": (
        "Return zero or more proposals to reshape an untidy (wide) table into "
        "long form. Only propose a reshape when a single latent dimension "
        "(or a small product of dimensions) is encoded across column names. "
        "Skip tables that are already tidy. Do not re-propose anything in "
        "the `already_detected_by_regex` block of the user message — those "
        "are already handled by the deterministic detector. Always call this "
        "tool exactly once, even if no reshapes are warranted (pass an empty "
        "list)."
    ),
    "input_schema": {
        "type": "object",
        "required": ["proposals"],
        "properties": {
            "proposals": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [
                        "table",
                        "dimensions",
                        "value_column",
                        "id_columns",
                        "column_mappings",
                        "confidence",
                        "rationale",
                    ],
                    "properties": {
                        "table": {
                            "type": "string",
                            "description": "Source table name from the schema.",
                        },
                        "dimensions": {
                            "type": "array",
                            "minItems": 1,
                            "description": (
                                "One entry per encoded dimension. "
                                "Multi-pivot reshapes use two or more entries."
                            ),
                            "items": {
                                "type": "object",
                                "required": ["name", "kind"],
                                "properties": {
                                    "name": {
                                        "type": "string",
                                        "description": (
                                            "Snake_case name for the new "
                                            "dimension column, e.g., "
                                            "'fuel_type', 'year', 'scenario'."
                                        ),
                                    },
                                    "kind": {
                                        "type": "string",
                                        "enum": [
                                            "date_period",
                                            "category",
                                            "geography",
                                            "scenario",
                                            "other",
                                        ],
                                    },
                                },
                            },
                        },
                        "value_column": {
                            "type": "string",
                            "description": (
                                "Snake_case name for the new measure column, "
                                "e.g., 'net_generation_mwh'."
                            ),
                        },
                        "id_columns": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": (
                                "Columns to carry through unchanged. Must not "
                                "overlap any column in column_mappings."
                            ),
                        },
                        "column_mappings": {
                            "type": "array",
                            "minItems": 2,
                            "description": (
                                "How each affected column maps to dimension "
                                "values. The dimension_values keys must "
                                "exactly match the names in `dimensions`."
                            ),
                            "items": {
                                "type": "object",
                                "required": ["column", "dimension_values"],
                                "properties": {
                                    "column": {"type": "string"},
                                    "dimension_values": {
                                        "type": "object",
                                        "description": (
                                            "Map of dimension name to the "
                                            "literal value the column "
                                            "encodes, e.g., {'fuel_type': "
                                            "'coal', 'year': '2020'}."
                                        ),
                                        "additionalProperties": {"type": "string"},
                                    },
                                },
                            },
                        },
                        "target_object_name": {
                            "type": "string",
                            "description": ("Optional. Defaults to '<table>_long' if omitted."),
                        },
                        "confidence": {
                            "type": "string",
                            "enum": ["high", "medium", "low"],
                        },
                        "rationale": {
                            "type": "string",
                            "description": (
                                "One or two sentences a domain expert would "
                                "understand. Mention the encoded dimension(s)."
                            ),
                        },
                    },
                },
            }
        },
    },
}


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------


SYSTEM_PROMPT = """\
You help a data engineer curate datasets for an energy-research analytics
tool. You inspect table schemas and propose reshapes from wide (untidy)
form to long (tidy) form, where each row is one observation of one
measure.

A reshape is appropriate when several columns share a latent dimension
encoded in their names — common cases:

- Period-as-column: `gen_2020_01`, `mwh_q1`, `hour_00`, `day_15`.
- Category-as-column: `coal_mwh`, `gas_mwh`, `nuclear_mwh`, `solar_mwh`.
- Geography-as-column: `ca_capacity`, `tx_capacity`, `ny_capacity`.
- Scenario-as-column: `base_case_load`, `high_growth_load`, `low_load`.
- Multi-axis pivots: `coal_2020`, `coal_2021`, `gas_2020`, `gas_2021` —
  one proposal with two dimensions (`fuel_type` and `year`), not two.

A reshape is NOT appropriate when:

- Columns represent distinct measures that happen to share a prefix
  (`capacity_mw`, `capacity_factor`, `capacity_cost`).
- The table is already in long form (one row per observation).
- The columns are unrelated identifiers or attributes.

When unsure, prefer to omit the proposal — the developer reviews
everything you return.

Always use snake_case for new column names. Prefer domain-meaningful
names (`fuel_type`, `net_generation_mwh`) over generic ones (`category`,
`value`) when the column names give you enough signal. Make `id_columns`
the columns whose values identify a row in the original wide form
(plant_id, region, report_date) — never include columns you are pivoting.

Call the `propose_reshapes` tool exactly once. Pass an empty list if no
reshapes are warranted.
"""


def build_user_message(
    schema_info: list[dict[str, Any]],
    deterministic_hits: list[dict[str, Any]],
    samples: dict[str, list[dict[str, Any]]] | None = None,
) -> str:
    """Build the user message sent alongside the system prompt.

    JSON-serializes the inputs into three labeled blocks. JSON keeps the
    model from spending tokens parsing rendered tables and lets it cite
    column names verbatim.

    ``schema_info`` is the list of table descriptors from
    :func:`datasight.schema.introspect_schema` (already a plain list of
    dicts via the data_profile pipeline). ``deterministic_hits`` is the
    output of ``analyze_tidy_patterns(schema_info)["suggestions"]`` —
    passing those tells the model "these are already covered, don't
    re-propose them." ``samples`` is optional opt-in row data per table.
    """
    blocks: list[str] = []
    blocks.append("<schema>")
    blocks.append(json.dumps(schema_info, indent=2, default=str))
    blocks.append("</schema>")
    blocks.append("")
    blocks.append("<already_detected_by_regex>")
    blocks.append(json.dumps(deterministic_hits, indent=2))
    blocks.append("</already_detected_by_regex>")
    if samples:
        blocks.append("")
        blocks.append("<samples>")
        blocks.append(json.dumps(samples, indent=2, default=str))
        blocks.append("</samples>")
    return "\n".join(blocks)


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------


@dataclass
class ProposeResult:
    """Outcome of a single ``propose_reshapes`` call.

    ``raw_proposals`` is the unparsed list straight from the model — kept
    for debugging and so callers can show "the LLM returned N proposals,
    K were dropped during validation." ``suggestions`` is the validated
    survivors ready to feed to the apply pipeline.
    """

    suggestions: list[TidySuggestion]
    raw_proposals: list[dict[str, Any]]
    parse_warnings: list[str]


async def propose_reshapes(
    llm_client: LLMClient,
    *,
    model: str,
    schema_info: list[dict[str, Any]],
    deterministic_hits: list[dict[str, Any]],
    samples: dict[str, list[dict[str, Any]]] | None = None,
    max_tokens: int = 4096,
) -> ProposeResult:
    """Ask the LLM for tidy-reshape proposals and parse the structured response.

    The model is constrained to call the ``propose_reshapes`` tool once.
    We don't loop the agent — there's no follow-up tool to chain into,
    and a free-text response is treated as "no proposals" rather than as
    a separate signal. That keeps the surface narrow and the failure
    modes obvious.
    """
    user_message = build_user_message(schema_info, deterministic_hits, samples)
    response = await llm_client.create_message(
        model=model,
        max_tokens=max_tokens,
        system=SYSTEM_PROMPT,
        tools=[PROPOSE_RESHAPES_TOOL],
        messages=[{"role": "user", "content": user_message}],
    )
    raw_proposals: list[dict[str, Any]] = []
    for block in response.content:
        if isinstance(block, ToolUseBlock) and block.name == "propose_reshapes":
            input_proposals = block.input.get("proposals")
            if isinstance(input_proposals, list):
                raw_proposals.extend(input_proposals)
        elif isinstance(block, TextBlock):
            # Models occasionally narrate alongside the tool call. We don't
            # surface the prose to the developer — they see the structured
            # rationale on each proposal. Logging at debug keeps it
            # available for diagnostics without cluttering output.
            logger.debug("LLM narration alongside tool call: {}", block.text)
    return parse_llm_proposals(raw_proposals)


# ---------------------------------------------------------------------------
# Parsing the tool output
# ---------------------------------------------------------------------------


def parse_llm_proposals(raw_proposals: list[dict[str, Any]]) -> ProposeResult:
    """Validate each proposal independently; drop malformed ones with a warning.

    Reuses ``tidy_review._parse_proposal`` so the LLM path applies the
    same structural rules a hand-written plan would (dimension key sets
    match, no id/mapping overlap, ≥2 mappings, valid kinds and confidence
    levels). Failures from a single proposal don't sink the batch — the
    developer still gets to review the survivors.
    """
    suggestions: list[TidySuggestion] = []
    warnings: list[str] = []
    for index, raw in enumerate(raw_proposals):
        try:
            suggestion = _parse_proposal(raw, index=index)
        except ValueError as exc:
            label = (
                raw.get("table", "<unknown table>")
                if isinstance(raw, dict)
                else "<malformed proposal>"
            )
            warning = f"Dropped LLM proposal #{index} ({label}): {exc}"
            warnings.append(warning)
            logger.warning(warning)
            continue
        # Stamp the source so the renderer can distinguish LLM-proposed
        # suggestions from deterministic ones.
        suggestion.source = "llm"
        suggestions.append(suggestion)
    return ProposeResult(
        suggestions=suggestions,
        raw_proposals=list(raw_proposals),
        parse_warnings=warnings,
    )


__all__ = [
    "PROPOSE_RESHAPES_TOOL",
    "SYSTEM_PROMPT",
    "ProposeResult",
    "build_user_message",
    "parse_llm_proposals",
    "propose_reshapes",
]
