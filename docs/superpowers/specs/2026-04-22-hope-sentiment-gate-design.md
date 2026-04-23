# Hope query: add positive sentiment gate to macro branch

**Date:** 2026-04-22
**Branch:** feat/keyword-extraction
**Status:** Approved

## Problem

The despair query's macro branch is gated by `sentiment:negative` so generic
negative-emotion words co-occurring with macro-economic terms only count when
the content is classified negative. Hope's macro branch has no equivalent
positive gate, so neutral or sarcastic "stock market" + "happy" content can
inflate hope. This makes hope and despair asymmetric in a way that biases the
Fear-Cheer Index.

## Goal

Make hope's macro branch mirror despair's: require a positive sentiment
classification alongside generic-positive-emotion + macro-economic-term
co-occurrence. Do not otherwise restructure hope — its `financial_context +
intent:purchase` branch and `personal_wins` branch stay as they are.

## Change

### `queries/composition.json`

Hope's third branch, from:

```json
{"op":"and","values":[
  {"bucket":"macro_economic_terms"},
  {"bucket":"generic_positive_emotion"}
]}
```

to:

```json
{"op":"and","values":[
  {"bucket":"generic_positive_emotion"},
  {"bucket":"macro_economic_terms"},
  {"sentiment":"positive"}
]}
```

The ESI query has a parallel macro-positive branch (esi is the union of hope
and despair shapes plus taxonomies). Apply the same three-clause AND there to
keep esi's hope-side consistent with the updated hope query.

Final hope shape:

```
OR(
  AND(financial_context, themes:intent|purchase),
  bucket_labeled:personal_wins,
  AND(generic_positive_emotion, macro_economic_terms, sentiment:positive)
)
```

### Regeneration

Run `python3 scripts/compile_queries.py` to regenerate `queries/hope.json` and
`queries/esi.json`. `--check` must pass afterward.

## Non-goals

- No keyword list edits.
- No changes to `personal_wins` or the `themes:intent|purchase` branch.
- No DSL changes — `{sentiment:"positive"}` already maps to `(Positive, p)` via
  `SENTIMENT_MAP` in `scripts/compile_queries.py`.
- No changes to `update_data.py` or `BASE_QUERY`.

## Verification

1. `python3 scripts/compile_queries.py` regenerates cleanly.
2. `python3 scripts/compile_queries.py --check` returns ok.
3. `queries/hope.json` third top-level OR branch contains the new
   `{"op":"contains","fields":["sentiment"],"labels":["Positive"],"values":["p"]}`
   clause alongside the generic-positive-emotion and macro-economic-term
   contains clauses.
4. `queries/esi.json` has the matching three-clause AND in its hope-side macro
   branch.

## Risk

Low. This only tightens the hope query; existing branches unchanged. Expected
effect: fewer false positives on the macro branch, slightly lower hope volume,
more symmetric comparison to despair.
