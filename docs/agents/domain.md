# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Layout

This repo is configured as a multi-context repo. It is expected to move toward bounded contexts.

Use:

- `CONTEXT-MAP.md` at the repo root when it exists. It points to the relevant context docs.
- Context-specific `CONTEXT.md` files as bounded contexts are introduced.
- `docs/adr/` for system-wide architectural decisions.
- `src/<context>/docs/adr/` for context-scoped architectural decisions when present.

## Before exploring

Read the context files and ADRs relevant to the area being changed.

If `CONTEXT-MAP.md`, a context `CONTEXT.md`, or context-scoped ADRs do not exist yet, proceed silently. Do not flag their absence or create them upfront. Producer skills such as `$grill-with-docs` can create them lazily when domain terms or architectural decisions are resolved.

## Vocabulary

When an output names a domain concept in an issue title, refactor proposal, hypothesis, or test name, use the term as defined in the relevant `CONTEXT.md`.

If the concept is not in the glossary yet, do not invent stable terminology. Note the gap for `$grill-with-docs` when it matters.

## ADR conflicts

If a proposal or implementation contradicts an existing ADR, surface the contradiction explicitly rather than silently overriding it.
