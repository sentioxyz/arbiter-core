# AGENTS.md

Guidance for coding agents working in `arbiter-core` — the Arbiter data plane / wire library (SNode, verifier, codec). Build, test and operations are described in `README.md`.

## Design docs

Specs, implementation plans, rollout plans, roadmaps, reports and decisions for this repo live in the `sentio-ws` workspace repo (`sentioxyz/sentio-ws`, private), not here: `sentio-ws/internal-docs/sentio-network/`. Start at its `README.md` (stage timeline, module index; this repo's index is `modules/arbiter/README.md`) and follow its `CONVENTIONS.md` for placement, naming, frontmatter and references.

- Do not add `docs/superpowers/`, `docs/specs/` or `docs/plans/` here. When a workflow skill (brainstorming, writing-plans, …) would save a spec or plan under `docs/superpowers/`, write it to `sentio-ws/internal-docs/sentio-network/stages/<stage>/<phase>/` instead and commit it in `sentio-ws` (direct to `main`, no PR).
- From this repo, cite a design as `sentio-ws/internal-docs/sentio-network/<path>` (or its GitHub URL).
- What stays here: README and wire-compatibility records that the code and tests cite (`docs/compatibility/`).
