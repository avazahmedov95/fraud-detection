# thesis

Material for the thesis, gathered here on 2026-10-03 so it can be removed in one
step once the thesis is written. Nothing in the running system reads these files,
and they are not kept in step with the code: they describe the project as of
commit `cfd3a4b`, and their links point to paths of that commit.

| File | What it is |
|---|---|
| `irp-framing.md` | the research question, every measurement with its interval, the answers to the review points, the silent failure modes |
| `related-work.md` | the literature and public datasets, and what each supports |
| `threat-model.md` | the fraud patterns, what the system sees of each, what it cannot |
| `generator-spec.md` | how the synthetic data is generated and calibrated |
| `audit-anchors.md` | the head hash of each audit chain, pinned to a commit |
| `ml.md` | the model's full history: every experiment, gate and decision |
| `validation.md` | the public datasets in full: rules, model, second look, typologies |
| `stream-processor.md`, `case-manager.md`, `second-look.md`, `data-generator.md` | the long design notes of each service, before they were shortened |

The experiment code these documents cite was removed on 2026-10-03; it is in git
history: `git show cfd3a4b:<path>`.
