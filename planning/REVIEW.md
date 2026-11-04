# Change Review

Reviewed changes relative to `HEAD` (`14550e1 Ready for Teams`).

## Findings

### [P1] Stop hook runs a full review on every Claude session stop

`independent-reviewer/hooks/hooks.json` invokes `codex exec "Review changes since last commit and write results to a file named planning/REVIEW.md"` for every `Stop` event. This makes a potentially long, mutating review run part of every normal Claude session, even when the user has not requested a review and there are no changes. It can also repeatedly overwrite `planning/REVIEW.md` with results unrelated to the current task. Make this an explicit command/skill invocation, or gate it on an opt-in condition instead of the global Stop event.

### [P2] Existing enabled plugins are silently disabled

`.claude/settings.json` removes `frontend-design`, `context7`, and `playwright` from `enabledPlugins` while adding `independent-reviewer`. This changes the project environment for existing users and can break workflows relying on those plugins. Preserve the existing entries unless their removal is intentional and documented.

## Other observations

- The README update correctly describes the current implementation as backend-only and points to the project plan.
- The added tracked-ticker rule in `planning/PLAN.md` clarifies an important relationship between watchlist membership and open positions. The associated DELETE endpoint description is consistent with it.
- `git diff --check HEAD` reported no whitespace errors.
