---
description: Run the Definition of Done checklist against the current change
---
Go through the **Definition of Done** checklist in `AGENTS.md` item by item for the
current diff (`git diff` plus any staged and untracked files).

For each item output one line: `PASS` / `FAIL` / `N/A` followed by a one-sentence reason
pointing at specific files or lines. Be skeptical — a missing test is a `FAIL`, not `N/A`.

Then:
1. Run `just check` and report the outcome verbatim.
2. If any Raft correctness invariant in `AGENTS.md` is touched by this change, state which
   one and how the change preserves it.
3. Give a final verdict: **DONE** only if every applicable item is `PASS` and `just check`
   is green. Otherwise list what remains.

Do not fix anything in this command — only report.
