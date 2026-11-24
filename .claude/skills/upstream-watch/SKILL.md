---
name: upstream-watch
description: "Review recent activity on upstream Go Litestream (benbjohnson/litestream) and the LTX library (superfly/ltx), and decide for each item whether litestream-rs has the same bug to fix, the same rough edge to smooth, a format change to follow, or nothing to do. Use for /upstream-watch, \"what has Go fixed lately\", \"anything upstream we should port\", \"check upstream issues and pull requests\", \"did Go change the format\", \"is the upstream pin stale\", before moving the pinned upstream commit, and before a release. Not for answering one question about how Go behaves (read .upstream/ directly), and not for contributing to upstream (that is oss-contribution)."
argument-hint: "[days | YYYY-MM-DD]"
---

# upstream-watch: what Go learned since we last looked

Go Litestream has users and we mostly do not yet. Every bug they report is a test someone else ran for us: either we copied the bug, or we differ and should know why. The output is a short list of work, each item checked against our code, never a digest of upstream news.

## 1. The window

- **Since when:** the argument if given (a number of days, or a date). Otherwise the newest comment on issue #47 that starts "Reviewed upstream through"; it names the date and the commits last reviewed. No such comment: 30 days.
- **The pins:** `scripts/fetch-upstream.sh` names the Litestream commit and the LTX tag the compat tests build. `just upstream` clones both into `.upstream/`.

## 2. Collect

```bash
since=2026-10-24
gh pr list    --repo benbjohnson/litestream --state all --search "updated:>=$since" --limit 300 --json number,title,state,mergedAt,labels,url
gh issue list --repo benbjohnson/litestream --state all --search "updated:>=$since" --limit 300 --json number,title,state,labels,comments,url
gh api "repos/benbjohnson/litestream/compare/<pinned sha>...main" --jq '.commits[] | .sha[0:7] + " " + (.commit.message | split("\n")[0])'
gh api "repos/benbjohnson/litestream/releases?per_page=5" --jq '.[] | .tag_name + " " + .published_at[0:10]'
gh api "repos/superfly/ltx/compare/<pinned tag>...main" --jq '.commits[] | .sha[0:7] + " " + (.commit.message | split("\n")[0])'
```

Assert the lists are not empty before trusting an "all quiet": a failed query looks the same as a quiet month. Read only. Nothing is ever posted on an upstream repository from this skill.

## 3. First pass: drop what cannot be ours

From titles, labels and a glance at the body, set aside without reading further:

- storage other than S3 and a local directory, the VFS, the control socket and its commands, MCP, Windows services, the v0.3 layout (the "Go has it, litestream-rs does not" list in `docs/differences-from-go.md` is the authority);
- Go-only upkeep: dependency bumps, Go test flakes, CI, release tooling. Keep a dependency bump only when it names a bug in how Go talks to S3 or SQLite.

Count them in the report and name none of them.

## 4. Second pass: check each survivor against our code

Sort each into one kind, then give it a verdict. Read the upstream diff (`gh pr diff <n> --repo benbjohnson/litestream`) and the issue thread, then read our counterpart. `docs/go-terms.md` maps Go's names to ours and `docs/upstream-test-map.toml` maps Go's tests to ours.

| Kind | The question |
|---|---|
| A fix in code we ported (sync, checkpoints, compaction, retention, restore, the S3 client, config parsing, flags and output we mirror) | Do we have the same defect? |
| A change to what is in the bucket (layout, file names, LTX, levels, snapshots) | Can we still read what Go now writes, and can Go v0.5.16 and later still read what we write? This is the contract in `AGENTS.md`. |
| A user's trouble (confusing log line, surprising default, a wall an operator hit, a question the docs did not answer) | Would our user hit the same wall? Is our message, default or doc better, the same, or worse? |
| A design still being argued | Is it already on #47's wait list? |

Verdicts, each with its proof:

- **port**: we have it. Name our `file:line` and the smallest scenario that shows it. When upstream added a test with the fix, that scenario is the cheapest probe: would ours pass it?
- **already fine**: name the test of ours that would fail if we had the defect. No such test means the verdict is **untested**, and the missing test is the finding.
- **differs on purpose**: name the decision file under `docs/decisions/` or the row in `docs/differences-from-go.md`.
- **wait**: upstream has not settled it. It goes on #47's list with the upstream numbers.
- **not ours**: only for something step 3 should have caught.

Never write "probably fine". A verdict with no file, test or decision behind it is not a verdict.

More than about 25 survivors: hand them to Sonnet agents in groups of about 20, each with this section and the list, each read-only and reporting to a scratchpad file. Do not judge them one per main-session turn.

## 5. The pin

If upstream's main or a release is past the pinned commit, say by how many commits and whether any of them is a **port** or a format change. Moving the pin is its own pull request (`scripts/fetch-upstream.sh`, the justfile recipes that name the sha, then the compat suites). When it moves, re-read #47's wait list: that is when those items come due.

## 6. Record

- **port** and **untested**: one issue each on our repository, searched for first (`gh issue list --state all --search`). The title is our defect in our words. The body has the upstream link, our `file:line`, the scenario, and what done looks like. A fix starts with a test that fails on the old code (`AGENTS.md`); this skill files the work, it does not do it.
- **A user's trouble we share**: one issue each, or one for a group that has one fix.
- **wait**: edit #47's list.
- **A bug in Go that we do not have** and upstream has not noticed: say so to the owner. Reporting it upstream goes through the oss-contribution skill and is his to submit.
- **The watermark**: one comment on #47, "Reviewed upstream through <date>: litestream main <sha>, ltx <tag or sha>. <n> items: <a> port (#...), <b> untested (#...), <c> already fine, <d> on purpose, <e> wait, <f> not ours."

Issues and the comment end with "Authored by Claude (<full model slug>)." and carry nothing private.

## 7. Report

A categorized list in plain words, ports first, one line each with its issue link, then untested, then shared user troubles, then what moved on the wait list, then one line of counts. No table of everything that was read. End by offering to start the clear ports (the pickup skill).
