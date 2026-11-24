---
name: upstream-watch
description: "Review recent activity on upstream Go Litestream (benbjohnson/litestream) and the LTX library (superfly/ltx), and decide for each item whether litestream-rs has the same bug to fix, the same rough edge to smooth, a format change to follow, or nothing to do. Use for /upstream-watch, \"what has Go fixed lately\", \"anything upstream we should port\", \"check upstream issues and pull requests\", \"did Go change the format\", \"is the upstream pin stale\", before moving the pinned upstream commit, and before a release. Not for answering one question about how Go behaves (read .upstream/ directly), and not for contributing to upstream (that is oss-contribution)."
argument-hint: "[days | YYYY-MM-DD]"
---

# upstream-watch: what Go learned since we last looked

Go Litestream has users and we mostly do not yet. Every bug they report is a test someone else ran for us: either we copied the bug, or we differ and should know why. The output is a short list of work, each item checked against our code, never a digest of upstream news.

## 1. The window

- **Since when:** the argument if given (a number of days, or a date); it always wins. Otherwise the newest comment on issue #47 that starts "Reviewed upstream through"; it names the date and the commits last reviewed. No such comment: 30 days.
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

Keep the quotes around every URL with a `?` in it (zsh globs it otherwise). A list of exactly 300 was cut short: narrow the window and run it again. Most pull requests merged in a window are already inside the pin: for those the question is whether our code has the same defect, not whether the pin lacks the fix. Say in the report which count is which.

Assert the lists are not empty before trusting an "all quiet": a failed query looks the same as a quiet month. Read only. Nothing is ever posted on an upstream repository from this skill.

## 3. First pass: drop what cannot be ours

A title often hides a ported area ("fix(wal)", "L0 retention check"). Never set an item aside on its title when it names retention, compaction, snapshots, checkpoints, restore, the WAL, sync or S3: read its body first. For the rest, from titles, labels and a glance at the body, set aside without reading further:

- storage other than S3 and a local directory, the VFS, the control socket and its commands, MCP, Windows services, the v0.3 layout (the authorities are the "Go has it, litestream-rs does not" list in `docs/differences-from-go.md` and the `[reasons]` in `docs/upstream-test-map.toml`);
- Go-only upkeep: dependency bumps, Go test flakes, CI, release tooling. Keep a dependency bump only when it names a bug in how Go talks to S3 or SQLite.

Count them in the report and name none of them.

## 4. Second pass: check each survivor against our code

Sort each into one kind, then give it a verdict. Read the upstream diff (`gh pr diff <n> --repo benbjohnson/litestream`) and the issue thread, then read our counterpart. `docs/go-terms.md` maps Go's names to ours and `docs/upstream-test-map.toml` maps Go's tests to ours. Where to start for the hot paths: Go's `verify` in `db.go` is `crates/core/src/recover.rs`; the snapshot reader is `crates/core/src/snapshot.rs`; `CalcRestorePlan` is `crates/core/src/plan.rs` and `restore.rs`; the store's monitor loops are the maintenance unit in `crates/core/src/actor.rs` and `crates/core/src/maintain/`; `replica.go`'s restore is `crates/core/src/restore.rs`.

| Kind | The question |
|---|---|
| A fix in code we ported (sync, checkpoints, compaction, retention, restore, the S3 client, config parsing, flags and output we mirror) | Do we have the same defect? |
| A change to what is in the bucket (layout, file names, LTX, levels, snapshots) | Can we still read what Go now writes, and can Go v0.5.16 and later still read what we write? This is the contract in `AGENTS.md`. |
| A user's trouble (confusing log line, surprising default, a wall an operator hit, a question the docs did not answer) | Would our user hit the same wall? Is our message, default or doc better, the same, or worse? |
| A design still being argued | Is it already on #47's wait list? |

Verdicts, each with its proof:

- **port**: we have it. Name our `file:line` and the smallest scenario that shows it. When upstream added a test with the fix, that scenario is the cheapest probe: would ours pass it?
- **already fine**: name the test of ours that would fail if we had the defect, and open it to check that it builds the scenario: a test with the right name that always syncs first does not cover a bug about unsynced frames. No such test means the verdict is **untested**, and the missing test is the finding.
- **needs a run**: for a cost or speed problem (requests per hour, time per file) where reading shows the same shape but only a count or a measurement shows it matters. Say exactly what to count.
- **differs on purpose**: name the decision file under `docs/decisions/` or the row in `docs/differences-from-go.md`.
- **wait**: upstream has not settled it. It goes on #47's list with the upstream numbers.
- **not ours**: only for something step 3 should have caught.

Never write "probably fine". A verdict with no file, test or decision behind it is not a verdict.

More than about 25 survivors: hand them to Sonnet agents in groups of about 20, each with this section and the list, each read-only and reporting to a scratchpad file, unless the owner said otherwise. Do not judge them one per main-session turn.

## 5. The pin

If upstream's main or a release is past the pinned commit, say by how many commits and whether any of them is a **port** or a format change. Moving the pin is its own pull request (`scripts/fetch-upstream.sh`, the justfile recipes that name the sha, then the compat suites). When it moves, re-read #47's wait list: that is when those items come due.

## 6. Record

- **port**, **untested** and **needs a run**: one issue each on our repository, searched for first (`gh issue list --state all --search "<two or three keywords>"`, open and closed). The title is our defect in our words. The body has the upstream link, our `file:line`, the scenario, and what done looks like. A fix starts with a test that fails on the old code (`AGENTS.md`); this skill files the work, it does not do it.
- **A user's trouble we share**: one issue each, or one for a group that has one fix.
- **wait**: edit #47's list.
- **A bug in Go that we do not have** and upstream has not noticed: say so to the owner. Reporting it upstream goes through the oss-contribution skill and is his to submit.
- **The watermark**: one comment on #47, "Reviewed upstream through <date>: litestream main <sha>, ltx <tag or sha>. <n> items: <a> port (#...), <b> untested (#...), <c> needs a run (#...), <d> already fine, <e> on purpose, <f> wait, <g> not ours."

Issues and the comment end with "Authored by Claude (<full model slug>)." and carry nothing private.

## 7. Report

A categorized list in plain words, ports first, one line each with its issue link, then untested, then shared user troubles, then what moved on the wait list, then one line of counts. No table of everything that was read. End by offering to start the clear ports (the pickup skill).
