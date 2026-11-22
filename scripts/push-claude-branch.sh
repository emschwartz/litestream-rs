#!/usr/bin/env bash
# Push the current branch to origin, only if it is a claude/* branch.
# The one push that .claude/settings.json allows without a prompt: it takes
# no arguments, so no refspec, force flag or other remote can be smuggled in.
set -euo pipefail
g() { git -c core.hooksPath=/dev/null "$@"; }
if [ "$#" -ne 0 ]; then
  echo "push-claude-branch: takes no arguments; it pushes the current branch" >&2
  exit 2
fi
branch=$(g symbolic-ref --quiet --short HEAD) || {
  echo "push-claude-branch: HEAD is detached" >&2
  exit 2
}
if ! [[ "$branch" =~ ^claude/[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$ ]]; then
  echo "push-claude-branch: '$branch' is not a claude/* branch" >&2
  exit 2
fi
url=$(g remote get-url --push origin)
if ! [[ "$url" =~ ^(https://github\.com/|http://(127\.0\.0\.1|localhost):[0-9]+/git/)emschwartz/litestream-rs(\.git)?$ ]]; then
  echo "push-claude-branch: origin does not push to emschwartz/litestream-rs" >&2
  exit 2
fi
g push --no-verify "$url" "refs/heads/$branch:refs/heads/$branch"
# A push to a URL leaves origin/<branch> stale; record what was pushed.
g update-ref "refs/remotes/origin/$branch" "refs/heads/$branch"
g config "branch.$branch.remote" origin
g config "branch.$branch.merge" "refs/heads/$branch"
