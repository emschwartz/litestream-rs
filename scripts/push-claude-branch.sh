#!/usr/bin/env bash
# Push the current branch to origin, only if it is a claude/* branch.
# The one push that .claude/settings.json allows without a prompt: it takes
# no arguments, so no refspec, force flag or other remote can be smuggled in.
set -euo pipefail
if [ "$#" -ne 0 ]; then
  echo "push-claude-branch: takes no arguments; it pushes the current branch" >&2
  exit 2
fi
branch=$(git symbolic-ref --quiet --short HEAD) || {
  echo "push-claude-branch: HEAD is detached" >&2
  exit 2
}
if ! [[ "$branch" =~ ^claude/[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$ ]]; then
  echo "push-claude-branch: '$branch' is not a claude/* branch" >&2
  exit 2
fi
exec git push -u origin "refs/heads/$branch:refs/heads/$branch"
