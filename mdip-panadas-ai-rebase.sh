#!/usr/bin/env bash
set -euo pipefail
#MARS-Padas-AI branch rebase script
# Update the local branch MARS-Pandas-AI with upstream/main while preserving local commits.

REPO_DIR="/Volumes/MAC_EXT/My_Home/code/MARS-AI/MARS-AI/pandas-ai"
BRANCH="MARS-Pandas-AI"
UPSTREAM_URL="https://github.com/sinaptik-ai/pandas-ai.git"

cd "$REPO_DIR"

# Ensure upstream remote exists
if ! git remote | grep -q "^upstream$"; then
	git remote add upstream "$UPSTREAM_URL"
fi

# Fetch upstream updates
git fetch upstream

# Ensure the target branch exists locally; if not, try to track origin/BRANCH
if git show-ref --verify --quiet "refs/heads/$BRANCH"; then
	git checkout "$BRANCH"
else
	if git ls-remote --heads origin "$BRANCH" | grep -q "$BRANCH"; then
		git checkout -b "$BRANCH" --track origin/"$BRANCH"
	else
		echo "Error: branch $BRANCH not found locally or on origin." >&2
		exit 1
	fi
fi

# Rebase local commits onto upstream/main (preferred to keep linear history).
echo "Rebasing $BRANCH onto upstream/main..."
git rebase upstream/main

# If rebase succeeded, push updates to origin branch. Use force-with-lease to be safer.
echo "Pushing $BRANCH to origin..."
git push --force-with-lease origin "$BRANCH"

echo "Update complete. If there were conflicts during rebase, resolve them and run 'git rebase --continue'."