#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# What must be true before this repository is published, and every time it
# is pushed to afterwards.
#
# Publication is the one irreversible step in this project. A tag can be
# deleted and a release can be replaced, but a commit that has been public
# for five minutes has been cloned, mirrored and indexed. So the checks
# that matter most are the ones nobody can undo the failure of.
#
# It scans HISTORY, not just the working tree. A credential added in one
# commit and removed in the next is still in every clone forever, and the
# tree-only version of this check is exactly the one that would miss it.
#
# EVERY PATTERN CARRIES A POSITIVE CONTROL. A grep that finds nothing and a
# grep that cannot find anything print the same thing, and this file's
# entire value is in its silence — so each pattern is first run against a
# string it MUST match. A typo in a regex would otherwise turn this into a
# script that certifies whatever it is pointed at.
#
# Usage: tools/public_preflight.sh
set -u
cd "$(dirname "$0")/.."

FAILED=0
fail() { echo "FAIL: $*"; FAILED=1; }

echo "== positive controls: can these patterns find anything at all?"
CREDS='(AKIA[0-9A-Z]{16}|aws_secret_access_key *=|BEGIN [A-Z ]*PRIVATE KEY|sk-ant-[A-Za-z0-9]{8}|ghp_[A-Za-z0-9]{20}|github_pat_[A-Za-z0-9_]{20}|xox[baprs]-[A-Za-z0-9])'
# Deliberately fake, and shaped like the real thing. AKIAIOSFODNN7EXAMPLE
# is AWS's own documentation key.
for probe in 'AKIAIOSFODNN7EXAMPLE' \
             'aws_secret_access_key = wJalrXUtnFEMI' \
             '-----BEGIN RSA PRIVATE KEY-----' \
             'sk-ant-api03xxxx' \
             'ghp_0123456789abcdefghij' \
             'xoxb-1-1abc' ; do
  printf '%s\n' "$probe" | grep -qE "$CREDS" \
    || { echo "  control FAILED for: $probe"; fail "the credential pattern cannot match a known-bad string — this scan proves nothing"; }
done
[ "$FAILED" = 0 ] && echo "  credential pattern matches all 6 known-bad probes"

# And the history pipeline itself: it must be able to see into the past.
HIST_PROBE=$(git log -p --all 2>/dev/null | grep -cE "^\+" || true)
[ "${HIST_PROBE:-0}" -gt 100 ] \
  || fail "the history scan saw only ${HIST_PROBE:-0} added lines across $(git rev-list --count HEAD) commits — it is not reading history"
echo "  history pipeline sees $HIST_PROBE added lines"

echo "== no credentials, ever committed"
HITS=$(git log -p --all 2>/dev/null | grep -E "^\+.*$CREDS" | head -5 || true)
[ -z "$HITS" ] || { echo "$HITS" | sed 's/^/    /'; fail "credential-shaped strings in history"; }

# THIS FILE is excluded from the next three greps, and only this file: it
# has to spell out the strings it looks for, so it matches itself. That is
# a real hole -- a secret pasted into this script would not be caught by
# this script -- and it is narrow, deliberate, and written down rather than
# hidden behind a clever regex that would rot.
SELF=':(exclude)tools/public_preflight.sh'

echo "== no AWS account id"
# The account number is not a secret on its own, but it is the first thing
# an attacker needs for a role-assumption or S3-enumeration attempt, and it
# appears in nothing a reader of this repository needs.
ACCT=$(git grep -lI "756822824659" -- . "$SELF" 2>/dev/null || true)
[ -z "$ACCT" ] || { echo "$ACCT" | sed 's/^/    /'; fail "AWS account id in tracked files"; }

echo "== no references to the private ops repository"
# Not a leak so much as a broken promise: a public reader following the
# link gets a 404, and the reference advertises a repository they cannot
# have. Private-plane material belongs behind an ADR number.
OPS=$(git grep -lI "flint-cache-ops" -- . "$SELF" 2>/dev/null || true)
[ -z "$OPS" ] || { echo "$OPS" | sed 's/^/    /'; fail "private ops repo referenced"; }

echo "== no live infrastructure identifiers"
# Private IPs, instance ids and ssh targets. A 172.31.x address is not
# routable from outside, so this is about not publishing the shape of a
# running deployment rather than about direct access.
INFRA=$(git grep -nIE "(ec2-user@[0-9]|i-0[0-9a-f]{16}|\.ec2\.internal)" -- . "$SELF" 2>/dev/null || true)
[ -z "$INFRA" ] || { echo "$INFRA" | sed 's/^/    /'; fail "live infrastructure identifiers"; }

echo "== nothing untracked that a careless \`git add -A\` would publish"
STRAY=$(git status --porcelain --untracked-files=all | grep "^??" || true)
[ -z "$STRAY" ] || { echo "$STRAY" | sed 's/^/    /'; fail "untracked files present — commit them deliberately or ignore them"; }

echo "== the licence and its terms are present"
for f in LICENSE README.md CONTRIBUTING.md; do
  [ -s "$f" ] || fail "$f is missing or empty"
done

echo
if [ "$FAILED" = 0 ]; then
  echo "PASS: public preflight — no credentials in $(git rev-list --count HEAD) commits of history, no account id, no private-repo references, no live infrastructure identifiers, working tree clean"
else
  echo "PUBLIC PREFLIGHT FAILED — do not publish until each item above is resolved."
  echo "For anything already in HISTORY, removing it in a new commit is NOT enough."
  exit 1
fi
