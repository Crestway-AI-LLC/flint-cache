---
name: Bug report
about: Something behaves differently from what the docs say
title: ''
labels: bug
assignees: ''
---

<!--
NOT for security issues. Those go through SECURITY.md — GitHub's private
vulnerability reporting — not a public issue.
-->

## What happened, and what you expected instead

## Version

<!-- `flintctl --version`, or `flint-server --build-version`. `build unstamped`
is the expected answer for a build from source; say so if that is what you have. -->

## From a release bundle or built from source?

## Reproduction

<!-- The most useful form is a diff against tools/quickstart.sh, which stands
up a full cluster in one command — control plane, replicated pair, proxy,
failover controller. If you cannot reduce it that far, the inventory file and
the commands you ran are the next best thing. -->

## What the system said about itself

<!-- Whatever you have, in rough order of usefulness:

  tools/quickstart.sh status        (or: flintctl -f <inventory> status)
  flintctl -f <inventory> verify
  the node/proxy logs under <statedir>/logs/

A refused write usually explains itself in the reply — please paste the
server's actual sentence rather than a summary of it. -->
