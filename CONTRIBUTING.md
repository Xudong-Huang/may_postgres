# Contributing to this fork

This repository is a fork. Its integration branch is `integration/microscaler-fork`: it carries every
change made in this fork, and it is the only branch that projects depending on
the fork should consume (a dependent can follow just one branch).

- **Changes land here first.** Every fix or feature goes to `integration/microscaler-fork` through a pull
  request merged with a merge commit. Branches that dependents pin are never
  rebased or force-pushed.
- **Upstream proposals are cut separately.** Once a change is on `integration/microscaler-fork`, the
  pull request for the upstream project is cut fresh from upstream's default
  branch as a single-purpose change (one concern, one commit, following
  upstream's conventions). It is not merged from `integration/microscaler-fork`.
- **Upstream is merged in regularly.** Upstream's default branch is merged into
  `integration/microscaler-fork` so the fork stays a superset of upstream.
- **Before an upstream pull request merges or closes,** check that its content is
  on `integration/microscaler-fork` (by content, not commit hashes: upstream branches are re-cut).

`master` is kept as it was when the integration branch was created, because existing lockfiles pin its
commits; new work goes to `integration/microscaler-fork`.
