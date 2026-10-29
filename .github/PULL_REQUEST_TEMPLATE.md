<!--
Thanks for the PR. A few things to know:

1. New Snowflake features need a scoped issue first (CONTRIBUTING.md
   has the details). PRs without one will be asked to back up.
2. Feature-matrix markers in README.md don't move from 🟡 → ✅ without
   an integration test backing the claim.
3. Run the unit and integration tests locally before pushing
   (see CONTRIBUTING.md, "Running Tests Locally").
-->

## Summary

<!-- One paragraph: what does this PR do and why. Link the issue. -->

Closes #

## Changes

<!-- Bullet list of the actual changes. Group by file or by concept. -->

-
-
-

## Test plan

<!-- Show what you ran and the output. -->

- [ ] `go test -race -count=1 ./...` (unit)
- [ ] `go test -race -count=1 -tags=integration ./test/integration/` (gosnowflake-driven)
- [ ] `gofmt -l .` empty and `go vet ./...` clean
- [ ] New tests added for the changed code path

## Snowflake parity check

<!-- For any new SQL surface: link the Snowflake docs page and confirm
     the wire shape / error code / column names match. -->

- Snowflake docs link:
- Wire shape verified against:

## README / CHANGELOG

- [ ] Feature matrix in `README.md` updated (if a marker moved)
- [ ] `CHANGELOG.md` entry added under `## [Unreleased]`

## Anything reviewers should know

<!-- Tricky parts of the diff, follow-up issues, intentional scope cuts. -->
