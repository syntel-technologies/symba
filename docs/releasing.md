# Releases and repository workflow

The canonical repository is https://github.com/syntel-technologies/symba. Use `origin` for Syntel. Development goes to `dev`; `main` only changes through a reviewed PR. Published history and `v*` tags must never be rewritten.

## Normal development

1. Branch from updated `dev`, make a focused change and open a PR back to `dev`. Use a meaningful Conventional Commit title: `fix: reconnect an idle worker`, `feat: add task capabilities`, or `feat!: change a public contract`.
2. Squash short-lived feature PRs so their reviewed titles become meaningful commits. Promote `dev` to `main` with a **merge commit**, preserving the long-lived branch ancestry. The main rules allow merge commits only; short-lived feature PRs into dev can still use squash. Never repeatedly squash `dev` into `main`.
3. The `main` rules require a PR, resolved review conversations, passing checks on an up-to-date branch, and no blocking CodeQL findings. Required approvals are **zero**: the owner can review and merge their own PR once checks pass; GitHub does not permit approving your own PR. Repository administrators have no bypass. `Required checks` aggregates the mandatory CI jobs so a skipped/failed dependency cannot look green. `PR title` validates the review title.
4. For commits pushed directly to `dev`, use Conventional Commit messages too. The title check cannot retroactively rename arbitrary commits inside a long-lived branch promotion. Keep changes cohesive; do not use `fixes` or `gooo`.

Existing historical commit messages are preserved. The migration commits explain the consolidated changes, and `docs/first-release-notes.md` provides a curated account of the first release. Review and add that summary to the first generated release PR; old vague commit messages cannot produce good notes automatically.

## Automated release sequence

After CI succeeds for a push to the current `main`, Release Please opens or updates a release PR with the next version, `CHANGELOG.md`, package metadata and the lockfile. An outdated CI completion cannot start a release for a newer untested main revision.

- `fix` and `perf`: patch release; `feat`: minor release; `!` / `BREAKING CHANGE:`: major release. This configuration deliberately applies these rules even before 1.0. The initial version is 0.1.0. Pure maintenance commits do not force a release.
- Review the proposed notes and compatibility impact. The bot never approves its own changes or bypasses rules. Enable auto-merge on the release PR if you want it merged after you have reviewed it and the required checks finish.
- After that PR merges and main CI passes, Release Please creates the matching `vMAJOR.MINOR.PATCH` tag and GitHub Release. The tag starts `Release`, which checks tag/package agreement, ancestry in reviewed main history and the release notes, then reruns the complete CI workflow on the **tagged commit** before publishing artifacts. Engine releases also rerun the full nightly performance suite; a green functional CI run cannot bypass a failing throughput/latency gate.
- Release Please updates `uv.lock` along with the package version. Its TOML updater represents strings as tagged values; the JSONPath intentionally uses `@.name.value`. The migration dry run tested the pinned action's Release Please 17.3.0 implementation and `uv lock --check`. Revalidate this behavior when updating the action. The engine console's two npm version records and runtime version also move together. Wire protocol versions are independent compatibility declarations and are not blindly rewritten by packaging automation.
- The bot opens a `main` → `dev` synchronization PR when needed. Merge it with a merge commit before the next promotion, preserving release metadata and avoiding conflicts. It never force-pushes `dev`.

A GitHub Release can exist while its artifact workflow is still running or has failed. A release is consumable only when **Release is green and its documented artifacts are attached**. Never claim that a tag alone proves a successful publication. Fix a publishing failure through a reviewed change and a new version when code changes; do not move a published tag. Existing release assets are never silently replaced with different bytes.

## Release bot configuration and rotation

Activation was completed and verified on 13 September 2026. `Syntel Release Bot` (App ID `4930737`, installation `161378012`) has accepted Contents, Pull requests and Issues write permissions on the engine and SDK repositories. `RELEASE_APP_CLIENT_ID` is configured per repository; the organization secret `RELEASE_APP_PRIVATE_KEY` is accessible to both. [The engine preflight passed](https://github.com/syntel-technologies/symba/actions/runs/34762024565), including the single-repository scope check. A separate tag-creation rule allows only this App to create `v*` tags. The App has no bypass for main or existing tags. No initial release or artifact upload has run yet.

The steps below document setup and key rotation; they are not outstanding activation work.

Create one private GitHub App owned by `syntel-technologies`, named `Syntel Release Bot` (choose a unique slug if GitHub requires one):

- Start at https://github.com/organizations/syntel-technologies/settings/apps/new.
- Homepage: https://github.com/syntel-technologies. Disable webhooks; no callback or user authorization URL is needed.
- Repository permissions: **Contents: read/write**, **Pull requests: read/write**, **Issues: read/write** (release labels), Metadata read. No organization permissions. Install it only on `symba` and `symba-sdk-python` initially.
- Generate its private key. Put the key directly into the organization Actions secret `RELEASE_APP_PRIVATE_KEY`, restricted to these two repositories. Do not paste it into chat or commit it. Put the App **Client ID** into the organization Actions variable `RELEASE_APP_CLIENT_ID`, also restricted to these repositories. Alternatively use repository variables/secrets with the same names.
- Workflows mint short-lived installation tokens restricted to their own repository and the listed permissions. A personal admin token is not needed. Keep the existing main protection and immutable-tag rules. Once the App is installed, add a separate tag-creation ruleset for `v*` allowing only this App; give it no main or immutable-tag bypass.

Run **Release bot check** after activation (or rerun its initial dev check). It requests the exact release permissions, validates that the installation token can access only the current repository, and revokes the short-lived token at job completion. It creates no release artifacts.

Until the App variable exists, release automation records an explicit setup notice and creates no release. This is deliberate: GitHub's built-in token does not produce normal unattended downstream tag workflows, and its bot-created PR workflows can require manual approval. An installation token supports the reviewed release flow without a personal access token. See [GitHub's triggering rules](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow) and [Release Please](https://github.com/googleapis/release-please-action).

No App subscription is required. These are public repositories, so standard GitHub-hosted Actions runners and public-repository protections are available without GitHub Team. Do not add paid review bots to establish this baseline.

## Dependency and security checks

Every PR CI run also checks workflow syntax with Actionlint and scans committed history with Gitleaks. Both CLIs use pinned releases verified against fixed SHA256 digests; Gitleaks redacts secret values. These checks are included in `Required checks`.

Dependabot sends grouped compatible dependency updates to `dev` weekly; major updates remain separately reviewable. GitHub also reports known vulnerabilities and security updates on the default branch. CodeQL, secret scanning and push protection cover the public repositories once configured. The nightly engine load suite is separate from PR CI and mandatory for engine artifact publication. Never lower its thresholds merely to make it green. See [the current performance finding](performance-release-blocker.md).

## Reproducing checks

```sh
uv sync --frozen --extra dev --python 3.13
make proto
uv run --no-sync ruff check src tests tools
uv run --no-sync ruff format --check src tests tools
uv run --no-sync pyright
uv run --no-sync python tools/check_imports.py
uv run --no-sync python tools/check_openapi.py
uv run --no-sync pytest tests/unit --cov=symba.core --cov-fail-under=100 -q
uv run --no-sync pytest -m 'l2 or l3 or l5 or l6' -q
```

Database tests create a disposable PostgreSQL 18 container when no external DSN is supplied. Do not point test setup at a deployed database. The EXPLAIN gate applies the schema itself and requires a **separate fresh database**. Run frontend `npm ci`, `npm run lint`, `npm test`, `npm run build`; run Buf 1.73.0 `format --diff --exit-code proto`, `lint proto`, and `breaking proto --against 'https://github.com/syntel-technologies/symba.git#branch=main,subdir=proto'`.


## Artifacts and publication

`Release` builds amd64/arm64 images for `ghcr.io/syntel-technologies/symba`, `symba-frontend` and `symba-flyway`, each using the release tag. It attaches the SQL migration bundle, SHA256 checksums and `images.json` containing the exact image digests and git revision. Deploy by digest for reproducibility. Apply Flyway migrations before starting the matching engine; the engine does not self-migrate. GHCR packages may need their initial visibility set to public by an owner after the first publication. The engine is not published to PyPI. The SDK distribution is `syntel-symba`; its Python imports and CLI remain `symba`.


## Dependency update reviews

Dependabot proposes changes into `dev`; an open update PR is not a supported release.
Compiler/runtime protobuf packages are grouped separately from general Python
updates. Review generated-code changes and required runtime floors together; do
not disable drift or minimum-dependency checks to accept a compiler upgrade.

ESLint and its plugins update as one toolchain, as do Vite and its build plugins.
Do not use `--force` or `--legacy-peer-deps` to bypass incompatible peer ranges.
TypeScript major proposals are deferred while `openapi-typescript` declares `^5.x`;
`typescript-eslint` also currently excludes TypeScript 7. Patch/minor updates stay
enabled. Remove the scoped major ignore after both upstream tools support the new
compiler, then verify strict types, lint, API generation, frontend tests and build.

Release-contract tests verify digest pins, exact Python tool pins and agreement
between code generation and runtime dependencies. They deliberately do not freeze
historical patch versions in assertions; reviewed dependency upgrades must still
preserve reproducible builds and pass their functional checks.
