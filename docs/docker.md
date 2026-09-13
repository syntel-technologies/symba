# Run Symba with Docker

Symba uses the Docker Hub namespace `synteltechnologies`, with an engine (`symba`),
operator console (`symba-frontend`), and
migration runner (`symba-flyway`). PostgreSQL 18 is required. Redis is optional.
Workers run separately using the [Python SDK](https://github.com/syntel-technologies/symba-sdk-python).

Docker Hub publication becomes available after the publisher completes the setup
below and the first configured release passes. Existing releases may not include
the new quickstart asset. A release tag alone does not prove publication succeeded.

## Start the complete local stack

Download `compose.quickstart.yml` from a successfully published release:

```sh
curl -fL https://github.com/syntel-technologies/symba/releases/latest/download/compose.quickstart.yml -o compose.quickstart.yml
docker compose -f compose.quickstart.yml up -d --wait
```

No Git checkout or image build is required. The release asset contains immutable
image digests and the matching release identity. Open http://localhost:8080 and
enter `symba-local-dev-token`. HTTP is on port 7300; workers connect on gRPC port
7233 with the same token and tenant `default`.

```sh
SYMBA_TOKEN=symba-local-dev-token
curl --fail http://localhost:7300/v1/jobs \
  -H "Authorization: Bearer $SYMBA_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"tenant":"default","specs":[{"task_name":"demo.echo","payload":{"hello":"world"}}]}'
```

Jobs remain queued until a worker supporting `demo.echo` connects. The quickstart
uses development credentials and binds all published ports to `127.0.0.1`.
Postgres has no host port. Use the [production configuration](configuration.md)
and existing production deployment overlay for routable deployments, TLS, and
separate database identities; this file is a standalone local evaluation stack.

Stop with `docker compose -f compose.quickstart.yml down`. This keeps the database
volume. Adding `--volumes` deletes the local jobs and database. To change ports,
set `ENGINE_HTTP_HOST_PORT`, `ENGINE_GRPC_HOST_PORT`, or `UI_HOST_PORT`.

From a checkout, the template requires explicit images and version:

```sh
export SYMBA_IMAGE_PREFIX=docker.io/synteltechnologies
export SYMBA_VERSION=vX.Y.Z # replace with a successfully published version
docker compose -f compose.quickstart.yml up -d --wait
```

`ghcr.io/syntel-technologies` is also supported as the image prefix. All three
images must come from the same release. Version tags include `v`. After release
smoke checks pass, the highest stable release also receives a floating `latest`
alias, so an untagged `docker pull synteltechnologies/symba` works. Keep version
or digest pins in application deployments; `latest` changes between releases.

## Run the engine against an existing PostgreSQL 18 database

The database must be reachable from the container. `localhost` inside a container
refers to that container; on Docker Desktop, `host.docker.internal` reaches the
host. Use your database's Docker network and service name for containerized PG.

Set `SYMBA_IMAGE_PREFIX` and `SYMBA_VERSION` as above. Create `migration.env`
locally (keep it out of Git):

```dotenv
FLYWAY_CONFIG_FILES=/opt/symba/flyway/flyway.toml
FLYWAY_URL=jdbc:postgresql://host.docker.internal:5432/symba
FLYWAY_USER=symba
FLYWAY_PASSWORD=YOUR_DATABASE_PASSWORD
```

Apply migrations before starting the matching engine:

```sh
docker run --rm --env-file migration.env \
  -e KNOR_RELEASE_ID="$SYMBA_VERSION" \
  -e KNOR_RELEASE_MANIFEST_SHA256=unreleased \
  "$SYMBA_IMAGE_PREFIX/symba-flyway:$SYMBA_VERSION" migrate
```

Create `engine.env` locally:

```dotenv
SYMBA_POSTGRES__HOST=host.docker.internal
SYMBA_POSTGRES__USER=symba
SYMBA_POSTGRES__PASSWORD=YOUR_DATABASE_PASSWORD
SYMBA_POSTGRES__DATABASE=symba
SYMBA_AUTH__MODE=token
SYMBA_AUTH__TOKENS={"YOUR_RANDOM_TOKEN":"default"}
```

Then start the engine:

```sh
docker run -d --name symba \
  -p 127.0.0.1:7300:7300 -p 127.0.0.1:7233:7233 \
  --env-file engine.env \
  -e KNOR_RELEASE_ID="$SYMBA_VERSION" \
  -e KNOR_RELEASE_MANIFEST_SHA256=unreleased \
  "$SYMBA_IMAGE_PREFIX/symba:$SYMBA_VERSION"
```

The release admission variables are required by the current image entrypoints.
Standalone GitHub releases use the `unreleased` manifest marker; coordinated
production bundles require their actual manifest hash and final admission checks.
Do not disable those production checks when using a standalone image.

## Publisher setup: Docker Hub and GitHub Actions

1. Create and verify your [Docker account](https://app.docker.com/signup). For
   company work, use a professional email. Your Docker ID is an individual login;
   a company organization is a separate namespace. Choose the ID carefully:
   Docker does not allow renaming it.
2. Select the owning namespace. A free individual account supports public
   repositories. Docker's current organization creation flow requires Team or
   Business; an organization can be added if shared company ownership is needed.
3. In [Docker Hub](https://hub.docker.com/repository/create), create three **public**
   repositories under that namespace: `symba`, `symba-frontend`, `symba-flyway`.
   Public visibility lets users pull without signing in, subject to Docker limits.
4. In Docker account settings, create a personal access token for
   `symba-github-actions` with Read and Write permissions, an expiry, and no Delete
   permission. Store it directly in GitHub; never paste it into chat or commit it.
5. In [GitHub Actions settings](https://github.com/syntel-technologies/symba/settings/secrets/actions),
   add secret `DOCKERHUB_TOKEN`, variable `DOCKERHUB_USERNAME` (the token owner's
   Docker ID), and variable `DOCKERHUB_NAMESPACE` (the owning username or
   organization). The token owner must be able to push all three repositories.
6. Merge the publishing change through the normal `dev` → `main` review flow and
   release a new version. The existing CI and performance gates remain required.
   Alternatively, run **Publish existing release to Docker Hub** with `v0.1.0`
   (or another successfully published release). This copies the validated images
   without rebuilding them and adds the standalone quickstart after a smoke test.
   Do not move an already published tag to add Docker Hub support.
7. Wait for **Release** to pass, inspect the image tags on Docker Hub, and download
   the attached quickstart. Verify an unauthenticated pull on a clean machine and
   both `linux/amd64` and `linux/arm64` in the published manifest.

Docker Hub hosting is separate from the curated Docker Official Images program.
Regular publication uses `<namespace>/symba`; obtaining the bare `symba` name
requires acceptance into that program.

References: [account setup](https://docs.docker.com/accounts/individual/create-account/),
[public repository limits](https://docs.docker.com/docker-hub/usage/),
[organization setup](https://docs.docker.com/accounts/organization/setup/orgs/),
[Docker with GitHub Actions](https://docs.docker.com/guides/gha/),
[Official Images](https://docs.docker.com/docker-hub/repos/manage/trusted-content/official-images/).
