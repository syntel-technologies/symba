# First release notes — draft for maintainer review

These notes describe the consolidated code intended for the initial 0.1.0 release. They are **not an announcement that a release has shipped**. Use them to enrich the first Release Please PR; verify all claims against its final diff and CI before merging.

- Postgres-backed durable job execution, retries, cancellation, chains, dependencies, gates and event-driven resume.
- Separate engine API and operator console, with Flyway-managed migrations and versioned deployment images.
- Token/TLS configuration and safer worker failure reporting.
- Worker task capability inventory in the API and console, fairer assignment and refreshed idle registrations.
- CI for protocol compatibility, core correctness, real database behavior, UI builds and scheduled chaos/load checks.

Canonical source: https://github.com/syntel-technologies/symba. Historical Amplior commits are preserved without rewriting their authors or messages. Ongoing development and release artifacts belong to Syntel.

Before publishing, include the exact engine/SDK compatibility reference, migration instructions, known limitations and final artifact links. The initial version is pre-1.0; do not advertise universal production readiness or performance beyond the measured environment.
