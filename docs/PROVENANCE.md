# Source provenance

Forge was extracted from the project owner's existing Python/SQLite workflow
runtime with explicit authorization to publish a separate source-only repository.
The reviewed original module history identifies GitHub account `xingzhao-ji` as
author. This repository starts with a new history; no parent-workspace commits or
data were copied.

The [machine-readable record](PROVENANCE.json) pins original file blobs, SHA-256
digests and transformation descriptions. The original source revision is
`b7dcefad3e8ea65e09da87235fda7f42027dd34b`. Source files were matched to that
workspace's recorded tree before extraction; the original checkout was not edited.

The models, store, scheduler, dispatcher, registry, routing and API adapters retain
their existing implementation with package-import changes. Executor contracts no
longer import private app configuration. Runtime composition now uses an explicit
data directory, local task registration and a bounded lifecycle. The original
inline text parser is retained. The text-count task, command-line entry point,
packaging and integration tests are additions for standalone use.

Twenty-two original core tests are retained with import changes. One test tied to
the private application's provider catalogue is excluded along with that catalogue.
The new integration tests use synthetic data and local executors. Tests containing
provider names use fabricated fixture metrics; no live providers are invoked.

Excluded material includes all personal files, private configuration, stored
SQLite databases, media/PDF adapters, paid-provider executors, unrelated code,
third-party media, saved workflow inputs and the private Git history. Required
runtime dependencies are limited to the Python standard library. No blanket
license grant is inferred from public availability.
