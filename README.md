# nar-tester

A CLI that loads a processor out of a NiFi Archive (`.nar`), runs it against
an input file using NiFi's own unit-testing harness (`nifi-mock`), and writes
the resulting flowfile content + attributes to disk per relationship.

## How it works

- The `.nar` is unpacked, and a `URLClassLoader` is built from the jars under
  `META-INF/bundled-dependencies/`, with the launcher's classloader (which has
  `nifi-api` / `nifi-mock` on it) as parent. This mirrors — in simplified
  form — how NiFi's own `NarClassLoader` works, without depending on NiFi's
  internal `nifi-nar-utils` classes, which aren't a stable public API. See
  [CLASSLOADING.md](CLASSLOADING.md) for why, and the pros/cons of that
  trade-off.
- The processor class is loaded from that classloader, instantiated, and
  handed to `org.apache.nifi.util.TestRunners.newTestRunner(...)`.
- Properties and one input flowfile (content + attributes) are set on the
  `TestRunner`, `run()` is called, and flowfiles are pulled back out per
  relationship via `getFlowFilesForRelationship(...)`.

### Resolving cross-NAR dependencies

By default (no `--nifi-properties` / `--nar-library`), the tool loads a
**single, self-contained NAR** — one that bundles all of its own dependency
jars, which is the common case for custom processor bundles.

If your NAR depends on classes from a *different* NAR at runtime (e.g. a
processor that depends on a separate controller-service-API NAR, or a
controller service implementation shipped in its own bundle), point at a
real NiFi instance's library directories instead:

```bash
java -jar target/nar-tester.jar \
  --nar path/to/my-processor.nar \
  --nifi-properties /opt/nifi/current \
  --input path/to/input.json \
  --config path/to/config.yaml
```

`--nifi-properties` accepts either a `nifi.properties` file directly, or a
NiFi home directory (it'll look for `conf/nifi.properties` under it). It's
parsed for the same properties a running NiFi instance uses to locate NARs:

- `nifi.nar.library.directory` (default `./lib`)
- any `nifi.nar.library.directory.<custom>` entries
- `nifi.nar.library.autoload.directory` (default `./extensions`)

— all resolved relative to the NiFi home directory, same as NiFi itself.
You can also point at arbitrary extra folders directly with `--nar-library`
(repeatable), with or without `--nifi-properties`:

```bash
java -jar target/nar-tester.jar \
  --nar path/to/my-processor.nar \
  --nar-library /opt/nifi/current/lib \
  --nar-library /opt/nifi/current/extensions \
  ...
```

Every `.nar` found across those directories is registered (cheaply — just
its manifest is read, nothing is unpacked yet), then the tool reads each
NAR's `Nar-Id` / `Nar-Dependency-Id` manifest attributes (the same contract
`nifi-nar-maven-plugin` writes and NiFi's own class-loading relies on) to
build the real dependency graph, and lazily builds the parent-chained
classloader for your target NAR — unpacking only the NARs actually on its
dependency chain. This is the same mechanism NiFi's own `NarClassLoaders`
uses at startup, reimplemented against the stable manifest contract instead
of NiFi's internal (and not-meant-to-be-public) implementation classes.

A NAR explicitly passed via `--nar` or `controllerServices[].nar` always
takes precedence over a same-`Nar-Id` bundle found in a scanned library
directory — handy for testing an in-progress build against dependencies
pulled from an otherwise-real NiFi install.

## Build

Requires JDK 21+ and Maven.

```bash
mvn package
```

This produces a single executable jar at `target/nar-tester.jar` (dependencies
are shaded in).

> The `nifi.version` property in `pom.xml` defaults to `2.11.0`. Set it to
> whatever NiFi 2.x API version your target NAR was compiled against.

## Usage

```bash
java -jar target/nar-tester.jar \
  --nar path/to/my-processor.nar \
  --input path/to/input.json \
  --config path/to/config.yaml \
  --output ./output
```

Add `--nifi-properties` and/or `--nar-library` if your NAR needs classes
from other NARs — see "Resolving cross-NAR dependencies" below.

`--nar` can be omitted entirely if `--nifi-properties` and/or `--nar-library`
are given: the NAR providing the config's `processorClass` is auto-discovered
by scanning those library directories (reading each NAR's
`META-INF/docs/extension-manifest.xml`, with a fallback to scanning its
bundled dependency jars directly). This fails with a clear error if no NAR
(or more than one) provides that class.

```bash
java -jar target/nar-tester.jar \
  --config path/to/config.yaml \
  --input path/to/input.json \
  --nifi-properties /opt/nifi/current
```

See `test.sh` at the repo root for a complete, runnable example against the
NiFi instance checked into `nifi/`: `LookupAttribute` (from `nifi-standard-nar`)
looks up a value via a `SimpleKeyValueLookupService` controller service that
lives in a *different* NAR (`nifi-lookup-services-nar`), exercising both NAR
auto-discovery and cross-NAR controller-service resolution with no NAR of
your own required. Its config is `examples/lookup-attribute-config.yaml`.
A simpler single-processor example (no controller services) is
`examples/update-attribute-config.yaml`.

### Config file

See `examples/lookup-attribute-config.yaml` for a real, runnable example, or
`examples/example-config.yaml` for an annotated, abstract reference:

```yaml
processorClass: org.apache.nifi.processors.example.MyProcessor
properties:
  Some Property: some-value
  Another Property: "${literal-value}"
  DBCP Connection Pool: my-service   # value = a controller service identifier below
attributes:
  filename: input.json
  mime.type: application/json
relationships:
  - success
  - failure
runIterations: 1
controllerServices:
  - identifier: my-service
    implementationClass: org.apache.nifi.example.MyControllerServiceImpl
    properties:
      Some Service Property: value
```

- `processorClass` — fully-qualified class name of the processor to test.
- `properties` — keyed by the property's display name (as shown in the NiFi
  UI / defined via `PropertyDescriptor.Builder().name(...)`). A property
  that identifies a controller service takes that service's `identifier`
  (from `controllerServices` below) as its value.
- `attributes` — attributes to set on the single input flowfile before
  running. `filename` defaults to the input file's name if omitted.
- `relationships` — which relationships to capture output from. Omit or
  leave empty to capture all relationships the processor declares.
- `runIterations` — how many times to call `onTrigger` (default `1`).
- `controllerServices` — each is added to the `TestRunner`, configured via
  `properties`, and enabled — all *before* the processor runs — so any
  processor property referencing a service's `identifier` validates
  correctly. Each service is loaded from the same NAR as the processor by
  default; set `nar: path/to/service.nar` on an entry to load it from a
  different NAR instead (requires `--nifi-properties` or `--nar-library`).

### Output

For each captured relationship, `output/<relationship>/` contains one
`<n>.content` file (raw flowfile bytes) and one `<n>.attributes.json` file
(flowfile attributes) per flowfile routed there, and console output prints a
per-relationship flowfile count.

Before that, console output also prints every `TRACE`/`DEBUG`/`INFO`/`WARN`/
`ERROR` message the processor logged during the run (via `ComponentLog`),
grouped by level; empty levels are omitted.

## `deps`: listing NAR/jar dependencies without running anything

To see which NARs (and bundled jars) a set of processor and/or controller
service classes would actually pull in — without running a test — use the
`deps` subcommand:

```bash
java -jar target/nar-tester.jar deps \
  --processor org.apache.nifi.processors.standard.LookupAttribute \
  --controller-service org.apache.nifi.lookup.SimpleKeyValueLookupService \
  --nifi-properties nifi/conf/nifi.properties
```

`--processor` and `--controller-service` are repeatable. You can instead (or
additionally) point `--config` at a nar-tester config file, which pulls in
its `processorClass` and every `controllerServices[].implementationClass`:

```bash
java -jar target/nar-tester.jar deps \
  --config examples/lookup-attribute-config.yaml \
  --nifi-properties nifi/conf/nifi.properties
```

Each requested class is located the same way `--nar` auto-discovery works
(via `NarLocator`), then its full `Nar-Dependency-Id` chain is resolved. The
output lists only the NARs that chain actually touches, and the bundled
dependency jars loaded from each — the same small subset of everything under
`--nifi-properties` / `--nar-library` that a real test run would load.

Each NAR is annotated with its Maven coordinates from its manifest
(`Nar-Group:Nar-Id:Nar-Version`), and each jar with its own coordinates read
from its embedded `META-INF/maven/<groupId>/<artifactId>/pom.properties`
(what maven-jar-plugin writes into a jar by default). A jar built without
that descriptor (rare, but it happens) is listed by file name alone:

```
NARs needed (4), 201 jar(s) total:
  - nifi-standard-nar (org.apache.nifi:nifi-standard-nar:2.12.0) [nifi/lib/nifi-standard-nar-2.12.0.nar] -- 126 jar(s)
      commons-lang3-3.20.0.jar (org.apache.commons:commons-lang3:3.20.0)
      protobuf-java-4.36.1.jar
      ...
```

## Project layout

```
pom.xml
src/main/java/com/example/nartester/
  Main.java                     CLI entry point (picocli); the top-level "run" behavior
  DepsCommand.java               "deps" subcommand: list NAR/jar dependencies without running anything
  TestConfig.java               YAML/JSON config model
  ControllerServiceConfig.java  Config model for one controller service
  NarClassLoaderBuilder.java    Unpacks a NAR, builds a classloader from its jars
  NiFiPropertiesLoader.java     Resolves NAR library directories from nifi.properties
  NarBundle.java                Manifest coordinates + lazily-built classloader for one NAR
  NarBundleGraph.java           Builds the Nar-Id / Nar-Dependency-Id dependency graph
  NarLocator.java                Finds which NAR in a set of directories provides a given processor class
  LoadedNarInfo.java              Reports one NAR + its jars once its classloader is actually built
  JarInfo.java                    One bundled jar's file name + Maven coordinates (from its pom.properties)
  ProcessorTestExecutor.java    Drives the processor via nifi-mock's TestRunner
  FileUtil.java                 Shared temp-directory cleanup helper
examples/example-config.yaml             Annotated, abstract reference config
examples/lookup-attribute-config.yaml    Working example with a controller service (see test.sh)
examples/update-attribute-config.yaml    Working example, no controller service
examples/example-input.txt
test.sh                                  Runnable end-to-end example
```
