# flow-tester

[nf3t.com](https://nf3t.com)

A CLI built for CI/CD: it runs a NiFi flow -- exported as a "flow
definition" JSON straight from the NiFi UI -- against one or more input
flowfiles, loading every processor and controller service it needs out of
NiFi Archives (`.nar`), using NiFi's own unit-testing harness (`nifi-mock`)
under the hood. Point it at the flow definition(s) in a repo that builds
custom NiFi processors, and it exits non-zero the moment one throws,
fails to load, or can't be wired up -- catching a broken processor/flow
before it ships, without needing a real NiFi instance running anywhere. It
also writes the resulting flowfile content + attributes to disk for whatever
comes out the other end of the flow, for inspection or golden-file diffing.

## How it works

- **Getting a flow definition JSON.** In the NiFi UI, right-click a process
  group and choose **Download Flow Definition** (or, for a running instance,
  `GET /nifi-api/process-groups/{id}/download`). This downloads exactly the
  `org.apache.nifi.flow.VersionedExternalFlow` / `VersionedProcessGroup` JSON
  that `flow-tester` reads -- no dependency beyond `nifi-api`, which
  `flow-tester` already depends on. See `examples/flow-lookup-attribute.json`
  for what one looks like, and "Flow definition schema" below for editor
  validation/autocomplete while hand-editing one.
- **Flattening the flow.** `FlowGraph` recursively collects every processor,
  port, controller service, and connection across the whole (possibly
  nested) process group tree -- component identifiers are unique UUIDs
  across the document, so nesting doesn't matter once everything's
  collected. Ports and funnels don't run any code, so they're spliced out of
  the routing graph: a port/funnel with nothing feeding it becomes a flow
  entry point, one with nowhere to go becomes a labeled output sink,
  otherwise it's replaced by a direct edge from whatever feeds it to
  whatever it feeds.
- **Loading each component.** Every processor/controller service carries a
  `Bundle` (NAR group/artifact/version) in the JSON. `NarLocator` finds the
  matching `.nar` across your `--nifi-properties` / `--nar-library`
  directories (falling back to scanning for the class itself if the bundle
  is missing or unmatched), and `NarBundleGraph` builds the parent-chained
  classloader for it the same way it always has -- see
  [CLASSLOADING.md](CLASSLOADING.md) for how that works and its trade-offs.
- **Resolving controller services.** `ControllerServiceResolver` walks each
  processor's properties, matches values against the flow's own controller
  services, and recurses (a service can reference another service) -- so
  nothing needs to be hand-declared.
- **Running it.** `FlowTestExecutor` instantiates one `nifi-mock`
  `TestRunner` per processor, wires up its resolved controller services,
  and runs the whole pipeline in topological order: every flowfile unpacked
  from `--input` is delivered to every flow entry point (or just
  `--input-port`, if given), and each processor's output is forwarded along
  the flow's connections to whatever is downstream. Anything that lands on
  an unconnected, non-auto-terminated relationship (or an output port) is
  captured as a result.

### Resolving NARs

Every processor and controller service in the flow needs its own NAR, so
`--nifi-properties` and/or `--nar-library` are effectively required:

```bash
java -jar target/flow-tester.jar \
  --flow path/to/flow-definition.json \
  --input path/to/input.json \
  --nifi-properties /opt/nifi/current \
  --output ./output
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
java -jar target/flow-tester.jar \
  --flow path/to/flow-definition.json \
  --input path/to/input.json \
  --nar-library /opt/nifi/current/lib \
  --nar-library /opt/nifi/current/extensions
```

Every `.nar` found across those directories is registered (cheaply — just
its manifest is read, nothing is unpacked yet). For each processor/service
in the flow, `NarLocator.findNarByBundle` first tries to match the flow's
recorded `Bundle` (group/artifact/version) against each NAR's manifest,
falling back to a group+artifact match (with a warning) if the exact
version isn't present locally, and finally to a class-presence scan
(`findNarsProviding`, reading `META-INF/docs/extension-manifest.xml`) if the
bundle is missing or unmatched entirely. Once resolved, `NarBundleGraph`
reads each NAR's `Nar-Id` / `Nar-Dependency-Id` manifest attributes to build
the real cross-NAR dependency graph, and lazily builds the parent-chained
classloader for it — unpacking only the NARs actually on its dependency
chain. This is the same mechanism NiFi's own `NarClassLoaders` uses at
startup, reimplemented against the stable manifest contract instead of
NiFi's internal (not-meant-to-be-public) implementation classes.

### Flow definition schema

```bash
./dump-flow-schema.sh flow-definition.schema.json
```

Generates the JSON Schema for a `--flow` file, straight from
`org.apache.nifi.flow.VersionedExternalFlow` via reflection (victools
`jsonschema-generator` + its Jackson module) -- so it always matches
whatever `nifi.version` this build was actually compiled against, with
nothing hand-maintained to drift. Handy for validation/autocomplete while
hand-authoring a flow definition (as the ones in `examples/` are) rather
than downloading one from a running NiFi instance. Same idea as the
`schema` subcommand for `--require-output` files, just for the `--flow`
input instead -- either add `"$schema": "<path>"` to the flow definition
file itself, or map it in your editor's schema settings.

## Build

Requires JDK 21+ and Maven.

```bash
mvn package
```

This produces a single executable jar at `target/flow-tester.jar` (dependencies
are shaded in).

> The `nifi.version` property in `pom.xml` defaults to `2.11.0`. Set it to
> whatever NiFi 2.x API version your target flow/NARs were built against.

## Usage

```bash
java -jar target/flow-tester.jar \
  --flow path/to/flow-definition.json \
  --input path/to/input.flowfile.pkg \
  --attribute mime.type=application/json \
  --nifi-properties /opt/nifi/current \
  --output ./output
```

- `--flow` — the flow definition JSON (see "How it works" above).
- `--input` — a **FlowFile Package V3** file: NiFi's own format for bundling
  one or more flowfiles (attributes + content each) into a single file. See
  "Input format" below for how to produce one. Every flowfile it contains is
  delivered to every flow entry point (a processor or port with no incoming
  connection) -- or just `--input-port`, if given.
- `--attribute KEY=VALUE` — repeatable; sets an attribute on *every* input
  flowfile, overriding whatever the package itself carries for that key.
- `--input-port <name-or-id>` — optional; deliver `--input` only to this
  input port instead of broadcasting it to every flow entry point. The port
  must have no incoming connection within the flow (i.e. actually be an
  entry point) -- handy when a flow has more than one entry point and you
  only want to exercise one of them.
- `--nifi-properties` / `--nar-library` — see "Resolving NARs" above.
- `--output` — where captured output flowfiles are written (default `output`).
- `--require-output <file.json>` — optional; assert the captured output
  matches expectations, exiting non-zero if it doesn't. See "Output
  verification" below.

### Input format

`--input` is read with `org.apache.nifi.util.FlowFileUnpackagerV3`
(`org.apache.nifi:nifi-flowfile-packager`, a small standalone module --
already a `pom.xml` dependency, no NiFi install needed to read it), the same
format NiFi itself uses to bundle flowfiles. You can produce one:

- **From a real flow's data**, with a `MergeContent` processor configured
  with Merge Format `FlowFile Stream, v3` -- route whatever flowfiles you
  want as your test input into it and grab its output.
- **Programmatically**, with `org.apache.nifi.util.FlowFilePackagerV3`
  directly:
  ```java
  FlowFilePackagerV3 packager = new FlowFilePackagerV3();
  try (OutputStream out = Files.newOutputStream(Path.of("input.flowfile.pkg"))) {
      packager.packageFlowFile(contentInputStream, out, attributesMap, contentLength);
      // call again on the same OutputStream to pack additional flowfiles into one file
  }
  ```

A package with more than one flowfile record delivers all of them; each one
is enqueued separately at every flow entry point.

See `test.sh` at the repo root for a complete, runnable example against the
NiFi instance checked into `nifi/`: `UpdateAttribute` feeds `LookupAttribute`
(both from `nifi-standard-nar`/`nifi-update-attribute-nar`), which looks up a
value via a `SimpleKeyValueLookupService` controller service that lives in a
*different* NAR (`nifi-lookup-services-nar`) — exercising bundle-based NAR
resolution and cross-NAR controller-service resolution automatically, driven
entirely by `examples/flow-lookup-attribute.json`. A simpler single-processor
example (no controller service, no connections) is
`examples/flow-update-attribute.json`.

> The example flow JSONs in `examples/` are hand-authored stand-ins for what
> a real "Download Flow Definition" export looks like (built by inspecting
> the relevant NAR manifests/extension docs directly), since building and
> exporting one from a live NiFi UI isn't something this environment can do.
> A real download will additionally carry `propertyDescriptors`,
> `position`, etc. -- `flow-tester` ignores whatever it doesn't need.
> `examples/example-input.flowfile.pkg` was generated with
> `FlowFilePackagerV3` directly, packaging `filename: example-input.txt`
> with the text "Hello from nar-tester!".

### Output

**If the flow declares at least one output port** (anywhere, at any nesting
level), only flowfiles that actually reach an output port are captured;
every other unconnected, non-auto-terminated relationship is dropped
instead, with a warning printed listing what got dropped and how many
flowfiles were lost — so a well-designed flow that marks its real outputs
with output ports doesn't get cluttered with incidental dangling
relationships (e.g. a `failure` path nobody wired up). **If the flow has no
output port at all**, every unconnected, non-auto-terminated relationship is
captured, same as before.

For each captured output, `output/<label>/` contains one `<n>.content` file
(raw flowfile bytes) and one `<n>.attributes.json` file (flowfile
attributes) per flowfile that landed there, and console output prints a
per-output flowfile count. A relationship's label is
`<processor-name>-<relationship-name>` (processor names are deduplicated
with a short id suffix if two processors in the flow share a name); a port's
label is its own name.

Before that, console output also prints every `TRACE`/`DEBUG`/`INFO`/`WARN`/
`ERROR` message logged by any processor during the run (via `ComponentLog`),
prefixed with the logging processor's name.

### Output verification

`--require-output <file.json>` asserts the captured output matches
expectations, so a broken flow that runs without throwing but produces the
wrong result still fails your CI pipeline. It's a JSON file mapping output
label (the same `<processor-name>-<relationship-name>`/port-name label
`output/` uses) to the exact list of flowfiles expected there:

```json
{
  "$schema": "./flow-tester-require-output.schema.json",
  "outputs": {
    "Lookup_Attribute-matched": [
      {
        "attributes": {
          "greeting": "Earth"
        }
      }
    ]
  }
}
```

- Only labels you mention are checked at all; an output the flow produces
  that isn't in the file is ignored.
- The flowfile **count** for a mentioned label must match exactly.
- `attributes` only checks the keys you list -- leave out anything dynamic
  (`uuid`, `path`, timestamps, ...) rather than trying to predict it.
- `content` (an inline UTF-8 string) or `contentFile` (a path, resolved
  relative to the require-output file itself, to a file with the exact
  expected bytes) are both optional and mutually exclusive; omit both to
  only check attributes (or nothing, to just assert a flowfile exists in
  that slot).

A mismatch prints every failure found (missing/extra flowfiles, which
attribute differed and how, content length mismatches) and exits `3`.

Get the JSON Schema for this file with the `schema` subcommand, so an editor
can validate/autocomplete it:

```bash
java -jar target/flow-tester.jar schema --output require-output.schema.json
```

Either reference it from the file itself with the `$schema` property shown
above (VS Code's built-in JSON support picks that up automatically, no
extra configuration needed), or map it in your workspace/user
`settings.json` without touching the require-output files at all:

```json
{
  "json.schemas": [
    {
      "fileMatch": ["**/require-output*.json"],
      "url": "./require-output.schema.json"
    }
  ]
}
```

## CI/CD usage

Exit codes are what a pipeline should key off of: `0` means the flow ran to
completion without any processor throwing, without a wiring/NAR resolution
failure, and (if `--require-output` was given) the output matched; `1` means
it threw or failed to load/wire up (`Class.forName`, NAR resolution,
controller service instantiation, etc. -- see the exception printed to
stderr); `2` means the invocation itself was invalid (missing/bad CLI
arguments); `3` means it ran cleanly but `--require-output` didn't match
(see "Output verification" above).

Minimal GitHub Actions example, run against every flow definition checked
into a repo's `flows/` directory, each with a matching
`flows/<name>.require-output.json`:

```yaml
- name: Test NiFi flows
  run: |
    for flow in flows/*.json; do
      java -jar flow-tester.jar \
        --flow "$flow" \
        --input test-input.flowfile.pkg \
        --nifi-properties /opt/nifi \
        --require-output "${flow%.json}.require-output.json" \
        --output "output/$(basename "$flow" .json)"
    done
```

Any non-zero exit from that loop fails the job.

### Known limitations

- **Flows must be acyclic.** A connection that loops back to an
  already-visited processor is rejected with a clear error naming the
  components involved -- retry loops aren't simulated.
- **Remote process groups aren't simulated.** A connection into/out of a
  remote input/output port is captured as a `remote-<name>` output sink
  rather than actually attempting site-to-site transfer.
- **Shared controller services may `@OnEnabled` more than once.** A
  controller service referenced by multiple processors is a single Java
  instance, registered on each processor's own `TestRunner` -- each
  registration enables it independently, so a stateful service's
  `@OnEnabled` can run more than once.

## `deps`: listing NAR/jar dependencies without running anything

To see which NARs (and bundled jars) a flow -- or an explicit set of
processor/controller-service classes -- would actually pull in, without
running a test, use the `deps` subcommand:

```bash
java -jar target/flow-tester.jar deps \
  --flow examples/flow-lookup-attribute.json \
  --nifi-properties nifi/conf/nifi.properties
```

`--processor` and `--controller-service` (both repeatable) can be used
instead of, or alongside, `--flow`:

```bash
java -jar target/flow-tester.jar deps \
  --processor org.apache.nifi.processors.standard.LookupAttribute \
  --controller-service org.apache.nifi.lookup.SimpleKeyValueLookupService \
  --nifi-properties nifi/conf/nifi.properties
```

Each class is located the same way the main command resolves NARs (bundle
match first, falling back to `NarLocator`'s class scan for anything given
directly via `--processor`/`--controller-service`, which carries no bundle
info), then its full `Nar-Dependency-Id` chain is resolved. The output lists
only the NARs that chain actually touches, and the bundled dependency jars
loaded from each — the same small subset of everything under
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
src/main/java/com/nf3t/flowtester/
  Main.java                       CLI entry point (picocli); the top-level "run" behavior
  DepsCommand.java                "deps" subcommand: list NAR/jar dependencies without running anything
  SchemaCommand.java              "schema" subcommand: print the --require-output JSON Schema
  FlowSchemaCommand.java          "flow-schema" subcommand: generate the --flow JSON Schema via reflection
  FlowDefinitionLoader.java       Reads a flow definition JSON into org.apache.nifi.flow.VersionedProcessGroup
  FlowFilePackageReader.java      Reads --input as a FlowFile Package V3 file (one or more flowfiles)
  FlowGraph.java                  Flattens a (nested) process group into one routing graph, splicing out ports/funnels
  ControllerServiceResolver.java  Finds every controller service a processor/service transitively depends on
  FlowTestExecutor.java           Runs the whole flow: one TestRunner per processor, wired and driven per FlowGraph
  RequireOutputSpec.java          Model for a --require-output JSON file
  ExpectedFlowFile.java           Model for one expected flowfile within a --require-output file
  ExpectedOutputLoader.java       Reads a --require-output JSON file into a RequireOutputSpec
  OutputVerifier.java             Checks captured output against a RequireOutputSpec
  NarClassLoaderBuilder.java      Unpacks a NAR, builds a classloader from its jars
  NiFiPropertiesLoader.java       Resolves NAR library directories from nifi.properties
  NarBundle.java                  Manifest coordinates + lazily-built classloader for one NAR
  NarBundleGraph.java             Builds the Nar-Id / Nar-Dependency-Id dependency graph
  NarLocator.java                 Finds which NAR provides a given extension class or Bundle
  LoadedNarInfo.java              Reports one NAR + its jars once its classloader is actually built
  JarInfo.java                    One bundled jar's file name + Maven coordinates (from its pom.properties)
  FileUtil.java                   Shared temp-directory cleanup helper
src/main/resources/require-output.schema.json    Bundled JSON Schema served by the "schema" subcommand
examples/flow-lookup-attribute.json    Two-processor flow with a cross-NAR controller service (see test.sh)
examples/flow-update-attribute.json    Single-processor flow, no controller service
examples/example-input.flowfile.pkg    FlowFile Package V3 input (see "Input format" above)
examples/require-output.json           --require-output example matching flow-lookup-attribute.json (see test.sh)
examples/flow-tester-require-output.schema.json    Generated by `schema`, referenced from require-output.json
test.sh                                Runnable end-to-end example
deps.sh                                Runnable `deps` example (see the deps section above)
dump-flow-schema.sh                    Runnable `flow-schema` example (see "Flow definition schema" above)
```
