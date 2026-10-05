# NAR class loading: why not NiFi's own `NarClassLoader`?

`flow-tester` needs to do the same job NiFi's framework does at startup:
turn a `.nar` file (or a chain of them) into a working classloader for a
processor or controller service. It doesn't reuse NiFi's own machinery for
this (`org.apache.nifi.nar.NarClassLoaders`, in `nifi-nar-utils`); instead
`NarClassLoaderBuilder` + `NarBundleGraph` reimplement a small, simplified
version of it. This doc explains why, and what that trade-off costs.

## What NiFi's real mechanism does

At framework startup, NiFi's `NarClassLoaders`:

- Scans every `.nar` under `lib/` and `extensions/` (plus any custom
  `nifi.nar.library.directory.*` dirs), unpacking each into a working
  directory.
- Builds one `NarClassLoader` (a `URLClassLoader`) per NAR, over its
  `META-INF/bundled-dependencies/*.jar` files, parented along a chain
  determined by the `Nar-Dependency-Id` manifest attribute the
  `nifi-nar-maven-plugin` writes into every NAR it builds.
- Roots that whole chain in the JVM's own application classloader — which,
  in a real NiFi install, already has the *flat* jars under `lib/`
  (`nifi-api`, `nifi-framework-api`, `nifi-runtime`, logging bridges, etc.
  — as opposed to the `.nar` files sitting alongside them) directly on its
  classpath, because that's how `bin/nifi.sh` launches the JVM.
- Does this as one piece of a much larger bootstrap: an `ExtensionManager`
  that indexes every discovered extension, a `FlowController` that owns the
  running flow, `StateManager`/bulletin/provenance services, and NiFi's own
  docs-generation pass that produces `META-INF/docs/extension-manifest.xml`
  for each NAR (the same file `NarLocator` reads for NAR auto-discovery and
  `deps`).

## Why not depend on it directly

1. **Not a public API.** `nifi-nar-utils` is internal to the NiFi framework
   — it isn't published as something meant to be embedded in a standalone
   tool, and nothing stops its internals from changing across NiFi releases
   without notice. Building on it would tie `flow-tester` to implementation
   details rather than a contract.
2. **Heavy dependency footprint.** Pulling in `nifi-nar-utils` (and its
   transitive graph — much of `nifi-framework-core`) just to get
   classloading would balloon this CLI's own dependencies and startup time,
   working against the point of a small, fast test tool.
3. **Deep coupling to a running instance.** The real mechanism expects a
   `FlowController`, `ExtensionManager`, and other framework services wired
   up around it. Using it standalone would mean either faking a large
   surface of that machinery, or dragging in most of an embedded NiFi
   runtime — exactly what `flow-tester` exists to avoid needing.
4. **Mismatched goal.** `flow-tester` only needs to load the handful of
   processors and controller services one flow actually uses, in isolation,
   and drive each with `nifi-mock`'s `TestRunner` — which itself is designed
   to run without a real NiFi instance. Two different "no running NiFi
   required" tools compose naturally; wiring one to depend on NiFi's full
   extension framework would not.

## What we do instead

- Read the same `Nar-Id` / `Nar-Dependency-Id` (+ `Nar-Group`/`Nar-Version`)
  manifest attributes the real mechanism keys off of — the one part of this
  that *is* a stable, documented contract (any NAR built with the standard
  Maven plugin carries it), rather than an internal Java class.
- Unpack a NAR's `META-INF/bundled-dependencies/*.jar` files into a temp
  directory and build a plain `URLClassLoader`, parented lazily along that
  same `Nar-Dependency-Id` chain — the same shape as a real `NarClassLoader`,
  built with nothing but `java.util.zip` and `java.net.URLClassLoader`.
- Since there's no "system bundle" of preloaded framework jars here, the
  root ancestor of that chain is just this CLI's own launcher classloader
  (`nifi-api` + `nifi-mock` + Jackson + picocli + slf4j from the shaded jar),
  not a running NiFi install's full `lib/` classpath.

For example, `nifi-standard-nar`'s real manifest chain is
`nifi-standard-nar → nifi-standard-shared-nar → nifi-standard-services-api-nar`
— `NarBundleGraph` walks exactly that.

## Pros of this approach

- **Lightweight.** No `nifi-framework-core` / `nifi-runtime` dependency; the
  shaded jar stays small and starts instantly.
- **Built on a stable contract.** Keys off documented manifest attributes,
  not internal implementation classes that could shift between NiFi
  versions.
- **No running instance required.** Treats a NiFi install's `lib/` /
  `extensions/` purely as a source of `.nar` files — no process, REST API,
  or framework service needs to be running.
- **Small and legible.** The whole mechanism is a couple hundred lines
  across `NarClassLoaderBuilder` and `NarBundleGraph`. A real
  `NarClassLoaders` integration would be effectively opaque without also
  embedding a large slice of NiFi's internals to read.
- **Fast iteration.** Only the NARs actually needed are resolved and
  unpacked, lazily — much cheaper than a real NiFi boot, which discovers and
  indexes every NAR under `lib/`/`extensions/` and generates docs for each.

## Cons / known gaps

- **Single-chain assumption.** Like real NiFi, each NAR gets exactly one
  classloader parent (its `Nar-Dependency-Id` target). If a component's
  actual runtime classpath in NiFi is ever assembled through anything wider
  than that simple chain, this tool won't reproduce it.
- **No system-bundle classpath.** A real NiFi instance's NAR classloaders
  ultimately root in a classloader that already has the flat jars under
  `lib/` (`nifi-api`, `nifi-framework-api`, logging bridges, `nifi-runtime`,
  etc.) on it. This tool's root is just its own minimal launcher classpath.
  In practice this has surfaced as a real gap: a couple of
  `nifi-standard-nar` processors (`LogMessage`, `LogAttribute`) fail here
  with `NoClassDefFoundError: org/eclipse/jetty/util/StringUtil`, because
  that class isn't reachable through the `Nar-Dependency-Id` chain this tool
  walks — whatever makes it available in a real running NiFi instance isn't
  something this model replicates.
- **No `ExtensionManager` validation.** There's no check that a class is a
  genuinely registered extension beyond what can be inferred from
  `META-INF/docs/extension-manifest.xml` (used only for NAR auto-discovery
  and `deps`, not for anything safety-related) or, failing that, a raw scan
  of bundled jars.
- **No instance class loading.** NiFi supports per-instance additional
  classpath resources for some processors (a property that dynamically
  extends the classpath); this tool has no equivalent.
- **Best-effort Maven coordinates.** `deps` reads each jar's own
  `pom.properties` if present — accurate for anything built by Maven's
  default jar plugin, but not a substitute for an actual dependency
  resolver, and absent on jars that don't embed one.

## When this matters

If a processor throws `ClassNotFoundException` / `NoClassDefFoundError`
here but works fine in a real NiFi instance, it's most likely one of the
gaps above. Start with `flow-tester deps` — it shows exactly which NARs and
jars this tool actually resolved and loaded. If the missing class isn't in
that list, it's probably being supplied to real NiFi through a mechanism
(the system-bundle classpath, instance class loading, ...) this simplified
model doesn't reproduce, and the practical workaround is to add whatever
NAR/jar provides it via `--nar-library`.
