package com.nf3t.flowtester;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Builds NiFi's NAR dependency graph -- as declared via the {@code Nar-Id}
 * / {@code Nar-Dependency-Id} (and Group/Version) manifest attributes that
 * the nifi-nar-maven-plugin writes into every NAR -- across a set of
 * library directories, and lazily constructs the chain of parent-linked
 * classloaders needed to load any one bundle correctly. This includes
 * bundles that only compile/link because another NAR in the graph
 * supplies shared classes as a parent classloader (the common case for a
 * controller-service-API NAR shared by a service implementation and the
 * processor that uses it).
 *
 * This mirrors what NiFi's own {@code NarClassLoaders} does at framework
 * startup, using only the stable, documented manifest contract rather
 * than NiFi's internal implementation classes.
 */
public final class NarBundleGraph {

    private final Map<String, NarBundle> bundlesByNarId = new LinkedHashMap<>();
    private final ClassLoader rootClassLoader;

    public NarBundleGraph(ClassLoader rootClassLoader) {
        this.rootClassLoader = rootClassLoader;
    }

    /** Scans a directory (non-recursive) for *.nar files and registers each as a candidate bundle. */
    public void scanDirectory(Path directory) throws IOException {
        if (!Files.isDirectory(directory)) {
            return;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*.nar")) {
            for (Path narPath : stream) {
                registerNar(narPath.toFile(), false);
            }
        }
    }

    /**
     * Explicitly registers a single NAR file, such as the NAR under test or a
     * controller service's NAR. Takes precedence over any bundle with the same
     * Nar-Id already discovered via scanDirectory, so testing an in-progress
     * build always wins over whatever happens to be installed in a library
     * directory.
     *
     * @return the Nar-Id this NAR was registered under.
     */
    public String registerNar(File narFile) throws IOException {
        return registerNar(narFile, true);
    }

    private String registerNar(File narFile, boolean override) throws IOException {
        NarBundle bundle = readManifest(narFile);
        NarBundle existing = bundlesByNarId.get(bundle.narId);
        if (existing == null || override) {
            bundlesByNarId.put(bundle.narId, bundle);
        }
        return bundle.narId;
    }

    /** Resolves (lazily building, from the dependency leaves up) the classloader for the given Nar-Id. */
    public ClassLoader resolveClassLoader(String narId) throws IOException {
        return resolveClassLoader(narId, new LinkedHashSet<>());
    }

    /** Closes every classloader built so far and deletes their temp working directories. */
    public void cleanup() {
        for (NarBundle bundle : bundlesByNarId.values()) {
            if (bundle.classLoader instanceof URLClassLoader urlClassLoader) {
                try {
                    urlClassLoader.close();
                } catch (IOException ignored) {
                    // best-effort
                }
            }
            if (bundle.unpackedDir != null) {
                try {
                    FileUtil.deleteRecursively(bundle.unpackedDir);
                } catch (IOException ignored) {
                    // best-effort
                }
            }
        }
    }

    private ClassLoader resolveClassLoader(String narId, Set<String> resolving) throws IOException {
        NarBundle bundle = bundlesByNarId.get(narId);
        if (bundle == null) {
            throw new IOException("No NAR providing Nar-Id '" + narId + "' was found in the scanned library " +
                    "directories. Check --nifi-properties / --nar-library, and that the NAR is actually present.");
        }
        if (bundle.classLoader != null) {
            return bundle.classLoader;
        }
        if (!resolving.add(narId)) {
            throw new IOException("Cycle detected in the NAR dependency graph while resolving '" + narId + "'");
        }

        ClassLoader parent = rootClassLoader;
        if (bundle.dependencyId != null && !bundle.dependencyId.isBlank()) {
            parent = resolveClassLoader(bundle.dependencyId, resolving);
        }

        if (bundle.unpackedDir == null) {
            bundle.unpackedDir = NarClassLoaderBuilder.unpack(bundle.narFile);
        }
        NarClassLoaderBuilder.BuiltClassLoader built = NarClassLoaderBuilder.buildClassLoader(bundle.unpackedDir, parent);
        bundle.classLoader = built.classLoader();
        bundle.jars = built.jars();
        resolving.remove(narId);
        return built.classLoader();
    }

    /** Every NAR whose classloader was actually built (as opposed to merely scanned/registered), with its jars. */
    public List<LoadedNarInfo> loadedBundles() {
        List<LoadedNarInfo> loaded = new ArrayList<>();
        for (NarBundle bundle : bundlesByNarId.values()) {
            if (bundle.classLoader != null) {
                String coordinates = (bundle.group != null && bundle.version != null)
                        ? bundle.group + ":" + bundle.narId + ":" + bundle.version
                        : null;
                loaded.add(new LoadedNarInfo(bundle.narId, bundle.narFile, coordinates, bundle.jars));
            }
        }
        return loaded;
    }

    private NarBundle readManifest(File narFile) throws IOException {
        try (ZipFile zip = new ZipFile(narFile)) {
            ZipEntry entry = zip.getEntry("META-INF/MANIFEST.MF");
            if (entry == null) {
                throw new IOException(narFile + " has no META-INF/MANIFEST.MF -- is it a valid NAR?");
            }
            Manifest manifest;
            try (InputStream in = zip.getInputStream(entry)) {
                manifest = new Manifest(in);
            }
            Attributes attrs = manifest.getMainAttributes();
            String narId = attrs.getValue("Nar-Id");
            if (narId == null || narId.isBlank()) {
                // Fall back to the file name so the NAR can still be loaded directly,
                // though it won't be reachable as a Nar-Dependency-Id target unless
                // that happens to match.
                narId = stripExtension(narFile.getName());
            }
            String group = attrs.getValue("Nar-Group");
            String version = attrs.getValue("Nar-Version");
            String dependencyId = attrs.getValue("Nar-Dependency-Id");
            return new NarBundle(narFile, narId, group, version, dependencyId);
        }
    }

    private static String stripExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return dot > 0 ? fileName.substring(0, dot) : fileName;
    }
}
