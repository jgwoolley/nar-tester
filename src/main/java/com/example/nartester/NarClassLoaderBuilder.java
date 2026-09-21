package com.example.nartester;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Unpacks a NiFi Archive (.nar) file and builds a classloader for the
 * processor code and the dependencies bundled inside it.
 *
 * This is a simplified stand-in for NiFi's own NarClassLoader /
 * NarClassLoaders machinery (org.apache.nifi.nar, in nifi-nar-utils),
 * which additionally resolves NAR-to-NAR dependency graphs across a whole
 * lib/ directory and isn't meant to be used as a stable public API outside
 * the NiFi framework itself. For a single self-contained processor NAR
 * (i.e. one that bundles all of its own dependency jars under
 * META-INF/bundled-dependencies, which is the common case for custom
 * processor bundles), unpacking the NAR and pointing a URLClassLoader at
 * those jars is sufficient.
 *
 * nifi-api classes (Processor, Relationship, PropertyDescriptor, ...) are
 * resolved from the parent classloader so that objects handed back to
 * nifi-mock's TestRunner are assignable to those interfaces.
 */
public final class NarClassLoaderBuilder {

    private NarClassLoaderBuilder() {
    }

    public static Path unpack(File narFile) throws IOException {
        Path workingDir = Files.createTempDirectory("nar-tester-");
        try (ZipFile zip = new ZipFile(narFile)) {
            Enumeration<? extends ZipEntry> entries = zip.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                Path target = workingDir.resolve(entry.getName()).normalize();
                if (!target.startsWith(workingDir)) {
                    throw new IOException("NAR entry escapes target directory: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(target);
                    continue;
                }
                Files.createDirectories(target.getParent());
                try (InputStream in = zip.getInputStream(entry)) {
                    Files.copy(in, target);
                }
            }
        }
        return workingDir;
    }

    /** The classloader built for one NAR, plus the bundled dependency jars that went into it (for reporting). */
    public record BuiltClassLoader(URLClassLoader classLoader, List<JarInfo> jars) {
    }

    public static BuiltClassLoader buildClassLoader(Path narWorkingDir, ClassLoader parent) throws IOException {
        Path bundledDependencies = narWorkingDir.resolve("META-INF").resolve("bundled-dependencies");
        List<URL> urls = new ArrayList<>();
        List<JarInfo> jars = new ArrayList<>();

        // Loose classes/resources at the root of the NAR, if any.
        urls.add(narWorkingDir.toUri().toURL());

        if (Files.isDirectory(bundledDependencies)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(bundledDependencies, "*.jar")) {
                for (Path jar : stream) {
                    urls.add(jar.toUri().toURL());
                    jars.add(new JarInfo(jar.getFileName().toString(), readMavenCoordinates(jar)));
                }
            }
        }

        // Note: an API-only NAR (e.g. a controller-service-API bundle) may have few or
        // no jars of its own here -- that's normal, not an error, when resolving a
        // dependency chain via NarBundleGraph.
        URLClassLoader classLoader = new URLClassLoader(urls.toArray(new URL[0]), parent);
        return new BuiltClassLoader(classLoader, jars);
    }

    /**
     * Reads groupId:artifactId:version from a jar's embedded
     * {@code META-INF/maven/<groupId>/<artifactId>/pom.properties}, which
     * maven-jar-plugin writes into jars by default. A shaded jar can carry
     * more than one of these (one per bundled dependency); prefer whichever
     * one's artifactId-version matches the jar's own file name, falling back
     * to the first entry found. Returns null if the jar has none at all.
     */
    private static String readMavenCoordinates(Path jarFile) {
        String baseName = jarFile.getFileName().toString();
        if (baseName.endsWith(".jar")) {
            baseName = baseName.substring(0, baseName.length() - 4);
        }
        try (ZipFile zip = new ZipFile(jarFile.toFile())) {
            String fallback = null;
            Enumeration<? extends ZipEntry> entries = zip.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (!entry.getName().startsWith("META-INF/maven/") || !entry.getName().endsWith("/pom.properties")) {
                    continue;
                }
                Properties props = new Properties();
                try (InputStream in = zip.getInputStream(entry)) {
                    props.load(in);
                }
                String groupId = props.getProperty("groupId");
                String artifactId = props.getProperty("artifactId");
                String version = props.getProperty("version");
                if (groupId == null || artifactId == null || version == null) {
                    continue;
                }
                String coordinates = groupId + ":" + artifactId + ":" + version;
                if (baseName.equals(artifactId + "-" + version)) {
                    return coordinates;
                }
                if (fallback == null) {
                    fallback = coordinates;
                }
            }
            return fallback;
        } catch (IOException e) {
            return null;
        }
    }
}
