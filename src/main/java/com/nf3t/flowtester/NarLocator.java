package com.nf3t.flowtester;

import org.apache.nifi.flow.Bundle;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * Discovers which .nar file in a set of library directories provides a given
 * extension class (processor, controller service, reporting task, ...), so
 * {@code --nar} can be omitted whenever {@code --nifi-properties} and/or
 * {@code --nar-library} already point at where the NAR lives.
 *
 * Primarily reads {@code META-INF/docs/extension-manifest.xml}, the extension
 * manifest the nifi-nar-maven-plugin writes into every NAR it builds. Falls back
 * to scanning the NAR's bundled dependency jars directly for the compiled class
 * file, for NARs built without that manifest.
 */
public final class NarLocator {

    private NarLocator() {
    }

    /** Every NAR across libraryDirectories (non-recursive) that provides extensionClassName. */
    public static List<File> findNarsProviding(List<Path> libraryDirectories, String extensionClassName) throws IOException {
        List<File> matches = new ArrayList<>();
        for (Path dir : libraryDirectories) {
            if (!Files.isDirectory(dir)) {
                continue;
            }
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.nar")) {
                for (Path narPath : stream) {
                    if (providesExtension(narPath.toFile(), extensionClassName)) {
                        matches.add(narPath.toFile());
                    }
                }
            }
        }
        return matches;
    }

    /**
     * Finds the NAR in libraryDirectories whose manifest coordinates
     * (Nar-Group/Nar-Id/Nar-Version) match bundle exactly. Falls back to a
     * Nar-Group/Nar-Id-only match (tolerating a Nar-Version mismatch, with a
     * warning printed) if no exact match exists. Returns null if bundle is
     * null/incomplete (as NiFi sometimes writes for a bundle it couldn't
     * determine at design time) or nothing matches at all, so callers can
     * fall back to {@link #findNarsProviding}.
     */
    public static File findNarByBundle(List<Path> libraryDirectories, Bundle bundle) throws IOException {
        if (bundle == null || bundle.getArtifact() == null || bundle.getArtifact().isBlank()) {
            return null;
        }

        File groupArtifactMatch = null;
        for (Path dir : libraryDirectories) {
            if (!Files.isDirectory(dir)) {
                continue;
            }
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.nar")) {
                for (Path narPath : stream) {
                    String[] coordinates = readNarCoordinates(narPath.toFile());
                    if (coordinates == null || !bundle.getArtifact().equals(coordinates[1])) {
                        continue;
                    }
                    boolean groupMatches = coordinates[0] != null && coordinates[0].equals(bundle.getGroup());
                    boolean versionMatches = coordinates[2] != null && coordinates[2].equals(bundle.getVersion());
                    if (groupMatches && versionMatches) {
                        return narPath.toFile();
                    }
                    if (groupMatches && groupArtifactMatch == null) {
                        groupArtifactMatch = narPath.toFile();
                    }
                }
            }
        }
        if (groupArtifactMatch != null) {
            System.err.println("Warning: no exact version match for " + bundle.getGroup() + ":" +
                    bundle.getArtifact() + ":" + bundle.getVersion() + " -- using " + groupArtifactMatch +
                    " (Nar-Id matches, Nar-Version differs).");
        }
        return groupArtifactMatch;
    }

    /** [Nar-Group, Nar-Id, Nar-Version] from a NAR's manifest, or null if unreadable. */
    private static String[] readNarCoordinates(File narFile) {
        try (ZipFile zip = new ZipFile(narFile)) {
            ZipEntry entry = zip.getEntry("META-INF/MANIFEST.MF");
            if (entry == null) {
                return null;
            }
            Manifest manifest;
            try (InputStream in = zip.getInputStream(entry)) {
                manifest = new Manifest(in);
            }
            Attributes attrs = manifest.getMainAttributes();
            return new String[] {attrs.getValue("Nar-Group"), attrs.getValue("Nar-Id"), attrs.getValue("Nar-Version")};
        } catch (IOException e) {
            return null;
        }
    }

    private static boolean providesExtension(File narFile, String extensionClassName) throws IOException {
        try (ZipFile zip = new ZipFile(narFile)) {
            ZipEntry manifestEntry = zip.getEntry("META-INF/docs/extension-manifest.xml");
            if (manifestEntry != null) {
                String xml;
                try (InputStream in = zip.getInputStream(manifestEntry)) {
                    xml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                }
                // The extension's own <name> is always the first child of <extension>, unlike
                // e.g. a <property>'s <name>, so this can't match a coincidentally-named property.
                String marker = "<extension><name>" + extensionClassName + "</name>";
                return xml.contains(marker);
            }
            return containsBundledClass(zip, extensionClassName.replace('.', '/') + ".class");
        }
    }

    private static boolean containsBundledClass(ZipFile zip, String classEntryName) throws IOException {
        Enumeration<? extends ZipEntry> entries = zip.entries();
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            if (!entry.getName().startsWith("META-INF/bundled-dependencies/") || !entry.getName().endsWith(".jar")) {
                continue;
            }
            try (InputStream in = zip.getInputStream(entry);
                 ZipInputStream nested = new ZipInputStream(in)) {
                ZipEntry nestedEntry;
                while ((nestedEntry = nested.getNextEntry()) != null) {
                    if (nestedEntry.getName().equals(classEntryName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
