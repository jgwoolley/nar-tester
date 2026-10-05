package com.nf3t.flowtester;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Reads a NiFi instance's {@code nifi.properties} and resolves the
 * directories it configures for locating NAR files:
 *
 * <ul>
 *   <li>{@code nifi.nar.library.directory} (default {@code ./lib})</li>
 *   <li>{@code nifi.nar.library.directory.<custom>} (any number of these)</li>
 *   <li>{@code nifi.nar.library.autoload.directory} (default {@code ./extensions})</li>
 * </ul>
 *
 * Relative paths are resolved against the NiFi home directory, matching
 * NiFi's own behavior.
 */
public final class NiFiPropertiesLoader {

    private NiFiPropertiesLoader() {
    }

    /**
     * @param nifiPropertiesOrHome either a nifi.properties file directly, or a NiFi
     *                             home directory containing conf/nifi.properties.
     */
    public static List<Path> resolveLibraryDirectories(Path nifiPropertiesOrHome) throws IOException {
        Path propertiesFile;
        Path homeDir;

        if (Files.isDirectory(nifiPropertiesOrHome)) {
            Path candidate = nifiPropertiesOrHome.resolve("conf").resolve("nifi.properties");
            if (Files.isRegularFile(candidate)) {
                propertiesFile = candidate;
                homeDir = nifiPropertiesOrHome;
            } else if ("conf".equals(pathFileName(nifiPropertiesOrHome))
                    && Files.isRegularFile(nifiPropertiesOrHome.resolve("nifi.properties"))) {
                propertiesFile = nifiPropertiesOrHome.resolve("nifi.properties");
                homeDir = nifiPropertiesOrHome.getParent() != null ? nifiPropertiesOrHome.getParent() : nifiPropertiesOrHome;
            } else {
                throw new IOException("Could not find conf/nifi.properties under " + nifiPropertiesOrHome);
            }
        } else {
            propertiesFile = nifiPropertiesOrHome;
            Path parent = propertiesFile.getParent();
            if (parent != null && "conf".equals(pathFileName(parent)) && parent.getParent() != null) {
                homeDir = parent.getParent();
            } else {
                homeDir = parent != null ? parent : Path.of(".");
            }
        }

        Properties properties = new Properties();
        try (var in = Files.newInputStream(propertiesFile)) {
            properties.load(in);
        }

        List<Path> directories = new ArrayList<>();
        directories.add(resolve(homeDir, properties.getProperty("nifi.nar.library.directory", "./lib")));

        for (String name : properties.stringPropertyNames()) {
            if (name.startsWith("nifi.nar.library.directory.")) {
                directories.add(resolve(homeDir, properties.getProperty(name)));
            }
        }

        directories.add(resolve(homeDir, properties.getProperty("nifi.nar.library.autoload.directory", "./extensions")));

        List<Path> existing = new ArrayList<>();
        for (Path dir : directories) {
            if (Files.isDirectory(dir) && !existing.contains(dir)) {
                existing.add(dir);
            }
        }
        return existing;
    }

    private static String pathFileName(Path path) {
        return path.getFileName() != null ? path.getFileName().toString() : "";
    }

    private static Path resolve(Path homeDir, String value) {
        Path path = Path.of(value);
        return (path.isAbsolute() ? path : homeDir.resolve(path)).normalize();
    }
}
