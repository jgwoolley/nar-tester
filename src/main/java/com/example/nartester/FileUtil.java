package com.example.nartester;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class FileUtil {

    private FileUtil() {
    }

    static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (var walk = Files.walk(path)) {
            walk.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException ignored) {
                    // best-effort cleanup of temp working directories
                }
            });
        }
    }
}
