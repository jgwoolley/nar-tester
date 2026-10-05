package com.nf3t.flowtester;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

final class NarBundle {

    final File narFile;
    final String narId;
    final String group;
    final String version;
    final String dependencyId;

    Path unpackedDir;
    ClassLoader classLoader;
    List<JarInfo> jars = List.of();

    NarBundle(File narFile, String narId, String group, String version, String dependencyId) {
        this.narFile = narFile;
        this.narId = narId;
        this.group = group;
        this.version = version;
        this.dependencyId = dependencyId;
    }
}
