package com.nf3t.flowtester;

import java.io.File;
import java.util.List;

/** Reports one NAR whose classloader was actually built (unpacked + jars loaded), and which jars went into it. */
public final class LoadedNarInfo {

    public final String narId;
    public final File narFile;
    /** groupId:narId:version from the NAR's manifest (Nar-Group/Nar-Id/Nar-Version), or null if unavailable. */
    public final String coordinates;
    public final List<JarInfo> jars;

    LoadedNarInfo(String narId, File narFile, String coordinates, List<JarInfo> jars) {
        this.narId = narId;
        this.narFile = narFile;
        this.coordinates = coordinates;
        this.jars = jars;
    }
}
