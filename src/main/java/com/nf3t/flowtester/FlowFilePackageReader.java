package com.nf3t.flowtester;

import org.apache.nifi.util.FlowFileUnpackagerV3;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads {@code --input} as a NiFi FlowFile Package V3 file -- the format
 * NiFi itself uses to bundle one or more flowfiles (attributes + content
 * each) into a single file, e.g. via a MergeContent processor configured
 * with Merge Format "FlowFile Stream, v3", or programmatically via
 * {@code org.apache.nifi.util.FlowFilePackagerV3}. A package can hold more
 * than one flowfile; every one of them is returned, in order.
 */
public final class FlowFilePackageReader {

    private FlowFilePackageReader() {
    }

    public static List<FlowTestExecutor.InputFlowFile> read(File packageFile) throws IOException {
        List<FlowTestExecutor.InputFlowFile> flowFiles = new ArrayList<>();
        FlowFileUnpackagerV3 unpackager = new FlowFileUnpackagerV3();
        try (InputStream in = Files.newInputStream(packageFile.toPath())) {
            while (unpackager.hasMoreData()) {
                ByteArrayOutputStream content = new ByteArrayOutputStream();
                Map<String, String> attributes = unpackager.unpackageFlowFile(in, content);
                flowFiles.add(new FlowTestExecutor.InputFlowFile(content.toByteArray(), new LinkedHashMap<>(attributes)));
            }
        }
        if (flowFiles.isEmpty()) {
            throw new IOException(packageFile + " contains no flowfiles -- is it a valid FlowFile Package V3 file?");
        }
        return flowFiles;
    }
}
