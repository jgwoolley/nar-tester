package com.nf3t.flowtester;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

/** Reads a {@code --require-output} JSON file into a {@link RequireOutputSpec}. */
public final class ExpectedOutputLoader {

    private ExpectedOutputLoader() {
    }

    public static RequireOutputSpec load(File file) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(file, RequireOutputSpec.class);
    }
}
