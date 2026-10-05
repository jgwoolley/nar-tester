package com.nf3t.flowtester;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.concurrent.Callable;

/**
 * Prints (or writes to a file) the JSON Schema for a {@code --require-output}
 * file, so an editor like VS Code can offer validation/autocomplete for it:
 * either add {@code "$schema": "./require-output.schema.json"} to the
 * require-output file itself, or map it in VS Code's {@code json.schemas}
 * setting.
 */
@Command(
        name = "schema",
        mixinStandardHelpOptions = true,
        description = "Print the JSON Schema for a --require-output file."
)
public class SchemaCommand implements Callable<Integer> {

    @Option(names = {"-o", "--output"},
            description = "Write the schema to this file instead of stdout.")
    private File outputFile;

    @Override
    public Integer call() throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/require-output.schema.json")) {
            if (in == null) {
                System.err.println("Bundled schema resource is missing -- this is a packaging bug.");
                return 1;
            }
            byte[] schema = in.readAllBytes();
            if (outputFile != null) {
                Files.write(outputFile.toPath(), schema);
                System.out.println("Wrote " + outputFile.getAbsolutePath());
            } else {
                System.out.write(schema);
                System.out.flush();
            }
        }
        return 0;
    }
}
