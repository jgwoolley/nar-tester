package com.nf3t.flowtester;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.victools.jsonschema.module.jackson.JacksonModule;
import org.apache.nifi.flow.VersionedExternalFlow;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.Callable;

/**
 * Generates the JSON Schema for a {@code --flow} file, straight from
 * {@code org.apache.nifi.flow.VersionedExternalFlow} via reflection (using
 * victools jsonschema-generator + its Jackson module), so it always matches
 * whatever {@code nifi.version} this build was compiled against -- no
 * hand-maintained schema to drift from the real model classes.
 */
@Command(
        name = "flow-schema",
        mixinStandardHelpOptions = true,
        description = "Print the JSON Schema for a --flow flow definition file " +
                "(org.apache.nifi.flow.VersionedExternalFlow)."
)
public class FlowSchemaCommand implements Callable<Integer> {

    @Option(names = {"-o", "--output"},
            description = "Write the schema to this file instead of stdout.")
    private File outputFile;

    @Override
    public Integer call() throws Exception {
        SchemaGeneratorConfigBuilder configBuilder =
                new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON);
        configBuilder.with(new JacksonModule());
        SchemaGeneratorConfig config = configBuilder.build();
        SchemaGenerator generator = new SchemaGenerator(config);
        ObjectNode schema = generator.generateSchema(VersionedExternalFlow.class);

        byte[] bytes = (schema.toPrettyString() + "\n").getBytes(StandardCharsets.UTF_8);
        if (outputFile != null) {
            Files.write(outputFile.toPath(), bytes);
            System.out.println("Wrote " + outputFile.getAbsolutePath());
        } else {
            System.out.write(bytes);
            System.out.flush();
        }
        return 0;
    }
}
