package com.example.nartester;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.nifi.util.MockFlowFile;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "nar-tester",
        mixinStandardHelpOptions = true,
        version = "nar-tester 1.0.0",
        description = "Load a processor out of a NiFi Archive (NAR) and run it against an input file for testing.",
        subcommands = {DepsCommand.class}
)
public class Main implements Callable<Integer> {

    @Option(names = {"-n", "--nar"},
            description = "Path to the .nar file containing the processor to test. If omitted, it is " +
                    "auto-discovered from --nifi-properties / --nar-library by scanning for the NAR that " +
                    "provides the configured processorClass.")
    private File narFile;

    @Option(names = {"-i", "--input"},
            description = "Path to the input file to use as the flowfile content.")
    private File inputFile;

    @Option(names = {"-c", "--config"},
            description = "Path to a YAML (or JSON) config file: processor class, properties, " +
                    "attributes, and relationships to capture.")
    private File configFile;

    @Option(names = {"-o", "--output"},
            description = "Directory to write captured output flowfiles to (default: ${DEFAULT-VALUE}).")
    private File outputDir = new File("output");

    @Option(names = {"-p", "--nifi-properties"},
            description = "Path to a NiFi instance's nifi.properties file, or its home directory " +
                    "(containing conf/nifi.properties). Used to resolve the NAR library directories " +
                    "(nifi.nar.library.directory[.*], nifi.nar.library.autoload.directory) so that NARs " +
                    "depending on other NARs in that instance can be resolved the way NiFi itself would.")
    private File nifiProperties;

    @Option(names = {"-L", "--nar-library"},
            description = "Additional director(y|ies) to scan for .nar files, on top of whatever " +
                    "--nifi-properties resolves. Repeatable.")
    private List<File> narLibraryDirs = new ArrayList<>();

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            if (inputFile == null) {
                System.err.println("Missing required option: --input");
                return 2;
            }
            if (configFile == null) {
                System.err.println("Missing required option: --config");
                return 2;
            }
            if (narFile != null && !narFile.isFile()) {
                System.err.println("NAR file not found: " + narFile);
                return 2;
            }
            if (!inputFile.isFile()) {
                System.err.println("Input file not found: " + inputFile);
                return 2;
            }
            if (!configFile.isFile()) {
                System.err.println("Config file not found: " + configFile);
                return 2;
            }

            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            TestConfig config = yamlMapper.readValue(configFile, TestConfig.class);

            if (config.getProcessorClass() == null || config.getProcessorClass().isBlank()) {
                System.err.println("Config error: 'processorClass' is required.");
                return 2;
            }

            if (narFile == null && nifiProperties == null && narLibraryDirs.isEmpty()) {
                System.err.println("Either --nar must be given, or --nifi-properties / --nar-library " +
                        "so the NAR can be auto-discovered.");
                return 2;
            }

            List<Path> libraryDirectories = new ArrayList<>();
            if (nifiProperties != null) {
                libraryDirectories.addAll(NiFiPropertiesLoader.resolveLibraryDirectories(nifiProperties.toPath()));
            }
            for (File dir : narLibraryDirs) {
                Path path = dir.toPath();
                if (!libraryDirectories.contains(path)) {
                    libraryDirectories.add(path);
                }
            }

            if (narFile == null) {
                List<File> candidates = NarLocator.findNarsProviding(libraryDirectories, config.getProcessorClass());
                if (candidates.isEmpty()) {
                    System.err.println("Could not find a NAR providing processor class '" +
                            config.getProcessorClass() + "' in:");
                    libraryDirectories.forEach(d -> System.err.println("  - " + d));
                    System.err.println("Pass --nar explicitly if the NAR lives elsewhere.");
                    return 2;
                }
                if (candidates.size() > 1) {
                    System.err.println("Multiple NARs provide processor class '" +
                            config.getProcessorClass() + "':");
                    candidates.forEach(f -> System.err.println("  - " + f));
                    System.err.println("Pass --nar to disambiguate.");
                    return 2;
                }
                narFile = candidates.get(0);
                System.out.println("NAR:       " + narFile.getAbsolutePath() + " (auto-discovered)");
            } else {
                System.out.println("NAR:       " + narFile.getAbsolutePath());
            }

            System.out.println("Input:     " + inputFile.getAbsolutePath());
            System.out.println("Processor: " + config.getProcessorClass());
            if (!libraryDirectories.isEmpty()) {
                System.out.println("NAR library directories:");
                libraryDirectories.forEach(d -> System.out.println("  - " + d));
            }
            System.out.println();

            ProcessorTestExecutor executor = new ProcessorTestExecutor();
            ProcessorTestExecutor.ExecutionResult result =
                    executor.execute(narFile, inputFile, config, libraryDirectories);

            printLogMessages("TRACE", result.loggedTrace);
            printLogMessages("DEBUG", result.loggedDebug);
            printLogMessages("INFO", result.loggedInfo);
            printLogMessages("WARN", result.loggedWarnings);
            printLogMessages("ERROR", result.loggedErrors);

            Files.createDirectories(outputDir.toPath());
            ObjectMapper jsonMapper = new ObjectMapper();

            for (ProcessorTestExecutor.RelationshipResult relResult : result.relationshipResults) {
                Path relDir = outputDir.toPath().resolve(relResult.relationshipName);
                Files.createDirectories(relDir);

                System.out.printf("Relationship '%s': %d flowfile(s)%n",
                        relResult.relationshipName, relResult.flowFiles.size());

                int index = 0;
                for (MockFlowFile flowFile : relResult.flowFiles) {
                    index++;
                    Path contentPath = relDir.resolve(index + ".content");
                    Path attributesPath = relDir.resolve(index + ".attributes.json");

                    Files.write(contentPath, flowFile.toByteArray());
                    jsonMapper.writerWithDefaultPrettyPrinter()
                            .writeValue(attributesPath.toFile(), flowFile.getAttributes());

                    System.out.println("  -> " + contentPath);
                }
            }

            System.out.println();
            System.out.println("Done. Output written to " + outputDir.getAbsolutePath());
            return 0;
        } catch (Exception e) {
            System.err.println("Test execution failed: " + e);
            e.printStackTrace();
            return 1;
        }
    }

    private static void printLogMessages(String level, List<String> messages) {
        if (messages.isEmpty()) {
            return;
        }
        System.out.println(level + " messages logged by the processor:");
        messages.forEach(m -> System.out.println("  - " + m));
        System.out.println();
    }
}
