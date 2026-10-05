package com.nf3t.flowtester;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.util.MockFlowFile;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@Command(
        name = "flow-tester",
        mixinStandardHelpOptions = true,
        version = "flow-tester 1.0.0",
        description = "Run a NiFi flow definition (a process group exported as JSON) against an input " +
                "file for testing, loading every processor/controller service out of NiFi Archives (NARs).",
        subcommands = {DepsCommand.class, SchemaCommand.class, FlowSchemaCommand.class}
)
public class Main implements Callable<Integer> {

    @Option(names = {"-f", "--flow"},
            description = "Path to a flow definition JSON -- from a process group's 'Download Flow " +
                    "Definition' action, or GET /nifi-api/process-groups/{id}/download.")
    private File flowFile;

    @Option(names = {"-i", "--input"},
            description = "Path to a FlowFile Package V3 file (attributes + content per flowfile, " +
                    "possibly more than one) to deliver to each flow entry point (a processor or port " +
                    "with no incoming connection). Produced e.g. by a MergeContent processor configured " +
                    "with Merge Format 'FlowFile Stream, v3', or programmatically via " +
                    "org.apache.nifi.util.FlowFilePackagerV3.")
    private File inputFile;

    @Option(names = {"-a", "--attribute"},
            description = "Attribute (KEY=VALUE) to set on every input flowfile, overriding whatever " +
                    "the package itself carries for that key. Repeatable.")
    private Map<String, String> attributes = new LinkedHashMap<>();

    @Option(names = {"-e", "--input-port"},
            description = "Deliver --input only to this input port (by name or identifier) instead of " +
                    "every flow entry point. Must be a root-level input port with no incoming connection " +
                    "within the flow.")
    private String inputPort;

    @Option(names = {"-o", "--output"},
            description = "Directory to write captured output flowfiles to (default: ${DEFAULT-VALUE}).")
    private File outputDir = new File("output");

    @Option(names = {"-r", "--require-output"},
            description = "Path to a JSON file describing expected output flowfiles (content and/or " +
                    "attributes); if any don't match, the run exits non-zero. Run 'flow-tester schema' " +
                    "to get its JSON Schema. See the README for the format.")
    private File requireOutputFile;

    @Option(names = {"-p", "--nifi-properties"},
            description = "Path to a NiFi instance's nifi.properties file, or its home directory " +
                    "(containing conf/nifi.properties). Used to resolve the NAR library directories " +
                    "to search for every processor/controller service the flow needs.")
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
            if (flowFile == null) {
                System.err.println("Missing required option: --flow");
                return 2;
            }
            if (inputFile == null) {
                System.err.println("Missing required option: --input");
                return 2;
            }
            if (!flowFile.isFile()) {
                System.err.println("Flow definition file not found: " + flowFile);
                return 2;
            }
            if (!inputFile.isFile()) {
                System.err.println("Input file not found: " + inputFile);
                return 2;
            }
            if (requireOutputFile != null && !requireOutputFile.isFile()) {
                System.err.println("Require-output file not found: " + requireOutputFile);
                return 2;
            }
            if (nifiProperties == null && narLibraryDirs.isEmpty()) {
                System.err.println("Either --nifi-properties or --nar-library is required to locate NARs.");
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

            VersionedProcessGroup flow = FlowDefinitionLoader.load(flowFile);

            System.out.println("Flow:      " + flowFile.getAbsolutePath());
            System.out.println("Input:     " + inputFile.getAbsolutePath());
            System.out.println("NAR library directories:");
            libraryDirectories.forEach(d -> System.out.println("  - " + d));
            System.out.println();

            List<FlowTestExecutor.InputFlowFile> inputs = FlowFilePackageReader.read(inputFile);
            if (!attributes.isEmpty()) {
                List<FlowTestExecutor.InputFlowFile> overridden = new ArrayList<>();
                for (FlowTestExecutor.InputFlowFile input : inputs) {
                    Map<String, String> merged = new LinkedHashMap<>(input.attributes());
                    merged.putAll(attributes);
                    overridden.add(new FlowTestExecutor.InputFlowFile(input.content(), merged));
                }
                inputs = overridden;
            }
            System.out.println("Input flowfile(s): " + inputs.size());

            FlowTestExecutor executor = new FlowTestExecutor();
            FlowTestExecutor.ExecutionResult result =
                    executor.execute(flow, inputs, libraryDirectories, inputPort);

            printLogMessages(result);
            printDroppedOutputs(result);

            Files.createDirectories(outputDir.toPath());
            ObjectMapper jsonMapper = new ObjectMapper();

            for (FlowTestExecutor.OutputGroup group : result.outputGroups) {
                Path groupDir = outputDir.toPath().resolve(group.label);
                Files.createDirectories(groupDir);

                System.out.printf("Output '%s': %d flowfile(s)%n", group.label, group.flowFiles.size());

                int index = 0;
                for (MockFlowFile outputFlowFile : group.flowFiles) {
                    index++;
                    Path contentPath = groupDir.resolve(index + ".content");
                    Path attributesPath = groupDir.resolve(index + ".attributes.json");

                    Files.write(contentPath, outputFlowFile.toByteArray());
                    jsonMapper.writerWithDefaultPrettyPrinter()
                            .writeValue(attributesPath.toFile(), outputFlowFile.getAttributes());

                    System.out.println("  -> " + contentPath);
                }
            }

            System.out.println();
            System.out.println("Done. Output written to " + outputDir.getAbsolutePath());

            if (requireOutputFile != null) {
                RequireOutputSpec spec = ExpectedOutputLoader.load(requireOutputFile);
                List<String> failures = OutputVerifier.verify(spec, result.outputGroups, requireOutputFile.getAbsoluteFile().getParentFile());
                System.out.println();
                if (failures.isEmpty()) {
                    System.out.println("Output verification: PASSED (" + requireOutputFile + ")");
                } else {
                    System.out.println("Output verification: FAILED (" + requireOutputFile + ")");
                    failures.forEach(msg -> System.out.println("  - " + msg));
                    return 3;
                }
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Test execution failed: " + e);
            e.printStackTrace();
            return 1;
        }
    }

    private static void printDroppedOutputs(FlowTestExecutor.ExecutionResult result) {
        if (result.droppedOutputs.isEmpty()) {
            return;
        }
        System.out.println("This flow has output port(s), so only output-port results are captured. " +
                "Dropped (not routed to any output port):");
        result.droppedOutputs.forEach(msg -> System.out.println("  - " + msg));
        System.out.println();
    }

    private static void printLogMessages(FlowTestExecutor.ExecutionResult result) {
        if (result.logs.isEmpty()) {
            return;
        }
        System.out.println("Messages logged by the flow's processors:");
        for (FlowTestExecutor.ProcessorLog log : result.logs) {
            System.out.printf("  [%s] %s: %s%n", log.level, log.processorName, log.message);
        }
        System.out.println();
    }
}
