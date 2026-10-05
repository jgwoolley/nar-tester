package com.nf3t.flowtester;

import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Resolves which NARs (and their bundled jars) are needed to load a given
 * set of processor / controller service classes, without instantiating or
 * running anything. Useful for answering "what would actually get loaded"
 * ahead of time, independent of any one test run.
 */
@Command(
        name = "deps",
        mixinStandardHelpOptions = true,
        description = "List which NARs (and their bundled jars) are needed to load a set of processor " +
                "and/or controller service classes, without running anything."
)
public class DepsCommand implements Callable<Integer> {

    @Option(names = {"--processor"},
            description = "Fully-qualified processor class to resolve. Repeatable.")
    private List<String> processorClasses = new ArrayList<>();

    @Option(names = {"--controller-service"},
            description = "Fully-qualified controller service class to resolve. Repeatable.")
    private List<String> controllerServiceClasses = new ArrayList<>();

    @Option(names = {"-f", "--flow"},
            description = "Optional: also pull every processor/controller service type out of a flow " +
                    "definition JSON, on top of any --processor / --controller-service given directly.")
    private File flowFile;

    @Option(names = {"-p", "--nifi-properties"},
            description = "Path to a NiFi instance's nifi.properties file, or its home directory " +
                    "(containing conf/nifi.properties). Used to resolve the NAR library directories to " +
                    "search, the same as for the top-level command.")
    private File nifiProperties;

    @Option(names = {"-L", "--nar-library"},
            description = "Additional director(y|ies) to scan for .nar files, on top of whatever " +
                    "--nifi-properties resolves. Repeatable.")
    private List<File> narLibraryDirs = new ArrayList<>();

    @Override
    public Integer call() {
        try {
            Set<String> processors = new LinkedHashSet<>(processorClasses);
            Set<String> controllerServices = new LinkedHashSet<>(controllerServiceClasses);

            if (flowFile != null) {
                if (!flowFile.isFile()) {
                    System.err.println("Flow definition file not found: " + flowFile);
                    return 2;
                }
                VersionedProcessGroup flow = FlowDefinitionLoader.load(flowFile);
                FlowGraph graph = new FlowGraph(flow);
                for (VersionedProcessor processor : graph.processors()) {
                    processors.add(processor.getType());
                    for (VersionedControllerService service : ControllerServiceResolver.resolve(graph, processor).values()) {
                        controllerServices.add(service.getType());
                    }
                }
            }

            if (processors.isEmpty() && controllerServices.isEmpty()) {
                System.err.println("Specify at least one --processor / --controller-service, or --flow.");
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

            NarBundleGraph bundleGraph = new NarBundleGraph(Thread.currentThread().getContextClassLoader());
            for (Path dir : libraryDirectories) {
                bundleGraph.scanDirectory(dir);
            }

            boolean ok = true;
            for (String processorClass : processors) {
                ok &= resolveOne(bundleGraph, libraryDirectories, "processor", processorClass);
            }
            for (String serviceClass : controllerServices) {
                ok &= resolveOne(bundleGraph, libraryDirectories, "controller service", serviceClass);
            }

            if (!ok) {
                bundleGraph.cleanup();
                return 2;
            }

            List<LoadedNarInfo> loaded = bundleGraph.loadedBundles();
            bundleGraph.cleanup();

            int totalJars = loaded.stream().mapToInt(info -> info.jars.size()).sum();
            System.out.println("NARs needed (" + loaded.size() + "), " + totalJars + " jar(s) total:");
            for (LoadedNarInfo info : loaded) {
                String narCoordinates = info.coordinates != null ? " (" + info.coordinates + ")" : "";
                System.out.println("  - " + info.narId + narCoordinates + " [" + info.narFile + "] -- "
                        + info.jars.size() + " jar(s)");
                for (JarInfo jar : info.jars) {
                    String jarCoordinates = jar.coordinates() != null ? " (" + jar.coordinates() + ")" : "";
                    System.out.println("      " + jar.fileName() + jarCoordinates);
                }
            }

            return 0;
        } catch (Exception e) {
            System.err.println("Failed to resolve dependencies: " + e);
            e.printStackTrace();
            return 1;
        }
    }

    /** Locates the NAR providing className, registers it, and resolves its full dependency chain. */
    private boolean resolveOne(NarBundleGraph bundleGraph, List<Path> libraryDirectories, String kind, String className)
            throws Exception {
        List<File> candidates = NarLocator.findNarsProviding(libraryDirectories, className);
        if (candidates.isEmpty()) {
            System.err.println("Could not find a NAR providing " + kind + " class '" + className + "' in:");
            libraryDirectories.forEach(d -> System.err.println("  - " + d));
            return false;
        }
        if (candidates.size() > 1) {
            System.err.println("Multiple NARs provide " + kind + " class '" + className + "':");
            candidates.forEach(f -> System.err.println("  - " + f));
            return false;
        }
        String narId = bundleGraph.registerNar(candidates.get(0));
        bundleGraph.resolveClassLoader(narId);
        return true;
    }
}
