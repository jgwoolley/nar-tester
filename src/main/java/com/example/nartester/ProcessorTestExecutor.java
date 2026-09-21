package com.example.nartester;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.io.File;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class ProcessorTestExecutor {

    public static final class RelationshipResult {
        public final String relationshipName;
        public final List<MockFlowFile> flowFiles;

        RelationshipResult(String relationshipName, List<MockFlowFile> flowFiles) {
            this.relationshipName = relationshipName;
            this.flowFiles = flowFiles;
        }
    }

    public static final class ExecutionResult {
        public final List<RelationshipResult> relationshipResults = new ArrayList<>();
        public final List<String> loggedTrace = new ArrayList<>();
        public final List<String> loggedDebug = new ArrayList<>();
        public final List<String> loggedInfo = new ArrayList<>();
        public final List<String> loggedWarnings = new ArrayList<>();
        public final List<String> loggedErrors = new ArrayList<>();
    }

    /**
     * @param libraryDirectories NAR library directories to resolve cross-NAR dependencies
     *                           against (from --nifi-properties and/or --nar-library). If
     *                           empty, falls back to loading narFile as a single
     *                           self-contained NAR, as before.
     */
    public ExecutionResult execute(File narFile, File inputFile, TestConfig config,
                                    List<Path> libraryDirectories) throws Exception {
        ClassLoader launcherClassLoader = Thread.currentThread().getContextClassLoader();

        NarBundleGraph bundleGraph = null;
        Path legacyWorkingDir = null;
        ClassLoader processorClassLoader;

        if (libraryDirectories.isEmpty()) {
            legacyWorkingDir = NarClassLoaderBuilder.unpack(narFile);
            processorClassLoader = NarClassLoaderBuilder.buildClassLoader(legacyWorkingDir, launcherClassLoader).classLoader();
        } else {
            bundleGraph = new NarBundleGraph(launcherClassLoader);
            for (Path dir : libraryDirectories) {
                bundleGraph.scanDirectory(dir);
            }
            String targetNarId = bundleGraph.registerNar(narFile);
            processorClassLoader = bundleGraph.resolveClassLoader(targetNarId);
        }

        // NiFi components commonly resolve resources (config files, service
        // providers, etc.) via the thread context classloader, so swap it
        // for the duration of instantiation + execution, the same way
        // NiFi's own NarThreadContextClassLoader does around each invocation.
        Thread.currentThread().setContextClassLoader(processorClassLoader);
        try {
            Class<?> processorClass = Class.forName(config.getProcessorClass(), true, processorClassLoader);
            Object instance = processorClass.getDeclaredConstructor().newInstance();
            if (!(instance instanceof Processor)) {
                throw new IllegalArgumentException(
                        config.getProcessorClass() + " does not implement org.apache.nifi.processor.Processor");
            }
            Processor processor = (Processor) instance;

            TestRunner runner = TestRunners.newTestRunner(processor);

            // Controller services must be added, configured, and enabled
            // before the processor is run so that any processor property
            // referencing a service's identifier validates successfully.
            for (ControllerServiceConfig csConfig : config.getControllerServices()) {
                ClassLoader serviceClassLoader = processorClassLoader;
                if (csConfig.getNar() != null && !csConfig.getNar().isBlank()) {
                    if (bundleGraph == null) {
                        throw new IllegalArgumentException("controllerServices[].nar ('" + csConfig.getNar() +
                                "') requires --nifi-properties or --nar-library so cross-NAR dependencies can be resolved.");
                    }
                    String serviceNarId = bundleGraph.registerNar(new File(csConfig.getNar()));
                    serviceClassLoader = bundleGraph.resolveClassLoader(serviceNarId);
                }

                Class<?> serviceClass = Class.forName(csConfig.getImplementationClass(), true, serviceClassLoader);
                Object serviceInstance = serviceClass.getDeclaredConstructor().newInstance();
                if (!(serviceInstance instanceof ControllerService)) {
                    throw new IllegalArgumentException(csConfig.getImplementationClass()
                            + " does not implement org.apache.nifi.controller.ControllerService");
                }
                ControllerService service = (ControllerService) serviceInstance;

                runner.addControllerService(csConfig.getIdentifier(), service);
                for (Map.Entry<String, String> prop : csConfig.getProperties().entrySet()) {
                    runner.setProperty(service, prop.getKey(), prop.getValue());
                }
                runner.enableControllerService(service);
            }

            for (Map.Entry<String, String> prop : config.getProperties().entrySet()) {
                runner.setProperty(prop.getKey(), prop.getValue());
            }

            byte[] content = Files.readAllBytes(inputFile.toPath());
            Map<String, String> attributes = new LinkedHashMap<>(config.getAttributes());
            attributes.putIfAbsent("filename", inputFile.getName());
            runner.enqueue(content, attributes);

            runner.run(Math.max(1, config.getRunIterations()));

            ExecutionResult result = new ExecutionResult();

            List<String> wanted = config.getRelationships();
            Set<Relationship> declaredRelationships = processor.getRelationships();
            for (Relationship relationship : declaredRelationships) {
                if (wanted != null && !wanted.isEmpty() && !wanted.contains(relationship.getName())) {
                    continue;
                }
                List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(relationship);
                result.relationshipResults.add(new RelationshipResult(relationship.getName(), flowFiles));
            }

            result.loggedTrace.addAll(toMessages(runner.getLogger().getTraceMessages()));
            result.loggedDebug.addAll(toMessages(runner.getLogger().getDebugMessages()));
            result.loggedInfo.addAll(toMessages(runner.getLogger().getInfoMessages()));
            result.loggedWarnings.addAll(toMessages(runner.getLogger().getWarnMessages()));
            result.loggedErrors.addAll(toMessages(runner.getLogger().getErrorMessages()));

            return result;
        } finally {
            Thread.currentThread().setContextClassLoader(launcherClassLoader);
            if (bundleGraph != null) {
                bundleGraph.cleanup();
            }
            if (legacyWorkingDir != null) {
                if (processorClassLoader instanceof URLClassLoader urlClassLoader) {
                    urlClassLoader.close();
                }
                FileUtil.deleteRecursively(legacyWorkingDir);
            }
        }
    }

    private List<String> toMessages(List<LogMessage> logMessages) {
        return logMessages.stream().map(LogMessage::getMsg).collect(Collectors.toList());
    }
}
