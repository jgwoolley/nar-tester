package com.nf3t.flowtester;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Runs every processor in a (possibly multi-processor, multi-NAR)
 * {@link FlowGraph} as a single pipeline: one external input is delivered
 * to each flow entry point, processors run in topological order forwarding
 * their output along the flow's connections, and whatever reaches a
 * terminal relationship or output port is captured.
 */
public final class FlowTestExecutor {

    private static final int MAX_ITERATIONS_PER_PROCESSOR = 100;

    public static final class OutputGroup {
        public final String label;
        public final List<MockFlowFile> flowFiles;

        OutputGroup(String label, List<MockFlowFile> flowFiles) {
            this.label = label;
            this.flowFiles = flowFiles;
        }
    }

    public static final class ProcessorLog {
        public final String processorName;
        public final String level;
        public final String message;

        ProcessorLog(String processorName, String level, String message) {
            this.processorName = processorName;
            this.level = level;
            this.message = message;
        }
    }

    public static final class ExecutionResult {
        public final List<OutputGroup> outputGroups = new ArrayList<>();
        public final List<ProcessorLog> logs = new ArrayList<>();
        public final List<String> droppedOutputs = new ArrayList<>();
    }

    /** One flowfile (content + attributes) to deliver into the flow -- e.g. one record unpacked from a FlowFile Package V3 file. */
    public record InputFlowFile(byte[] content, Map<String, String> attributes) {
    }

    /**
     * @param inputs    every flowfile to deliver -- each one is delivered to every flow entry
     *                  point (or just {@code inputPort}, if given).
     * @param inputPort if non-null, deliver inputs only to this input port (by name or
     *                  identifier) -- which must be a flow entry point with no incoming
     *                  connection -- instead of to every entry point in the flow.
     */
    public ExecutionResult execute(VersionedProcessGroup root, List<InputFlowFile> inputs,
                                    List<Path> libraryDirectories, String inputPort) throws Exception {
        FlowGraph graph = new FlowGraph(root);
        List<String> order = graph.topologicalOrder();
        Map<String, String> processorLabels = computeProcessorLabels(graph);

        ClassLoader launcherClassLoader = Thread.currentThread().getContextClassLoader();
        NarBundleGraph bundleGraph = new NarBundleGraph(launcherClassLoader);
        for (Path dir : libraryDirectories) {
            bundleGraph.scanDirectory(dir);
        }

        Map<String, TestRunner> runners = new LinkedHashMap<>();
        Map<String, Processor> processors = new LinkedHashMap<>();
        Map<String, ControllerService> serviceInstances = new LinkedHashMap<>();

        try {
            // Build + configure every processor (and the controller services it needs) up front,
            // so the whole pipeline is ready before anything actually runs.
            for (String processorId : order) {
                VersionedProcessor vp = graph.processor(processorId);
                ClassLoader processorClassLoader =
                        resolveClassLoader(bundleGraph, libraryDirectories, vp.getBundle(), vp.getType());

                Thread.currentThread().setContextClassLoader(processorClassLoader);
                try {
                    Processor processor = instantiate(processorClassLoader, vp.getType(), Processor.class);
                    processors.put(processorId, processor);
                    TestRunner runner = TestRunners.newTestRunner(processor);
                    runners.put(processorId, runner);

                    for (VersionedControllerService vcs : ControllerServiceResolver.resolve(graph, vp).values()) {
                        ControllerService service = serviceInstances.computeIfAbsent(vcs.getIdentifier(), id -> {
                            try {
                                ClassLoader serviceClassLoader =
                                        resolveClassLoader(bundleGraph, libraryDirectories, vcs.getBundle(), vcs.getType());
                                return instantiate(serviceClassLoader, vcs.getType(), ControllerService.class);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
                        runner.addControllerService(vcs.getIdentifier(), service);
                        for (Map.Entry<String, String> p : nullToEmpty(vcs.getProperties()).entrySet()) {
                            runner.setProperty(service, p.getKey(), p.getValue());
                        }
                        runner.enableControllerService(service);
                    }

                    for (Map.Entry<String, String> p : nullToEmpty(vp.getProperties()).entrySet()) {
                        runner.setProperty(p.getKey(), p.getValue());
                    }
                } finally {
                    Thread.currentThread().setContextClassLoader(launcherClassLoader);
                }
            }

            List<String> entryPoints = inputPort != null
                    ? graph.entryProcessorsForPort(inputPort)
                    : List.copyOf(graph.sourceProcessorIds());

            Map<String, List<InputFlowFile>> pending = new LinkedHashMap<>();
            for (String entryId : entryPoints) {
                pending.computeIfAbsent(entryId, k -> new ArrayList<>()).addAll(inputs);
            }

            ExecutionResult result = new ExecutionResult();
            Map<String, List<MockFlowFile>> sinkOutputs = new LinkedHashMap<>();
            boolean preferOutputPorts = graph.hasOutputPorts();

            for (String processorId : order) {
                VersionedProcessor vp = graph.processor(processorId);
                TestRunner runner = runners.get(processorId);
                Processor processor = processors.get(processorId);

                for (InputFlowFile delivery : pending.getOrDefault(processorId, List.of())) {
                    runner.enqueue(delivery.content(), delivery.attributes());
                }

                Thread.currentThread().setContextClassLoader(processor.getClass().getClassLoader());
                try {
                    int iterations = 0;
                    do {
                        runner.run(1);
                        iterations++;
                    } while (runner.getQueueSize().getObjectCount() > 0 && iterations < MAX_ITERATIONS_PER_PROCESSOR);
                } finally {
                    Thread.currentThread().setContextClassLoader(launcherClassLoader);
                }

                Set<String> autoTerminated = nullToEmpty(vp.getAutoTerminatedRelationships());
                for (Relationship relationship : processor.getRelationships()) {
                    List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(relationship);
                    if (flowFiles.isEmpty()) {
                        continue;
                    }
                    List<FlowGraph.Edge> targets = graph.targetsFor(processorId, relationship.getName());
                    if (!targets.isEmpty()) {
                        for (FlowGraph.Edge edge : targets) {
                            if (edge.target() instanceof FlowGraph.SinkTarget st && preferOutputPorts && !st.isOutputPort()) {
                                result.droppedOutputs.add(processorLabels.get(processorId) + " -> '" + relationship.getName() +
                                        "' (" + st.label() + "): " + flowFiles.size() +
                                        " flowfile(s) not routed to an output port -- dropped.");
                                continue;
                            }
                            for (MockFlowFile flowFile : flowFiles) {
                                InputFlowFile delivery = new InputFlowFile(
                                        flowFile.toByteArray(), new LinkedHashMap<>(flowFile.getAttributes()));
                                if (edge.target() instanceof FlowGraph.ProcessorTarget pt) {
                                    pending.computeIfAbsent(pt.processorId(), k -> new ArrayList<>()).add(delivery);
                                } else if (edge.target() instanceof FlowGraph.SinkTarget st) {
                                    sinkOutputs.computeIfAbsent(st.label(), k -> new ArrayList<>()).add(flowFile);
                                }
                            }
                        }
                    } else if (!autoTerminated.contains(relationship.getName())) {
                        if (preferOutputPorts) {
                            result.droppedOutputs.add(processorLabels.get(processorId) + " -> '" + relationship.getName() +
                                    "': " + flowFiles.size() + " flowfile(s) not routed to an output port -- dropped.");
                        } else {
                            String label = processorLabels.get(processorId) + "-" + sanitize(relationship.getName());
                            sinkOutputs.computeIfAbsent(label, k -> new ArrayList<>()).addAll(flowFiles);
                        }
                    }
                    // else: routed nowhere, but explicitly auto-terminated -- intentionally dropped.
                }

                String label = processorLabels.get(processorId);
                addLogs(result, label, "TRACE", runner.getLogger().getTraceMessages());
                addLogs(result, label, "DEBUG", runner.getLogger().getDebugMessages());
                addLogs(result, label, "INFO", runner.getLogger().getInfoMessages());
                addLogs(result, label, "WARN", runner.getLogger().getWarnMessages());
                addLogs(result, label, "ERROR", runner.getLogger().getErrorMessages());
            }

            for (Map.Entry<String, List<MockFlowFile>> entry : sinkOutputs.entrySet()) {
                result.outputGroups.add(new OutputGroup(entry.getKey(), entry.getValue()));
            }
            return result;
        } finally {
            Thread.currentThread().setContextClassLoader(launcherClassLoader);
            bundleGraph.cleanup();
        }
    }

    private void addLogs(ExecutionResult result, String processorLabel, String level, List<LogMessage> messages) {
        for (LogMessage m : messages) {
            result.logs.add(new ProcessorLog(processorLabel, level, m.getMsg()));
        }
    }

    /** Every processor's display name, deduplicated with a short id suffix when two processors share a name. */
    private Map<String, String> computeProcessorLabels(FlowGraph graph) {
        Map<String, List<String>> idsByName = new LinkedHashMap<>();
        for (VersionedProcessor vp : graph.processors()) {
            String name = vp.getName() != null && !vp.getName().isBlank() ? vp.getName() : vp.getIdentifier();
            idsByName.computeIfAbsent(sanitize(name), k -> new ArrayList<>()).add(vp.getIdentifier());
        }
        Map<String, String> labels = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : idsByName.entrySet()) {
            List<String> ids = entry.getValue();
            if (ids.size() == 1) {
                labels.put(ids.get(0), entry.getKey());
            } else {
                for (String id : ids) {
                    labels.put(id, entry.getKey() + "-" + id.substring(0, Math.min(8, id.length())));
                }
            }
        }
        return labels;
    }

    private ClassLoader resolveClassLoader(NarBundleGraph bundleGraph, List<Path> libraryDirectories,
                                            Bundle bundle, String extensionClassName) throws Exception {
        File narFile = NarLocator.findNarByBundle(libraryDirectories, bundle);
        if (narFile == null) {
            List<File> candidates = NarLocator.findNarsProviding(libraryDirectories, extensionClassName);
            if (candidates.size() == 1) {
                narFile = candidates.get(0);
            } else if (candidates.isEmpty()) {
                String bundleDescription = bundle == null ? "none"
                        : bundle.getGroup() + ":" + bundle.getArtifact() + ":" + bundle.getVersion();
                throw new IllegalStateException("Could not find a NAR providing '" + extensionClassName +
                        "' (bundle: " + bundleDescription + ") in the given --nifi-properties / --nar-library directories.");
            } else {
                throw new IllegalStateException("Multiple NARs provide '" + extensionClassName + "': " + candidates);
            }
        }
        String narId = bundleGraph.registerNar(narFile);
        return bundleGraph.resolveClassLoader(narId);
    }

    @SuppressWarnings("unchecked")
    private <T> T instantiate(ClassLoader classLoader, String className, Class<T> expectedType) throws Exception {
        Class<?> clazz = Class.forName(className, true, classLoader);
        Object instance = clazz.getDeclaredConstructor().newInstance();
        if (!expectedType.isInstance(instance)) {
            throw new IllegalArgumentException(className + " does not implement " + expectedType.getName());
        }
        return (T) instance;
    }

    private static String sanitize(String s) {
        return s.trim().replaceAll("[^a-zA-Z0-9._-]+", "_");
    }

    private static Map<String, String> nullToEmpty(Map<String, String> m) {
        return m == null ? Map.of() : m;
    }

    private static Set<String> nullToEmpty(Set<String> s) {
        return s == null ? Set.of() : s;
    }
}
