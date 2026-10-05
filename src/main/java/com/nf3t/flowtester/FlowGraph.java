package com.nf3t.flowtester;

import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.PortType;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Flattens a (possibly nested) {@link VersionedProcessGroup} into one flat
 * routing graph, so the whole exported process group can be executed as a
 * single pipeline regardless of how many sub-groups it's organized into.
 *
 * NiFi component identifiers are unique UUIDs across the whole exported
 * document, so nesting can be ignored entirely once every processor, port,
 * controller service, and connection has been collected: a connection's
 * source/destination id resolves to the right component no matter which
 * (sub-)group it actually lives in.
 *
 * Ports and funnels don't run any code of their own -- they're spliced out
 * of the routing graph, replaced by direct virtual edges from whatever real
 * processor feeds them to whatever real processor(s) they feed. A port/
 * funnel with no incoming connection becomes a flow entry point (its
 * resolved destinations receive the external input); one with no outgoing
 * connection becomes a labeled output sink.
 */
public final class FlowGraph {

    public sealed interface Target permits ProcessorTarget, SinkTarget {
    }

    public record ProcessorTarget(String processorId) implements Target {
    }

    /**
     * A terminal point that isn't a processor: an output port, a funnel with
     * nowhere to go, or a remote endpoint. {@code isOutputPort} is true only
     * for a genuine output port dead-end, so callers that prefer output
     * ports over other kinds of terminal points can tell them apart.
     */
    public record SinkTarget(String label, boolean isOutputPort) implements Target {
    }

    public record Edge(Target target) {
    }

    private final Map<String, VersionedProcessor> processorsById = new LinkedHashMap<>();
    private final Map<String, VersionedPort> portsById = new LinkedHashMap<>();
    private final Map<String, VersionedControllerService> controllerServicesById = new LinkedHashMap<>();
    private final List<VersionedConnection> connections = new ArrayList<>();

    private final Map<String, List<VersionedConnection>> outgoingBySourceId = new LinkedHashMap<>();
    private final Map<String, Map<String, List<Edge>>> routing = new LinkedHashMap<>();
    private final Set<String> sourceProcessorIds = new LinkedHashSet<>();
    private final Set<String> idsWithIncoming = new LinkedHashSet<>();

    public FlowGraph(VersionedProcessGroup root) {
        collect(root);
        for (VersionedConnection c : connections) {
            outgoingBySourceId.computeIfAbsent(c.getSource().getId(), k -> new ArrayList<>()).add(c);
        }
        buildRouting();
        findSources();
    }

    private void collect(VersionedProcessGroup group) {
        for (VersionedProcessor p : nullToEmpty(group.getProcessors())) {
            processorsById.put(p.getIdentifier(), p);
        }
        for (VersionedPort p : nullToEmpty(group.getInputPorts())) {
            portsById.put(p.getIdentifier(), p);
        }
        for (VersionedPort p : nullToEmpty(group.getOutputPorts())) {
            portsById.put(p.getIdentifier(), p);
        }
        for (VersionedControllerService s : nullToEmpty(group.getControllerServices())) {
            controllerServicesById.put(s.getIdentifier(), s);
        }
        connections.addAll(nullToEmpty(group.getConnections()));
        for (VersionedProcessGroup child : nullToEmpty(group.getProcessGroups())) {
            collect(child);
        }
    }

    private void buildRouting() {
        for (VersionedConnection c : connections) {
            ConnectableComponent source = c.getSource();
            if (source.getType() != ConnectableComponentType.PROCESSOR) {
                // Handled by resolveTargets() from whatever real processor feeds this port/funnel chain.
                continue;
            }
            List<Edge> resolved = resolveTargets(c.getDestination().getId(), c.getDestination().getType(), new LinkedHashSet<>());
            Map<String, List<Edge>> byRelationship =
                    routing.computeIfAbsent(source.getId(), k -> new LinkedHashMap<>());
            for (String relationship : nullToEmpty(c.getSelectedRelationships())) {
                byRelationship.computeIfAbsent(relationship, k -> new ArrayList<>()).addAll(resolved);
            }
        }
    }

    /** Follows a destination through any chain of ports/funnels until it reaches a processor or a dead end. */
    private List<Edge> resolveTargets(String id, ConnectableComponentType type, Set<String> visited) {
        if (type == ConnectableComponentType.PROCESSOR) {
            return List.of(new Edge(new ProcessorTarget(id)));
        }
        if (type == ConnectableComponentType.REMOTE_INPUT_PORT || type == ConnectableComponentType.REMOTE_OUTPUT_PORT) {
            return List.of(new Edge(new SinkTarget("remote-" + sanitize(nameOrId(portLabel(id), id)), false)));
        }
        if (!visited.add(id)) {
            throw new IllegalStateException("Cycle detected through port/funnel '" + id + "' -- looping flows are not supported.");
        }
        List<VersionedConnection> further = outgoingBySourceId.getOrDefault(id, List.of());
        if (further.isEmpty()) {
            String label = type == ConnectableComponentType.FUNNEL
                    ? "funnel-" + id.substring(0, Math.min(8, id.length()))
                    : "port-" + sanitize(nameOrId(portLabel(id), id));
            return List.of(new Edge(new SinkTarget(label, type == ConnectableComponentType.OUTPUT_PORT)));
        }
        List<Edge> results = new ArrayList<>();
        for (VersionedConnection c : further) {
            results.addAll(resolveTargets(c.getDestination().getId(), c.getDestination().getType(), visited));
        }
        return results;
    }

    private String portLabel(String portId) {
        VersionedPort port = portsById.get(portId);
        return port == null ? null : port.getName();
    }

    private void findSources() {
        idsWithIncoming.addAll(connections.stream()
                .map(c -> c.getDestination().getId())
                .collect(Collectors.toCollection(LinkedHashSet::new)));

        for (String processorId : processorsById.keySet()) {
            if (!idsWithIncoming.contains(processorId)) {
                sourceProcessorIds.add(processorId);
            }
        }

        Set<String> allSourceIds = connections.stream()
                .map(c -> c.getSource().getId())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        for (String id : allSourceIds) {
            if (idsWithIncoming.contains(id) || processorsById.containsKey(id)) {
                continue;
            }
            // A port/funnel with no incoming connection: fed from outside this exported
            // document (or a design-time dangling port) -- treat it as an entry point too.
            for (VersionedConnection c : outgoingBySourceId.getOrDefault(id, List.of())) {
                for (Edge edge : resolveTargets(c.getDestination().getId(), c.getDestination().getType(), new LinkedHashSet<>())) {
                    if (edge.target() instanceof ProcessorTarget pt) {
                        sourceProcessorIds.add(pt.processorId());
                    }
                }
            }
        }
    }

    /**
     * Resolves a named (or identified) input port that has no incoming
     * connection -- i.e. a genuine flow entry point -- to the real
     * processor(s) it feeds, for delivering external input to just that one
     * entry point instead of every entry point in the flow.
     */
    public List<String> entryProcessorsForPort(String nameOrId) {
        VersionedPort port = findInputPort(nameOrId);
        if (idsWithIncoming.contains(port.getIdentifier())) {
            throw new IllegalArgumentException("Input port '" + nameOrId +
                    "' is not a flow entry point -- it has an incoming connection within this flow.");
        }
        List<VersionedConnection> outgoing = outgoingBySourceId.getOrDefault(port.getIdentifier(), List.of());
        if (outgoing.isEmpty()) {
            throw new IllegalArgumentException("Input port '" + nameOrId + "' has no outgoing connections.");
        }

        List<String> processorIds = new ArrayList<>();
        for (VersionedConnection c : outgoing) {
            for (Edge edge : resolveTargets(c.getDestination().getId(), c.getDestination().getType(), new LinkedHashSet<>())) {
                if (edge.target() instanceof ProcessorTarget pt) {
                    processorIds.add(pt.processorId());
                }
            }
        }
        if (processorIds.isEmpty()) {
            throw new IllegalArgumentException("Input port '" + nameOrId + "' doesn't reach any processor.");
        }
        return processorIds;
    }

    private VersionedPort findInputPort(String nameOrId) {
        VersionedPort byId = portsById.get(nameOrId);
        if (byId != null && byId.getType() == PortType.INPUT_PORT) {
            return byId;
        }
        List<VersionedPort> byName = portsById.values().stream()
                .filter(p -> p.getType() == PortType.INPUT_PORT)
                .filter(p -> nameOrId.equals(p.getName()))
                .collect(Collectors.toList());
        if (byName.size() == 1) {
            return byName.get(0);
        }
        if (byName.isEmpty()) {
            throw new IllegalArgumentException("No input port named or identified by '" + nameOrId + "' found in the flow.");
        }
        throw new IllegalArgumentException("Multiple input ports are named '" + nameOrId + "' -- use its identifier instead.");
    }

    public Collection<VersionedProcessor> processors() {
        return processorsById.values();
    }

    public VersionedProcessor processor(String id) {
        return processorsById.get(id);
    }

    public VersionedControllerService controllerService(String id) {
        return controllerServicesById.get(id);
    }

    public Set<String> sourceProcessorIds() {
        return sourceProcessorIds;
    }

    /** True if the flow declares at least one output port anywhere (at any nesting level). */
    public boolean hasOutputPorts() {
        return portsById.values().stream().anyMatch(p -> p.getType() == PortType.OUTPUT_PORT);
    }

    public List<Edge> targetsFor(String processorId, String relationshipName) {
        Map<String, List<Edge>> byRelationship = routing.get(processorId);
        if (byRelationship == null) {
            return List.of();
        }
        return byRelationship.getOrDefault(relationshipName, List.of());
    }

    /** Kahn's algorithm over the processor-only subgraph; fails fast if the flow isn't a DAG. */
    public List<String> topologicalOrder() {
        Map<String, Integer> inDegree = new LinkedHashMap<>();
        Map<String, List<String>> adjacency = new LinkedHashMap<>();
        for (String id : processorsById.keySet()) {
            inDegree.put(id, 0);
            adjacency.put(id, new ArrayList<>());
        }
        for (Map.Entry<String, Map<String, List<Edge>>> bySource : routing.entrySet()) {
            String from = bySource.getKey();
            if (!processorsById.containsKey(from)) {
                continue;
            }
            for (List<Edge> edges : bySource.getValue().values()) {
                for (Edge edge : edges) {
                    if (edge.target() instanceof ProcessorTarget pt) {
                        adjacency.get(from).add(pt.processorId());
                        inDegree.merge(pt.processorId(), 1, Integer::sum);
                    }
                }
            }
        }

        Deque<String> queue = new ArrayDeque<>();
        inDegree.forEach((id, degree) -> {
            if (degree == 0) {
                queue.add(id);
            }
        });

        List<String> order = new ArrayList<>();
        while (!queue.isEmpty()) {
            String id = queue.poll();
            order.add(id);
            for (String next : adjacency.get(id)) {
                if (inDegree.merge(next, -1, Integer::sum) == 0) {
                    queue.add(next);
                }
            }
        }

        if (order.size() != processorsById.size()) {
            Set<String> remaining = new LinkedHashSet<>(processorsById.keySet());
            remaining.removeAll(order);
            String names = remaining.stream()
                    .map(id -> processorsById.get(id).getName() + " (" + id + ")")
                    .collect(Collectors.joining(", "));
            throw new IllegalStateException("Flow contains a cycle involving: " + names +
                    " -- looping flows are not supported.");
        }
        return order;
    }

    private static String nameOrId(String name, String id) {
        return name != null && !name.isBlank() ? name : id;
    }

    private static String sanitize(String s) {
        return s.trim().replaceAll("[^a-zA-Z0-9._-]+", "_");
    }

    private static <T> Collection<T> nullToEmpty(Collection<T> c) {
        return c == null ? List.of() : c;
    }
}
