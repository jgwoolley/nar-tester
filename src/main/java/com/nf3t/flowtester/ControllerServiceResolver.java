package com.nf3t.flowtester;

import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedPropertyDescriptor;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Finds every controller service (transitively -- a service can reference
 * another service) that a processor's or service's properties depend on,
 * by matching property values against the flow's known controller service
 * identifiers. Prefers each property's {@code identifiesControllerService}
 * flag when the flow definition carries property descriptor metadata;
 * falls back to matching the raw value against known service ids otherwise.
 */
public final class ControllerServiceResolver {

    private ControllerServiceResolver() {
    }

    public static Map<String, VersionedControllerService> resolve(FlowGraph graph, VersionedConfigurableExtension extension) {
        Map<String, VersionedControllerService> resolved = new LinkedHashMap<>();
        collect(graph, extension, resolved, new LinkedHashSet<>());
        return resolved;
    }

    private static void collect(FlowGraph graph, VersionedConfigurableExtension extension,
                                 Map<String, VersionedControllerService> resolved, Set<String> visiting) {
        Map<String, String> properties = extension.getProperties();
        if (properties == null) {
            return;
        }
        Map<String, VersionedPropertyDescriptor> descriptors = extension.getPropertyDescriptors();

        for (Map.Entry<String, String> property : properties.entrySet()) {
            String value = property.getValue();
            if (value == null) {
                continue;
            }
            VersionedControllerService service = graph.controllerService(value);
            if (service == null) {
                continue;
            }
            VersionedPropertyDescriptor descriptor = descriptors == null ? null : descriptors.get(property.getKey());
            if (descriptor != null && !descriptor.getIdentifiesControllerService()) {
                // A property explicitly not meant to reference a service, whose value
                // just happens to coincide with a service id -- not a real reference.
                continue;
            }
            if (resolved.containsKey(value) || !visiting.add(value)) {
                continue;
            }
            resolved.put(value, service);
            collect(graph, service, resolved, visiting);
            visiting.remove(value);
        }
    }
}
