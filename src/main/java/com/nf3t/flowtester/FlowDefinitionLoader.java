package com.nf3t.flowtester;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.io.File;
import java.io.IOException;

/**
 * Reads a NiFi "flow definition" JSON -- what a process group's
 * "Download Flow Definition" action (or {@code GET
 * /nifi-api/process-groups/{id}/download}) produces -- into the same
 * {@code org.apache.nifi.flow.*} model NiFi itself uses, straight off
 * the existing nifi-api dependency (no extra Maven dependency needed).
 *
 * Handles both shapes actually seen in the wild: the usual
 * {@link VersionedExternalFlow} envelope (with a {@code flowContents}
 * field), and a bare {@link VersionedProcessGroup} with no envelope.
 */
public final class FlowDefinitionLoader {

    private FlowDefinitionLoader() {
    }

    public static VersionedProcessGroup load(File flowFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        VersionedExternalFlow externalFlow = mapper.readValue(flowFile, VersionedExternalFlow.class);
        if (externalFlow.getFlowContents() != null) {
            return externalFlow.getFlowContents();
        }
        return mapper.readValue(flowFile, VersionedProcessGroup.class);
    }
}
