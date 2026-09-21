package com.example.nartester;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deserialized from a YAML (or JSON) config file. Example:
 *
 * <pre>
 * processorClass: org.apache.nifi.processors.example.MyProcessor
 * properties:
 *   Some Property: some-value
 *   Another Property: "${literal}"
 * attributes:
 *   filename: input.json
 *   mime.type: application/json
 * relationships:
 *   - success
 *   - failure
 * runIterations: 1
 * controllerServices:
 *   - identifier: my-service
 *     implementationClass: org.apache.nifi.example.MyControllerServiceImpl
 *     properties:
 *       Some Service Property: value
 * </pre>
 *
 * A processor property that references a controller service should use
 * that service's {@code identifier} as its value (see
 * {@code examples/example-config.yaml}).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TestConfig {

    private String processorClass;
    private Map<String, String> properties = new LinkedHashMap<>();
    private Map<String, String> attributes = new LinkedHashMap<>();

    /** If null or empty, every relationship the processor declares is captured. */
    private List<String> relationships;

    private int runIterations = 1;

    /** Controller services to instantiate, configure, and enable before the processor runs. */
    private List<ControllerServiceConfig> controllerServices = new ArrayList<>();

    public String getProcessorClass() {
        return processorClass;
    }

    public void setProcessorClass(String processorClass) {
        this.processorClass = processorClass;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public List<String> getRelationships() {
        return relationships;
    }

    public void setRelationships(List<String> relationships) {
        this.relationships = relationships;
    }

    public int getRunIterations() {
        return runIterations;
    }

    public void setRunIterations(int runIterations) {
        this.runIterations = runIterations;
    }

    public List<ControllerServiceConfig> getControllerServices() {
        return controllerServices;
    }

    public void setControllerServices(List<ControllerServiceConfig> controllerServices) {
        this.controllerServices = controllerServices;
    }
}
