package com.example.nartester;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One controller service to instantiate, configure, and enable on the
 * TestRunner before the processor runs. The identifier is what processor
 * properties that reference this service (PropertyDescriptor built with
 * .identifiesControllerService(...)) should use as their value.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ControllerServiceConfig {

    private String identifier;
    private String implementationClass;
    private Map<String, String> properties = new LinkedHashMap<>();

    /**
     * Optional path to the .nar file that provides this service's implementation,
     * when it lives in a different NAR than the processor under test. Requires
     * --nifi-properties or --nar-library so the dependency graph can be resolved.
     * If omitted, the service is loaded from the same NAR as the processor.
     */
    private String nar;

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getImplementationClass() {
        return implementationClass;
    }

    public void setImplementationClass(String implementationClass) {
        this.implementationClass = implementationClass;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getNar() {
        return nar;
    }

    public void setNar(String nar) {
        this.nar = nar;
    }
}
