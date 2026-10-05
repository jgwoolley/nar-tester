package com.nf3t.flowtester;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Root of a {@code --require-output} JSON file:
 *
 * <pre>
 * {
 *   "$schema": "./require-output.schema.json",
 *   "outputs": {
 *     "Lookup_Attribute-matched": [
 *       { "attributes": { "greeting": "Earth" } }
 *     ]
 *   }
 * }
 * </pre>
 *
 * {@code $schema} is a plain JSON property that editors like VS Code read
 * to enable schema validation/autocomplete for this file; flow-tester
 * itself ignores it entirely (see the {@code schema} subcommand for
 * generating that file).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RequireOutputSpec {

    private Map<String, List<ExpectedFlowFile>> outputs = new LinkedHashMap<>();

    public Map<String, List<ExpectedFlowFile>> getOutputs() {
        return outputs;
    }

    public void setOutputs(Map<String, List<ExpectedFlowFile>> outputs) {
        this.outputs = outputs;
    }
}
