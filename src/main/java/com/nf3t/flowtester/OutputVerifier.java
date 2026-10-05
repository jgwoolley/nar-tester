package com.nf3t.flowtester;

import org.apache.nifi.util.MockFlowFile;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Checks a {@link FlowTestExecutor.ExecutionResult}'s captured outputs
 * against a {@code --require-output} spec. Only what the spec actually
 * mentions is checked: an output label the spec doesn't mention is ignored,
 * and an attribute an expected flowfile doesn't list is never compared
 * (dynamic attributes like {@code uuid}/{@code path} are safe to leave out).
 */
public final class OutputVerifier {

    private OutputVerifier() {
    }

    /**
     * @param contentFileBaseDir directory that a relative {@code contentFile} in the spec resolves against
     *                           (the require-output file's own directory).
     * @return one human-readable failure message per mismatch; empty if everything matched.
     */
    public static List<String> verify(RequireOutputSpec spec, List<FlowTestExecutor.OutputGroup> actualGroups,
                                       File contentFileBaseDir) throws IOException {
        List<String> failures = new ArrayList<>();

        Map<String, FlowTestExecutor.OutputGroup> actualByLabel = new LinkedHashMap<>();
        for (FlowTestExecutor.OutputGroup group : actualGroups) {
            actualByLabel.put(group.label, group);
        }

        for (Map.Entry<String, List<ExpectedFlowFile>> entry : spec.getOutputs().entrySet()) {
            String label = entry.getKey();
            List<ExpectedFlowFile> expectedFlowFiles = entry.getValue();
            FlowTestExecutor.OutputGroup actual = actualByLabel.get(label);
            int actualCount = actual == null ? 0 : actual.flowFiles.size();

            if (actualCount != expectedFlowFiles.size()) {
                failures.add("Output '" + label + "': expected " + expectedFlowFiles.size() +
                        " flowfile(s), got " + actualCount + ".");
                continue;
            }

            for (int i = 0; i < expectedFlowFiles.size(); i++) {
                verifyOne(label, i, expectedFlowFiles.get(i), actual.flowFiles.get(i), contentFileBaseDir, failures);
            }
        }

        return failures;
    }

    private static void verifyOne(String label, int index, ExpectedFlowFile expected, MockFlowFile actual,
                                   File contentFileBaseDir, List<String> failures) throws IOException {
        String prefix = "Output '" + label + "' flowfile[" + index + "]";

        if (expected.getContent() != null && expected.getContentFile() != null) {
            failures.add(prefix + ": specifies both 'content' and 'contentFile' -- use only one.");
        } else if (expected.getContent() != null || expected.getContentFile() != null) {
            byte[] expectedContent = expected.getContent() != null
                    ? expected.getContent().getBytes(StandardCharsets.UTF_8)
                    : Files.readAllBytes(resolveContentFile(contentFileBaseDir, expected.getContentFile()));
            byte[] actualContent = actual.toByteArray();
            if (!java.util.Arrays.equals(expectedContent, actualContent)) {
                failures.add(prefix + ": content mismatch (expected " + expectedContent.length +
                        " byte(s), got " + actualContent.length + " byte(s)).");
            }
        }

        for (Map.Entry<String, String> attribute : expected.getAttributes().entrySet()) {
            String actualValue = actual.getAttribute(attribute.getKey());
            if (!Objects.equals(actualValue, attribute.getValue())) {
                failures.add(prefix + ": attribute '" + attribute.getKey() + "' expected '" + attribute.getValue() +
                        "', got " + (actualValue == null ? "<missing>" : "'" + actualValue + "'") + ".");
            }
        }
    }

    private static Path resolveContentFile(File baseDir, String contentFile) {
        Path path = Path.of(contentFile);
        return path.isAbsolute() ? path : baseDir.toPath().resolve(path);
    }
}
