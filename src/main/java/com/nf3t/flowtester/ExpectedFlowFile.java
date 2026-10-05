package com.nf3t.flowtester;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One expected flowfile in a {@code --require-output} YAML file. Both
 * {@code content}/{@code contentFile} and {@code attributes} are optional
 * and independent: omit content entirely to only check attributes (or
 * nothing at all, to just assert a flowfile exists in that slot), and give
 * only the attributes you actually care about -- unlisted attributes
 * (uuid, path, or anything else dynamic) are never checked.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExpectedFlowFile {

    private String content;
    private String contentFile;
    private Map<String, String> attributes = new LinkedHashMap<>();

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getContentFile() {
        return contentFile;
    }

    public void setContentFile(String contentFile) {
        this.contentFile = contentFile;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }
}
