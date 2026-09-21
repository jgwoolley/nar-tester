package com.example.nartester;

/**
 * One bundled dependency jar loaded from a NAR, with its Maven coordinates
 * if the jar carries a {@code META-INF/maven/<groupId>/<artifactId>/pom.properties}
 * entry (as jars built by Maven normally do). {@code coordinates} is null when
 * no such entry could be found (e.g. a jar not built by Maven).
 */
public record JarInfo(String fileName, String coordinates) {
}
