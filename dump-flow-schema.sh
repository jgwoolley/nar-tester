#!/usr/bin/env bash
# Dumps the JSON Schema for a --flow flow definition file
# (org.apache.nifi.flow.VersionedExternalFlow), generated straight from the
# real NiFi model classes via reflection -- so it always matches whatever
# nifi.version this build was compiled against. Point an editor's JSON
# schema support at the output (or add "$schema": "<path>" to a flow
# definition file itself) for validation/autocomplete while hand-editing one.
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -f target/flow-tester.jar ]; then
  echo "Building flow-tester.jar..."
  mvn -q package
fi

OUTPUT="${1:-flow-definition.schema.json}"

java -jar target/flow-tester.jar flow-schema --output "$OUTPUT"
