#!/usr/bin/env bash
# Example end-to-end run of flow-tester against a real, off-the-shelf flow:
# UpdateAttribute (from nifi-update-attribute-nar) feeds LookupAttribute
# (from nifi-standard-nar), which looks up a value via a
# SimpleKeyValueLookupService controller service that lives in a *different*
# NAR (nifi-lookup-services-nar). NAR resolution and controller-service
# wiring are both fully automatic, driven entirely by
# examples/flow-lookup-attribute.json (a hand-authored stand-in for what a
# process group's "Download Flow Definition" action would produce).
# --require-output asserts the captured output matches
# examples/require-output.json, so this script fails (exit 3) if the flow's
# actual behavior ever drifts from what's expected.
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -f target/flow-tester.jar ]; then
  echo "Building flow-tester.jar..."
  mvn -q package
fi

java -jar target/flow-tester.jar \
  --flow examples/flow-lookup-attribute.json \
  --input examples/example-input.flowfile.pkg \
  --nifi-properties nifi/conf/nifi.properties \
  --require-output examples/require-output.json \
  --output output
