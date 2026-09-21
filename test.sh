#!/usr/bin/env bash
# Example end-to-end run of nar-tester against real, off-the-shelf components
# from the NiFi instance checked into nifi/: LookupAttribute (from
# nifi-standard-nar) looks up a value via a SimpleKeyValueLookupService
# controller service (from the separate nifi-lookup-services-nar) and adds
# it as a flowfile attribute.
#
# --nar is deliberately omitted: nar-tester auto-discovers which .nar in
# nifi/conf/nifi.properties' library directories provides the configured
# processorClass. The controller service's NAR is named explicitly via
# controllerServices[].nar in the config, since it lives in a different NAR
# than the processor.
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -f target/nar-tester.jar ]; then
  echo "Building nar-tester.jar..."
  mvn -q package
fi

java -jar target/nar-tester.jar \
  --config examples/lookup-attribute-config.yaml \
  --input examples/example-input.txt \
  --nifi-properties nifi/conf/nifi.properties \
  --output output

# `deps` resolves the same NAR/jar dependencies as the run above, but without
# instantiating or running anything -- handy for checking what a processor +
# controller service combination would pull in ahead of time. --config here
# reuses the same config, pulling processorClass and every
# controllerServices[].implementationClass out of it; --processor /
# --controller-service can also be passed directly instead.
echo
echo "=== deps ==="
java -jar target/nar-tester.jar deps \
  --config examples/lookup-attribute-config.yaml \
  --nifi-properties nifi/conf/nifi.properties
