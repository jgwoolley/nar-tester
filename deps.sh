#!/usr/bin/env bash

# `deps` resolves the same NAR/jar dependencies as the run above, but without
# instantiating or running anything -- handy for checking what a flow would
# pull in ahead of time. --flow here pulls every processor's and
# transitively-referenced controller service's type out of the flow
# definition; --processor / --controller-service can also be passed
# directly instead.
echo
echo "=== deps ==="
java -jar target/flow-tester.jar deps \
  --flow examples/flow-lookup-attribute.json \
  --nifi-properties nifi/conf/nifi.properties