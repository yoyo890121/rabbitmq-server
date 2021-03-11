#!/bin/bash

set -euo pipefail

cd /workspace/rabbitmq/deps/$project

trap 'catch $?' EXIT

catch() {
    if [ "$1" != "0" ]; then
        make ct-logs-archive && mv *-ct-logs-*.tar.xz /workspace/ct-logs/
    fi
}

make test-build

make tests \
    FULL= \
    FAIL_FAST=1 \
    SKIP_AS_ERROR=1
