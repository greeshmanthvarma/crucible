#!/bin/sh
set -eu
test "$(sh /source/greet.sh Ada)" = "Hello, Ada!"
test "$(sh /source/greet.sh Lin)" = "Hello, Lin!"
