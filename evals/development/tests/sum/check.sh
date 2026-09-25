#!/bin/sh
set -eu
test "$(sh /source/sum.sh 3 4)" = "7"
test "$(sh /source/sum.sh -2 5)" = "3"
